package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/open-ug/conveyor/internal/models"
	"github.com/open-ug/conveyor/internal/utils"
	"github.com/open-ug/conveyor/pkg/types"
)

type EngineContext struct {
	NatsContext   utils.NatsContext
	PipelineModel *models.PipelineModel
	ResourceModel *models.ResourceModel
	LogModel      *models.LogModel
	ctx           context.Context
	cancel        context.CancelFunc
}

type PipelineEvent struct {
	Event             string            `json:"event"` // e.g "create", "update", "delete"
	RunID             string            `json:"run_id"`
	Resource          types.Resource    `json:"resource"`
	DriverResultEvent DriverResultEvent `json:"driverresult"`
}

func NewEngineContext(logmodel *models.LogModel, natsContext utils.NatsContext, db *badger.DB) *EngineContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &EngineContext{
		NatsContext:   natsContext,
		PipelineModel: models.NewPipelineModel(db),
		ResourceModel: models.NewResourceModel(db),
		LogModel:      logmodel,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func (ec *EngineContext) Start() error {
	log.Println("Starting the engine...")

	consumer, err := utils.CreateDurableConsumer(context.Background(), ec.NatsContext.JetStream, "pipeline-engine", jetstream.ConsumerConfig{
		Name:          "pipeline-engine",
		FilterSubject: "pipelines.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Println("Error creating consumer: ", err)
		return err
	}

	log.Println("Engine started and listening for pipeline events...")
	consumerErrors := make(chan error, 1)
	reportUnavailable := func(err error) {
		if errors.Is(err, nats.ErrNoResponders) {
			select {
			case consumerErrors <- err:
			default:
			}
		}
	}
	cc, err := consumer.Consume(ec.consumePipelineEvents, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		log.Printf("Pipeline consumer error: %v", err)
		reportUnavailable(err)
	}))
	if err != nil {
		log.Println("Error consuming pipeline events: ", err)
		return err
	}
	defer cc.Stop()

	// Create log consumer
	logconsumer, err := utils.CreateDurableConsumer(context.Background(), ec.NatsContext.JetStream, "logs-engine", jetstream.ConsumerConfig{
		Name:          "logs-engine",
		FilterSubject: "logs.>",
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Println("Error creating consumer: ", err)
		return err
	}

	log.Println("Log consumer started...")
	lc, err := logconsumer.Consume(ec.consumeLogEvents, jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		log.Printf("Log consumer error: %v", err)
		reportUnavailable(err)
	}))
	if err != nil {
		log.Println("Error consuming log events: ", err)
		return err
	}
	defer lc.Stop()

	select {
	case <-ec.ctx.Done():
		return nil
	case <-cc.Closed():
		return fmt.Errorf("pipeline consumer stopped unexpectedly")
	case <-lc.Closed():
		return fmt.Errorf("log consumer stopped unexpectedly")
	case err := <-consumerErrors:
		return fmt.Errorf("engine consumer unavailable: %w", err)
	}
}

func (ec *EngineContext) consumePipelineEvents(msg jetstream.Msg) {
	// Logic to consume pipeline-related events from NATS
	msg.Ack()

	data := msg.Data()
	subject := msg.Subject()
	var event PipelineEvent
	err := json.Unmarshal(data, &event)
	if err != nil {
		log.Println("Error unmarshaling pipeline event: ", err)
		return
	}

	current, err := ec.ResourceModel.FindOne(event.Resource.Name, event.Resource.Resource)
	if err != nil || current.ID != event.Resource.ID {
		return
	}
	// Older runtimes report a generic result event. Recover its operation only
	// when the stored run matches, so stale results cannot enter cleanup.
	if subject == "pipelines.driver.result" && event.Event == "driver.result" && current.Metadata["current_pipeline_run"] == event.RunID {
		if operation, ok := current.Metadata["current_pipeline_event"].(string); ok {
			event.Event = operation
		}
	}
	if current.Metadata["deleting"] == true && (event.Event != "delete" || current.Metadata["deletion_runid"] != event.RunID) {
		return
	}
	if event.Resource.Pipeline == "" {
		// No pipeline associated, ignore
		return
	}

	// Get pipeline details
	pipeline, err := ec.PipelineModel.GetPipeline(event.Resource.Pipeline)
	if err != nil {
		log.Println("Error getting pipeline details: ", err)
		return
	}

	mID, _ := utils.GenerateRandomID()

	switch subject {
	case "pipelines.driver.result":

		// Process driver result and move to next step
		ec.handleProcessDriverResult(event, pipeline)

	case "pipelines.pipeline.init":
		if len(pipeline.Steps) == 0 {
			log.Printf("Pipeline '%s' has no steps for run '%s'", pipeline.Name, event.RunID)
			return
		}
		firstStep := pipeline.Steps[0]
		event.Resource, err = ec.ResourceModel.StartPipelineRun(event.Resource.Name, event.Resource.Resource, event.RunID, event.Event, firstStep.ID)
		if err != nil {
			log.Println("Error starting pipeline run: ", err)
			return
		}

		resourceJson, err := json.Marshal(event.Resource)
		if err != nil {
			log.Println("Error marshaling resource: ", err)
			return
		}
		// Driver message
		driverMessage := types.DriverMessage{
			Event:   event.Event,
			RunID:   event.RunID,
			Payload: string(resourceJson),
			ID:      mID,
		}

		// Publish to the first step's driver
		driverSubject := "drivers." + firstStep.Driver + ".resources." + event.Resource.Resource
		if err := ec.publishEvent(driverSubject, driverMessage); err != nil {
			log.Printf("Failed to dispatch first step '%s' for run '%s': %v", firstStep.Name, event.RunID, err)
			if event.Event == "delete" {
				_ = ec.ResourceModel.MarkDeletionFailed(event.Resource.Name, event.Resource.Resource)
			} else {
				_ = ec.ResourceModel.MarkPipelineFailure(event.Resource.Name, event.Resource.Resource, event.RunID, firstStep.ID, "failed to dispatch first step: "+err.Error())
			}
			return
		}

	case "pipelines.pipeline.resume":
		ec.handleResumePipelineRun(event, current, pipeline)
	default:
		log.Println("Unhandled pipeline event subject: ", subject)
	}

}

// handleResumePipelineRun re-dispatches the first untrusted step of a resumed
// run. PreparePipelineResume already rewrote the metadata and kept trusted
// results, so this only validates the run and publishes the driver message.
func (ec *EngineContext) handleResumePipelineRun(event PipelineEvent, current types.Resource, pipeline *types.Pipeline) {
	if current.Metadata["current_pipeline_run"] != event.RunID {
		log.Printf("Ignoring stale resume event for run '%s' (resource '%s')", event.RunID, current.Name)
		return
	}
	stepID, ok := current.Metadata["current_pipeline_step"].(string)
	if !ok || stepID == "" {
		log.Printf("Resume event for run '%s' has no current step", event.RunID)
		return
	}
	var step *types.Step
	for i := range pipeline.Steps {
		if pipeline.Steps[i].ID == stepID {
			step = &pipeline.Steps[i]
			break
		}
	}
	if step == nil {
		log.Printf("Resume step '%s' no longer exists in pipeline '%s'", stepID, pipeline.Name)
		_ = ec.ResourceModel.MarkPipelineFailure(current.Name, current.Resource, event.RunID, stepID, "resume step no longer exists in pipeline definition")
		return
	}
	operation, ok := current.Metadata["current_pipeline_event"].(string)
	if !ok || operation == "" {
		log.Printf("Resume event for run '%s' is missing its operation", event.RunID)
		return
	}

	mID, err := utils.GenerateRandomID()
	if err != nil {
		log.Println("Error generating driver message ID: ", err)
		return
	}
	resourceJson, err := json.Marshal(current)
	if err != nil {
		log.Println("Error marshaling resource: ", err)
		return
	}
	driverMessage := types.DriverMessage{
		Event:   operation,
		RunID:   event.RunID,
		Payload: string(resourceJson),
		ID:      mID,
	}
	subject := "drivers." + step.Driver + ".resources." + current.Resource
	if err := ec.publishEvent(subject, driverMessage); err != nil {
		log.Printf("Failed to dispatch resumed step '%s' for run '%s': %v", step.Name, event.RunID, err)
		_ = ec.ResourceModel.MarkPipelineFailure(current.Name, current.Resource, event.RunID, step.ID, "failed to dispatch resumed step: "+err.Error())
		return
	}
	log.Printf("Resumed run '%s' at step '%s' for resource '%s'", event.RunID, step.Name, current.Name)
}

// publishEvent publishes an event to driver
func (ec *EngineContext) publishEvent(subject string, message types.DriverMessage) error {
	jsonMsg, merr := json.Marshal(message)
	if merr != nil {
		return merr
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := ec.NatsContext.JetStream.Publish(ctx, subject, jsonMsg)

	if err != nil {
		log.Println("Error publishing event to driver: ", err)
		return err
	}
	return nil
}

func (ec *EngineContext) handleProcessDriverResult(event PipelineEvent, pipeline *types.Pipeline) {
	resource, err := ec.ResourceModel.FindOne(event.Resource.Name, event.Resource.Resource)
	if err != nil {
		log.Println("Error retrieving resource: ", err)
		return
	}
	if runID, ok := resource.Metadata["current_pipeline_run"].(string); ok && runID != event.RunID {
		log.Printf("Ignoring result from superseded run '%s' for resource '%s'", event.RunID, resource.Name)
		return
	}
	currentStepID, err := ec.ResourceModel.GetCurrentPipelineStep(resource.Name, resource.Resource)
	if err != nil {
		log.Println("Error getting current pipeline step: ", err)
		return
	}
	if stepID, ok := event.Resource.Metadata["current_pipeline_step"].(string); ok && stepID != currentStepID {
		log.Printf("Ignoring result from previous step '%s' for run '%s'", stepID, event.RunID)
		return
	}
	currentStepIndex := -1
	for i, step := range pipeline.Steps {
		if step.ID == currentStepID && step.Driver == event.DriverResultEvent.Driver {
			currentStepIndex = i
			break
		}
	}
	if currentStepIndex < 0 {
		log.Printf("Ignoring result from driver '%s' outside current step '%s' for run '%s'", event.DriverResultEvent.Driver, currentStepID, event.RunID)
		return
	}

	// Save the driver result. Successful non-deletion steps also record an
	// ordered result plus a signature so a resume can keep trusted prefix work.
	isDeletion := event.Event == "delete"
	if event.DriverResultEvent.Success && !isDeletion {
		err = ec.ResourceModel.RecordStepResult(event.Resource.Name, event.Resource.Resource, currentStepID, event.DriverResultEvent.Driver, event.DriverResultEvent)
	} else {
		err = ec.ResourceModel.SaveDriverResult(event.Resource.Name, event.Resource.Resource, event.DriverResultEvent.Driver, event.DriverResultEvent)
	}
	if err != nil {
		log.Println("Error saving driver result: ", err)
		return
	}

	// update resource to include latest driver result
	updatedResource, err := ec.ResourceModel.FindOne(event.Resource.Name, event.Resource.Resource)
	if err != nil {
		log.Println("Error retrieving updated resource: ", err)
		return
	}
	event.Resource = updatedResource

	// Use the stored operation so older drivers that emit "driver.result"
	// still preserve cleanup throughout the run.
	operation, ok := event.Resource.Metadata["current_pipeline_event"].(string)
	if !ok || operation == "" {
		if event.Event == "delete" {
			operation = nextOperation(event.Event)
		} else {
			log.Printf("Missing pipeline operation for run '%s'; start a new run", event.RunID)
			return
		}
	}

	if operation == "delete" && !deletionStageSucceeded(event.DriverResultEvent) {
		if err := ec.ResourceModel.MarkDeletionFailed(event.Resource.Name, event.Resource.Resource); err != nil {
			log.Println("Error recording deletion failure: ", err)
		}
		return
	}
	if !event.DriverResultEvent.Success {
		log.Printf("Pipeline run '%s' stopped at step '%s' for resource '%s': %s", event.RunID, pipeline.Steps[currentStepIndex].Name, event.Resource.Name, event.DriverResultEvent.Message)
		if err := ec.ResourceModel.MarkPipelineFailure(event.Resource.Name, event.Resource.Resource, event.RunID, currentStepID, event.DriverResultEvent.Message); err != nil {
			log.Println("Error recording pipeline failure: ", err)
		}
		return
	}

	if currentStepIndex+1 == len(pipeline.Steps) {
		if operation == "delete" {
			if err := ec.ResourceModel.FinishDeletion(event.Resource.Name, event.Resource.Resource, event.RunID); err != nil {
				log.Println("Deletion cleanup failed: ", err)
				return
			}
		} else if err := ec.ResourceModel.FinishPipelineRun(event.Resource.Name, event.Resource.Resource); err != nil {
			log.Println("Error finishing pipeline run: ", err)
			return
		}
		log.Printf("Pipeline run '%s' completed for resource '%s'", event.RunID, event.Resource.Name)
		return
	}
	nextStep := pipeline.Steps[currentStepIndex+1]

	mID, err := utils.GenerateRandomID()
	if err != nil {
		log.Println("Error generating driver message ID: ", err)
		return
	}

	event.Resource.Metadata["current_pipeline_step"] = nextStep.ID
	resourceJson, err := json.Marshal(event.Resource)
	if err != nil {
		log.Println("Error marshaling resource: ", err)
		return
	}

	driverMessage := types.DriverMessage{
		Event:   operation,
		RunID:   event.RunID,
		Payload: string(resourceJson),
		ID:      mID,
	}
	subject := "drivers." + nextStep.Driver + ".resources." + event.Resource.Resource
	if err := ec.publishEvent(subject, driverMessage); err != nil {
		log.Printf("Failed to dispatch step '%s' for run '%s': %v", nextStep.Name, event.RunID, err)
		if operation == "delete" {
			_ = ec.ResourceModel.MarkDeletionFailed(event.Resource.Name, event.Resource.Resource)
		} else {
			_ = ec.ResourceModel.MarkPipelineFailure(event.Resource.Name, event.Resource.Resource, event.RunID, nextStep.ID, "failed to dispatch next step: "+err.Error())
		}
		return
	}

	if err := ec.ResourceModel.SetCurrentPipelineStep(event.Resource.Name, event.Resource.Resource, nextStep.ID); err != nil {
		log.Println("Error setting current pipeline step: ", err)
		return
	}
	log.Printf("Moving to next step '%s' for resource '%s' (run '%s')", nextStep.Name, event.Resource.Name, event.RunID)
}

func (ec *EngineContext) Stop() error {
	ec.cancel()
	return nil
}

func nextOperation(event string) string {
	if event == "delete" {
		return "delete"
	}
	return "process"
}

// Crane stages carry operational success inside the driver payload.
func deletionStageSucceeded(result DriverResultEvent) bool {
	if !result.Success {
		return false
	}
	if result.Data == nil {
		return true
	}
	encoded, err := json.Marshal(result.Data)
	if err != nil {
		return false
	}
	var payload struct {
		Success *bool `json:"success"`
	}
	if json.Unmarshal(encoded, &payload) != nil {
		return false
	}
	return payload.Success == nil || *payload.Success
}
