package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/open-ug/conveyor/internal/models"
	"github.com/open-ug/conveyor/pkg/types"
)

type engineFakeClock struct {
	now time.Time
}

func (e engineFakeClock) Now() time.Time { return e.now }

// publishInit publishes a create/update resource event and processes it.
func publishInit(t *testing.T, ec *EngineContext, consumer jetstream.Consumer, resource types.Resource, operation string) string {
	t.Helper()
	runID, err := PublishResourceEvent(operation, resource, ec.NatsContext.JetStream)
	if err != nil {
		t.Fatal(err)
	}
	processEvent(t, ec, consumer)
	return runID
}

func reportResult(t *testing.T, ec *EngineContext, consumer jetstream.Consumer, driver string, success bool, runID string, resource types.Resource) {
	t.Helper()
	event := DriverResultEvent{Driver: driver, Success: success, Message: "test result"}
	if err := event.PublishEvent(runID, resource, ec.NatsContext.JetStream); err != nil {
		t.Fatal(err)
	}
	processEvent(t, ec, consumer)
}

// driveToFailure runs steps 1 and 2 to success then fails step 3 (docker).
func driveToFailure(t *testing.T, ec *EngineContext, consumer jetstream.Consumer, drivers map[string]jetstream.Consumer, resource types.Resource, operation string) string {
	t.Helper()
	runID := publishInit(t, ec, consumer, resource, operation)
	_, build := driverMessage(t, drivers["buildpacks"], operation, runID, "1")
	reportResult(t, ec, consumer, "buildpacks", true, runID, build)
	_, status := driverMessage(t, drivers["status"], operation, runID, "2")
	reportResult(t, ec, consumer, "status", true, runID, status)
	_, deploy := driverMessage(t, drivers["docker"], operation, runID, "3")
	reportResult(t, ec, consumer, "docker", false, runID, deploy)
	return runID
}

func insertResource(t *testing.T, ec *EngineContext, name string) types.Resource {
	t.Helper()
	resource := types.Resource{Name: name, Resource: "gitop", Pipeline: "gitops", Metadata: map[string]interface{}{"label": "keep"}}
	data, _ := json.Marshal(resource)
	if err := ec.ResourceModel.Insert(name, resource.Resource, data); err != nil {
		t.Fatal(err)
	}
	return resource
}

func TestPipelineFailureIsRecordedAndCleared(t *testing.T) {
	ec, consumer, drivers := testEngine(t)
	ec.ResourceModel.Clock = engineFakeClock{now: time.Date(2026, 11, 26, 9, 0, 0, 0, time.UTC)}
	resource := insertResource(t, ec, "failing-app")

	runID := driveToFailure(t, ec, consumer, drivers, resource, "update")
	stored, err := ec.ResourceModel.FindOne(resource.Name, resource.Resource)
	if err != nil {
		t.Fatal(err)
	}
	var failure models.PipelineFailure
	if err := decodeEngineMeta(stored.Metadata["pipeline_failure"], &failure); err != nil {
		t.Fatalf("pipeline_failure missing or malformed: %v", err)
	}
	if failure.RunID != runID || failure.StepID != "3" || failure.Message != "test result" {
		t.Fatalf("unexpected failure record: %+v", failure)
	}
	if failure.FailedAt != "2026-11-26T09:00:00Z" || failure.Version != "1" {
		t.Fatalf("failed_at/version wrong: %+v", failure)
	}

	// Successful steps carry signatures; the failed step does not.
	var signatures []models.StepSignature
	if err := decodeEngineMeta(stored.Metadata["pipeline_step_signatures"], &signatures); err != nil {
		t.Fatal(err)
	}
	if len(signatures) != 2 || signatures[0].StepID != "1" || signatures[1].Driver != "status" {
		t.Fatalf("unexpected signatures: %+v", signatures)
	}
	if results, ok := stored.Metadata["driverresults"].(map[string]interface{}); !ok || results["docker"] == nil || results["buildpacks"] == nil {
		t.Fatalf("driver results not retained as expected: %+v", stored.Metadata["driverresults"])
	}

	// A successful full run clears the failure marker and signatures.
	plan, err := ec.ResourceModel.PreparePipelineResume(resource.Name, resource.Resource, mustPipeline(t, ec, "gitops"))
	if err != nil {
		t.Fatal(err)
	}
	if err := PublishResumeEvent(plan.RunID, plan.Resource, ec.NatsContext.JetStream); err != nil {
		t.Fatal(err)
	}
	processEvent(t, ec, consumer)
	_, redeploy := driverMessage(t, drivers["docker"], "update", plan.RunID, "3")
	reportResult(t, ec, consumer, "docker", true, plan.RunID, redeploy)
	_, finalStatus := driverMessage(t, drivers["status"], "update", plan.RunID, "4")
	reportResult(t, ec, consumer, "status", true, plan.RunID, finalStatus)

	finished, _ := ec.ResourceModel.FindOne(resource.Name, resource.Resource)
	if finished.Metadata["pipeline_failure"] != nil || finished.Metadata["pipeline_step_signatures"] != nil {
		t.Fatalf("successful run left failure bookkeeping: %+v", finished.Metadata)
	}
}

func TestResumeContinuesWithNewRunAndKeepsPrefixResults(t *testing.T) {
	ec, consumer, drivers := testEngine(t)
	resource := insertResource(t, ec, "resume-app")
	oldRun := driveToFailure(t, ec, consumer, drivers, resource, "update")

	plan, err := ec.ResourceModel.PreparePipelineResume(resource.Name, resource.Resource, mustPipeline(t, ec, "gitops"))
	if err != nil {
		t.Fatal(err)
	}
	if plan.RunID == oldRun || plan.ResumeStepID != "3" {
		t.Fatalf("resume must mint a new run and restart at step 3: %+v", plan)
	}
	if err := PublishResumeEvent(plan.RunID, plan.Resource, ec.NatsContext.JetStream); err != nil {
		t.Fatal(err)
	}
	processEvent(t, ec, consumer)

	message, redeploy := driverMessage(t, drivers["docker"], "update", plan.RunID, "3")
	if message.Payload == "" {
		t.Fatal("resume dispatched an empty payload")
	}
	var payload types.Resource
	if err := json.Unmarshal([]byte(message.Payload), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Metadata["resumed_from"] != nil {
		t.Fatal("resume payload must not carry resumed_from")
	}
	// The expensive first-step build result survives the resume.
	results := payload.Metadata["driverresults"].(map[string]interface{})
	if results["buildpacks"] == nil || results["status"] == nil {
		t.Fatalf("trusted prefix results were lost: %+v", results)
	}
	if results["docker"] != nil {
		t.Fatal("failed step result must be discarded before redispatch")
	}

	// A late result from the old run is ignored and cannot write failure state.
	reportResult(t, ec, consumer, "docker", false, oldRun, redeploy)
	stored, _ := ec.ResourceModel.FindOne(resource.Name, resource.Resource)
	if stored.Metadata["current_pipeline_step"] != "3" {
		t.Fatalf("late result changed current step: %v", stored.Metadata["current_pipeline_step"])
	}
	var failure models.PipelineFailure
	if err := decodeEngineMeta(stored.Metadata["pipeline_failure"], &failure); err == nil && failure.RunID == oldRun {
		t.Fatal("late result from the old run rewrote the failure marker")
	}

	// Complete the resumed run successfully.
	reportResult(t, ec, consumer, "docker", true, plan.RunID, redeploy)
	_, finalStatus := driverMessage(t, drivers["status"], "update", plan.RunID, "4")
	reportResult(t, ec, consumer, "status", true, plan.RunID, finalStatus)
	finished, _ := ec.ResourceModel.FindOne(resource.Name, resource.Resource)
	if finished.Metadata["pipeline_failure"] != nil {
		t.Fatal("resumed run did not clear the failure marker on success")
	}
}

func TestResumeRestartsEarlierWhenPipelineChanged(t *testing.T) {
	ec, consumer, drivers := testEngine(t)
	resource := insertResource(t, ec, "changed-pipeline-app")
	driveToFailure(t, ec, consumer, drivers, resource, "update")

	// Add a notify driver consumer, then change step 2's driver.
	if _, err := ec.NatsContext.JetStream.CreateOrUpdateConsumer(context.Background(), "messages", jetstream.ConsumerConfig{
		Name: "notify", FilterSubject: "drivers.notify.resources.gitop", AckPolicy: jetstream.AckExplicitPolicy,
	}); err != nil {
		t.Fatal(err)
	}
	notifyConsumer, err := ec.NatsContext.JetStream.Consumer(context.Background(), "messages", "notify")
	if err != nil {
		t.Fatal(err)
	}
	drivers["notify"] = notifyConsumer

	changed := mustPipeline(t, ec, "gitops")
	changed.Steps[1].Driver = "notify"
	if err := ec.PipelineModel.UpdatePipeline(changed); err != nil {
		t.Fatal(err)
	}

	plan, err := ec.ResourceModel.PreparePipelineResume(resource.Name, resource.Resource, changed)
	if err != nil {
		t.Fatal(err)
	}
	if plan.RestartIndex != 1 || plan.ResumeStepID != "2" {
		t.Fatalf("changing step 2 must restart from step 2, got %+v", plan)
	}
	if err := PublishResumeEvent(plan.RunID, plan.Resource, ec.NatsContext.JetStream); err != nil {
		t.Fatal(err)
	}
	processEvent(t, ec, consumer)
	message, _ := driverMessage(t, drivers["notify"], "update", plan.RunID, "2")
	if message.RunID != plan.RunID {
		t.Fatalf("resume did not redispatch step 2 to notify: %+v", message)
	}
}

func TestResumeRejectsStaleVersionAndMissingStep(t *testing.T) {
	t.Run("version moved on after a PUT", func(t *testing.T) {
		ec, consumer, drivers := testEngine(t)
		resource := insertResource(t, ec, "stale-app")
		driveToFailure(t, ec, consumer, drivers, resource, "update")
		stored, _ := ec.ResourceModel.FindOne(resource.Name, resource.Resource)
		if _, err := ec.ResourceModel.Update(resource.Name, resource.Resource, stored); err != nil {
			t.Fatal(err)
		}
		if _, err := ec.ResourceModel.PreparePipelineResume(resource.Name, resource.Resource, mustPipeline(t, ec, "gitops")); !errors.Is(err, models.ErrFailureVersionStale) {
			t.Fatalf("want ErrFailureVersionStale, got %v", err)
		}
	})

	t.Run("failed step removed with an unchanged prefix", func(t *testing.T) {
		ec, consumer, drivers := testEngine(t)
		resource := insertResource(t, ec, "removed-step-app")
		driveToFailure(t, ec, consumer, drivers, resource, "update")
		shrunk := &types.Pipeline{Name: "gitops", Resource: "gitop", Steps: []types.Step{
			{ID: "1", Driver: "buildpacks"}, {ID: "2", Driver: "status"}, {ID: "4", Driver: "status"},
		}}
		if err := ec.PipelineModel.UpdatePipeline(shrunk); err != nil {
			t.Fatal(err)
		}
		if _, err := ec.ResourceModel.PreparePipelineResume(resource.Name, resource.Resource, shrunk); !errors.Is(err, models.ErrResumeStepRemoved) {
			t.Fatalf("want ErrResumeStepRemoved, got %v", err)
		}
	})

	t.Run("resource without a failure marker cannot resume", func(t *testing.T) {
		ec, _, _ := testEngine(t)
		plain := insertResource(t, ec, "plain-app")
		if _, err := ec.ResourceModel.PreparePipelineResume(plain.Name, plain.Resource, mustPipeline(t, ec, "gitops")); !errors.Is(err, models.ErrNoPipelineFailure) {
			t.Fatalf("want ErrNoPipelineFailure, got %v", err)
		}
	})
}

func TestResumeDispatchFailureRestoresMarker(t *testing.T) {
	ec, consumer, drivers := testEngine(t)
	resource := insertResource(t, ec, "dispatch-fail-app")
	driveToFailure(t, ec, consumer, drivers, resource, "update")

	plan, err := ec.ResourceModel.PreparePipelineResume(resource.Name, resource.Resource, mustPipeline(t, ec, "gitops"))
	if err != nil {
		t.Fatal(err)
	}
	// The resume event itself is accepted on the pipeline stream; the engine
	// then fails to dispatch to the driver stream, which we remove.
	if err := PublishResumeEvent(plan.RunID, plan.Resource, ec.NatsContext.JetStream); err != nil {
		t.Fatalf("resume event should be accepted: %v", err)
	}
	if err := ec.NatsContext.JetStream.DeleteStream(context.Background(), "messages"); err != nil {
		t.Fatal(err)
	}
	processEvent(t, ec, consumer)
	stored, _ := ec.ResourceModel.FindOne(resource.Name, resource.Resource)
	var failure models.PipelineFailure
	if err := decodeEngineMeta(stored.Metadata["pipeline_failure"], &failure); err != nil {
		t.Fatalf("a failed resume dispatch must restore the failure marker: %v", err)
	}
	if failure.RunID != plan.RunID {
		t.Fatalf("restored marker references run %q, want %q", failure.RunID, plan.RunID)
	}
}

func TestDeletionFailureDoesNotWritePipelineFailure(t *testing.T) {
	ec, consumer, drivers := testEngine(t)
	resource := insertResource(t, ec, "delete-app")
	deleting, err := ec.ResourceModel.BeginDeletion(resource.Name, resource.Resource)
	if err != nil {
		t.Fatal(err)
	}
	// Drive the real delete flow: publish the delete event and process its init.
	runID, err := PublishResourceEvent("delete", deleting, ec.NatsContext.JetStream)
	if err != nil {
		t.Fatal(err)
	}
	processEvent(t, ec, consumer)
	_, build := driverMessage(t, drivers["buildpacks"], "delete", runID, "1")
	reportResult(t, ec, consumer, "buildpacks", false, runID, build)

	stored, _ := ec.ResourceModel.FindOne(resource.Name, resource.Resource)
	if stored.Metadata["pipeline_failure"] != nil {
		t.Fatal("deletion failure must not write pipeline_failure")
	}
	if stored.Metadata["deletion_failed"] != true {
		t.Fatal("deletion failure must keep using deletion_failed")
	}
}

func mustPipeline(t *testing.T, ec *EngineContext, name string) *types.Pipeline {
	t.Helper()
	pipeline, err := ec.PipelineModel.GetPipeline(name)
	if err != nil {
		t.Fatal(err)
	}
	return pipeline
}

func decodeEngineMeta(value, target interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}
