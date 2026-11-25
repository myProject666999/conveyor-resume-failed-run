package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/google/uuid"
	"github.com/open-ug/conveyor/pkg/types"
)

var resourceMutationLocks sync.Map

type resourceLockKey struct {
	db  *badger.DB
	key string
}

// Metadata keys used to track pipeline execution state.
const (
	metaVersion         = "version"
	metaDriverResults   = "driverresults"
	metaCurrentRun      = "current_pipeline_run"
	metaCurrentEvent    = "current_pipeline_event"
	metaCurrentStep     = "current_pipeline_step"
	metaPipelineFailure = "pipeline_failure"
	metaStepSignatures  = "pipeline_step_signatures"
	metaStepResults     = "pipeline_step_results"
)

// Sentinel errors for resume validation; handlers map these to 409 responses.
var (
	ErrNoPipelineFailure   = errors.New("resource has no pipeline failure to resume")
	ErrResourceDeleting    = errors.New("resource is being deleted; resume is not allowed")
	ErrFailureVersionStale = errors.New("resource changed since the failed run; trigger a new run")
	ErrResumeStepRemoved   = errors.New("failed pipeline step no longer exists with an unchanged prefix; trigger a new run")
)

// Clock allows tests to control timestamps written into resource metadata.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// PipelineFailure records where and why a non-deletion pipeline run stopped.
type PipelineFailure struct {
	RunID    string `json:"run_id"`
	StepID   string `json:"step_id"`
	Message  string `json:"message"`
	FailedAt string `json:"failed_at"`
	Version  string `json:"version"`
}

// StepSignature captures the identity of a successfully completed step so a
// resume can detect pipeline definition changes made after the failure.
type StepSignature struct {
	StepID string `json:"step_id"`
	Driver string `json:"driver"`
}

// StepResultRecord is the ordered, per-step source of truth for driver results.
// driverresults remains a driver-keyed view rebuilt from these records.
type StepResultRecord struct {
	StepID string      `json:"step_id"`
	Driver string      `json:"driver"`
	Result interface{} `json:"result"`
}

// ResumePlan is the outcome of preparing a resource for a continued run.
type ResumePlan struct {
	Resource     types.Resource
	RunID        string
	ResumeStepID string
	ResumeEvent  string
	RestartIndex int
}

func (m *ResourceModel) lockMutation(name, resourceType string) func() {
	value, _ := resourceMutationLocks.LoadOrStore(resourceLockKey{m.DB, m.key(name, resourceType)}, &sync.Mutex{})
	mutex := value.(*sync.Mutex)
	mutex.Lock()
	return mutex.Unlock
}

func (m *ResourceModel) clock() Clock {
	if m.Clock != nil {
		return m.Clock
	}
	return realClock{}
}

// persist marshals and writes a resource back under its primary key.
func (m *ResourceModel) persist(name, resourceType string, resource types.Resource) error {
	data, err := json.Marshal(resource)
	if err != nil {
		return fmt.Errorf("failed to marshal updated resource: %v", err)
	}
	return m.DB.Update(func(txn *badger.Txn) error {
		return txn.Set([]byte(m.key(name, resourceType)), data)
	})
}

// decodeMetadataValue converts a metadata value (which JSON round-tripping may
// represent as map[string]interface{}/[]interface{}) into a typed target.
func decodeMetadataValue(value, target interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

func metaString(value interface{}) string {
	if s, ok := value.(string); ok {
		return s
	}
	return ""
}

type ResourceModel struct {
	DB    *badger.DB
	Clock Clock
}

func NewResourceModel(db *badger.DB) *ResourceModel {
	return &ResourceModel{
		DB:    db,
		Clock: realClock{},
	}
}

// key generates a unique key for a resource based on its name and type.
func (m *ResourceModel) key(name string, resourceType string) string {
	return fmt.Sprintf("/resources/%s/%s", resourceType, name)
}

// Insert adds a new resource to the etcd store.
// It returns an error if a resource with the same name and type already exists.

func (m *ResourceModel) Insert(name string, resourceType string, resource []byte) error {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	key := []byte(m.key(name, resourceType))

	// first unmarshal to resource inorder to set the version 1
	var real_resource types.Resource

	err := json.Unmarshal(resource, &real_resource)
	if err != nil {
		return fmt.Errorf("failed to unmarshal resource: %v", err)
	}

	setResourceVersion(&real_resource, 1)

	// marshal back to json
	resourceData, err := json.Marshal(real_resource)
	if err != nil {
		return fmt.Errorf("failed to marshal resource: %v", err)
	}

	// check existence
	err = m.DB.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			return fmt.Errorf("resource with name %s and type %s already exists", name, resourceType)
		}
		if err != badger.ErrKeyNotFound {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	// insert resource
	err = m.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, resourceData)
	})
	if err != nil {
		return fmt.Errorf("failed to insert resource: %v", err)
	}

	// insert versioned resource
	versionedKey := []byte(m.key(name, resourceType) + "/1")
	return m.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(versionedKey, resourceData)
	})
}

// FindOne retrieves a single resource by its name and type.
// It returns the resource data as a byte slice or an error if not found.
func (m *ResourceModel) FindOne(name string, resourceType string) (types.Resource, error) {
	key := []byte(m.key(name, resourceType))

	var resource types.Resource
	err := m.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("resource with name %s and type %s not found: %w", name, resourceType, badger.ErrKeyNotFound)
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &resource)
		})
	})
	if err != nil {
		return types.Resource{}, err
	}

	return resource, nil
}

// Delete removes a resource by its name and type.
// It returns an error if the resource does not exist.
func (m *ResourceModel) Delete(name string, resourceType string) error {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	return m.deleteResource(name, resourceType)
}

// Finalize only the active deletion run, never an outdated retry result.
func (m *ResourceModel) FinishDeletion(name, resourceType, runID string) error {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return err
	}
	if resource.Metadata["deletion_runid"] != runID {
		return errors.New("outdated deletion result")
	}
	return m.deleteResource(name, resourceType)
}

func (m *ResourceModel) deleteResource(name, resourceType string) error {
	key := []byte(m.key(name, resourceType))

	return m.DB.Update(func(txn *badger.Txn) error {
		// Remove snapshots too: these may contain environment secrets.
		prefix := append(append([]byte{}, key...), '/')
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			if err := txn.Delete(it.Item().KeyCopy(nil)); err != nil {
				return err
			}
		}
		return txn.Delete(key)
	})
}

// List retrieves all resources of a specific type.
// It returns a slice of resource names or an error if the operation fails.
func (m *ResourceModel) List(resourceType string) ([]types.Resource, error) {
	prefix := []byte(fmt.Sprintf("/resources/%s/", resourceType))

	var resources []types.Resource

	err := m.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			key := string(item.Key())

			// Skip versioned resources like:
			// /resources/pipe4/pipeline-1/1
			remaining := strings.TrimPrefix(key, string(prefix))
			if strings.Contains(remaining, "/") {
				continue
			}

			err := item.Value(func(val []byte) error {
				var res types.Resource

				if err := json.Unmarshal(val, &res); err != nil {
					return fmt.Errorf("failed to unmarshal resource: %v", err)
				}

				resources = append(resources, res)
				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list resources: %v", err)
	}

	return resources, nil
}

func getResourceVersion(resource types.Resource) (int, error) {
	if resource.Metadata == nil {
		return 0, fmt.Errorf("resource metadata is nil")
	}

	versionStr, ok := resource.Metadata["version"].(string)
	if !ok {
		return 0, fmt.Errorf("version not found in resource metadata or is not a string")
	}

	version, err := strconv.Atoi(versionStr)
	if err != nil {
		return 0, fmt.Errorf("failed to parse version: %v", err)
	}

	return version, nil
}

func setResourceVersion(resource *types.Resource, version int) {
	if resource.Metadata == nil {
		resource.Metadata = make(map[string]interface{})
	}
	resource.Metadata["version"] = strconv.Itoa(version)
}

// Update modifies an existing resource's data.
// It returns an error if the resource does not exist.
func (m *ResourceModel) Update(name string, resourceType string, resource types.Resource) (types.Resource, error) {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	key := []byte(m.key(name, resourceType))

	// Check existence
	currentResource, err := m.FindOne(name, resourceType)
	if err != nil {
		return types.Resource{}, fmt.Errorf("resource with name %s and type %s not found: %v ", name, resourceType, err)
	}

	if currentResource.Metadata["deleting"] == true {
		return types.Resource{}, errors.New("resource is being deleted; retry deletion instead of updating")
	}
	resource.ID = currentResource.ID // Ensure the ID remains unchanged
	version, err := getResourceVersion(currentResource)
	if err != nil {
		return types.Resource{}, fmt.Errorf("failed to get current resource version: %v", err)
	}

	setResourceVersion(&currentResource, version+1)

	resource.Metadata = currentResource.Metadata // Preserve existing metadata and versioning

	// Marshal the updated resource to JSON
	resourceData, err := json.Marshal(resource)
	if err != nil {
		return types.Resource{}, fmt.Errorf("failed to marshal resource: %v", err)
	}

	err = m.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, resourceData)
	})
	if err != nil {
		return types.Resource{}, fmt.Errorf("failed to update resource: %v", err)
	}

	// save versioned resource
	versionedKey := []byte(fmt.Sprintf("%s/%s", key, resource.Metadata["version"]))
	err = m.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(versionedKey, resourceData)
	})
	if err != nil {
		return types.Resource{}, fmt.Errorf("failed to save versioned resource: %v", err)
	}

	return resource, nil
}

// FindAll retrieves all resources of a specific type.
// It returns a slice of resource names or an error if no resources are found.
func (m *ResourceModel) FindAll(resourceType string) ([]types.Resource, error) {
	prefix := []byte(fmt.Sprintf("/resources/%s/", resourceType))

	var resources []types.Resource
	err := m.DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false

		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()

			key := string(item.Key())

			// Skip versioned resources like:
			// /resources/pipe4/pipeline-1/1
			remaining := strings.TrimPrefix(key, string(prefix))
			if strings.Contains(remaining, "/") {
				continue
			}

			err := item.Value(func(val []byte) error {
				var res types.Resource

				if err := json.Unmarshal(val, &res); err != nil {
					return fmt.Errorf("failed to unmarshal resource: %v", err)
				}

				resources = append(resources, res)
				return nil
			})

			if err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to find resources: %v", err)
	}

	if len(resources) == 0 {
		return nil, fmt.Errorf("no resources of type %s found", resourceType)
	}

	return resources, nil
}

func (m *ResourceModel) FindByVersion(name string, resourceType string, version string) (types.Resource, error) {
	key := []byte(fmt.Sprintf("/resources/%s/%s/%s", resourceType, name, version))

	var resource types.Resource
	err := m.DB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return fmt.Errorf("resource with name %s, type %s and version %s not found", name, resourceType, version)
			}
			return err
		}

		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &resource)
		})
	})
	if err != nil {
		return types.Resource{}, err
	}

	return resource, nil
}

// / A function that saves the driver result. This data is then stored in the metadata.driverresults.[driver] field of the resource and is arbitrary data types
func (m *ResourceModel) SaveDriverResult(name string, resourceType string, driver string, result interface{}) error {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	// Retrieve the current resource
	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return fmt.Errorf("failed to find resource: %v", err)
	}

	// Update the resource's metadata with the driver result
	err = updateDriverResult(&resource, driver, result)
	if err != nil {
		return fmt.Errorf("failed to update driver result: %v", err)
	}

	// Marshal the updated resource to JSON
	resourceData, err := json.Marshal(resource)
	if err != nil {
		return fmt.Errorf("failed to marshal updated resource: %v", err)
	}

	// Save the updated resource back to BadgerDB
	key := []byte(m.key(name, resourceType))
	return m.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, resourceData)
	})
}

func updateDriverResult(resource *types.Resource, driver string, result interface{}) error {
	// generate metadata object for driver results
	driverResults := make(map[string]interface{})
	if resource.Metadata != nil {
		if existingResults, ok := resource.Metadata["driverresults"]; ok {
			if existingResultsMap, ok := existingResults.(map[string]interface{}); ok {
				driverResults = existingResultsMap
			}
		}
	}

	// Update the driver result
	driverResults[driver] = result

	// set the updated driver results back to resource metadata
	if resource.Metadata == nil {
		resource.Metadata = make(map[string]interface{})
	}

	resource.Metadata["driverresults"] = driverResults

	return nil
}

func (m *ResourceModel) SetCurrentPipelineStep(name string, resourceType string, stepID string) error {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	// Retrieve the current resource
	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return fmt.Errorf("failed to find resource: %v", err)
	}

	// set the current pipeline step
	if resource.Metadata == nil {
		resource.Metadata = make(map[string]interface{})
	}

	resource.Metadata["current_pipeline_step"] = stepID

	// Marshal the updated resource to JSON
	resourceData, err := json.Marshal(resource)
	if err != nil {
		return fmt.Errorf("failed to marshal updated resource: %v", err)
	}

	// Save the updated resource back to BadgerDB
	key := []byte(m.key(name, resourceType))
	return m.DB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, resourceData)
	})
}

// StartPipelineRun resets execution metadata without discarding resource metadata.
// Driver results belong to one run; retaining them makes a retry inherit failures.
func (m *ResourceModel) StartPipelineRun(name, resourceType, runID, event, stepID string) (types.Resource, error) {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	var resource types.Resource
	key := []byte(m.key(name, resourceType))
	err := m.DB.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		if err := item.Value(func(data []byte) error { return json.Unmarshal(data, &resource) }); err != nil {
			return err
		}
		if resource.Metadata == nil {
			resource.Metadata = make(map[string]interface{})
		}
		resource.Metadata[metaDriverResults] = make(map[string]interface{})
		resource.Metadata[metaCurrentRun] = runID
		resource.Metadata[metaCurrentEvent] = event
		resource.Metadata[metaCurrentStep] = stepID
		// A fresh run must not inherit a previous failure or step bookkeeping.
		delete(resource.Metadata, metaPipelineFailure)
		delete(resource.Metadata, metaStepSignatures)
		delete(resource.Metadata, metaStepResults)
		data, err := json.Marshal(resource)
		if err != nil {
			return err
		}
		return txn.Set(key, data)
	})
	return resource, err
}

func (m *ResourceModel) GetCurrentPipelineStep(name string, resourceType string) (string, error) {
	// Retrieve the current resource
	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return "", fmt.Errorf("failed to find resource: %v", err)
	}

	// get the current pipeline step
	if resource.Metadata == nil {
		return "", fmt.Errorf("resource metadata is nil")
	}

	stepID, ok := resource.Metadata["current_pipeline_step"].(string)
	if !ok {
		return "", fmt.Errorf("current pipeline step not found in resource metadata or is not a string")
	}

	return stepID, nil
}

// BeginDeletion keeps the resource available until every cleanup stage succeeds.
func (m *ResourceModel) BeginDeletion(name, resourceType string) (types.Resource, error) {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return resource, err
	}
	if resource.Metadata == nil {
		resource.Metadata = map[string]interface{}{}
	}
	if DeletionRunning(resource) {
		return resource, nil
	}
	resource.Metadata["deleting"] = true
	resource.Metadata["deletion_runid"] = uuid.New().String()
	resource.Metadata["deletion_started_at"] = m.clock().Now().UTC().Format(time.RFC3339Nano)
	delete(resource.Metadata, "deletion_failed")
	data, err := json.Marshal(resource)
	if err != nil {
		return resource, err
	}
	err = m.DB.Update(func(txn *badger.Txn) error { return txn.Set([]byte(m.key(name, resourceType)), data) })
	return resource, err
}

func (m *ResourceModel) MarkDeletionFailed(name, resourceType string) error {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return err
	}
	resource.Metadata["deletion_failed"] = true
	data, err := json.Marshal(resource)
	if err != nil {
		return err
	}
	return m.DB.Update(func(txn *badger.Txn) error { return txn.Set([]byte(m.key(name, resourceType)), data) })
}

// Stalled cleanup can be restarted after a node crash or lost result.
func DeletionRunning(resource types.Resource) bool {
	if resource.Metadata["deleting"] != true || resource.Metadata["deletion_failed"] == true {
		return false
	}
	started, _ := resource.Metadata["deletion_started_at"].(string)
	at, err := time.Parse(time.RFC3339Nano, started)
	return err == nil && time.Since(at) < 2*time.Minute
}

// MarkPipelineFailure records a failed non-deletion step. It is a no-op when the
// stored run no longer matches, so late results from a superseded run cannot
// overwrite the current run's state.
func (m *ResourceModel) MarkPipelineFailure(name, resourceType, runID, stepID, message string) error {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return err
	}
	if current, ok := resource.Metadata[metaCurrentRun].(string); ok && current != runID {
		return nil
	}
	if resource.Metadata == nil {
		resource.Metadata = make(map[string]interface{})
	}
	failure := PipelineFailure{
		RunID:    runID,
		StepID:   stepID,
		Message:  message,
		FailedAt: m.clock().Now().UTC().Format(time.RFC3339Nano),
		Version:  metaString(resource.Metadata[metaVersion]),
	}
	resource.Metadata[metaPipelineFailure] = failure
	return m.persist(name, resourceType, resource)
}

// ClearPipelineFailure removes any failure marker without touching other state.
func (m *ResourceModel) ClearPipelineFailure(name, resourceType string) error {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return err
	}
	delete(resource.Metadata, metaPipelineFailure)
	return m.persist(name, resourceType, resource)
}

// FinishPipelineRun clears run bookkeeping after a fully successful run. Driver
// results are retained until the next StartPipelineRun, matching prior behavior.
func (m *ResourceModel) FinishPipelineRun(name, resourceType string) error {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return err
	}
	delete(resource.Metadata, metaPipelineFailure)
	delete(resource.Metadata, metaStepSignatures)
	delete(resource.Metadata, metaStepResults)
	return m.persist(name, resourceType, resource)
}

// RecordStepResult stores a successful step: the driver-keyed result view, the
// ordered per-step record, and the completion signature, in one transaction.
func (m *ResourceModel) RecordStepResult(name, resourceType, stepID, driver string, result interface{}) error {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()
	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return fmt.Errorf("failed to find resource: %v", err)
	}
	if resource.Metadata == nil {
		resource.Metadata = make(map[string]interface{})
	}

	driverResults := make(map[string]interface{})
	if existing, ok := resource.Metadata[metaDriverResults].(map[string]interface{}); ok {
		driverResults = existing
	}
	driverResults[driver] = result
	resource.Metadata[metaDriverResults] = driverResults

	var records []StepResultRecord
	if err := decodeMetadataValue(resource.Metadata[metaStepResults], &records); err != nil {
		return fmt.Errorf("failed to decode step results: %v", err)
	}
	upserted := false
	for i := range records {
		if records[i].StepID == stepID {
			records[i] = StepResultRecord{StepID: stepID, Driver: driver, Result: result}
			upserted = true
			break
		}
	}
	if !upserted {
		records = append(records, StepResultRecord{StepID: stepID, Driver: driver, Result: result})
	}
	resource.Metadata[metaStepResults] = records

	var signatures []StepSignature
	if err := decodeMetadataValue(resource.Metadata[metaStepSignatures], &signatures); err != nil {
		return fmt.Errorf("failed to decode step signatures: %v", err)
	}
	alreadySigned := false
	for _, sig := range signatures {
		if sig.StepID == stepID {
			alreadySigned = true
			break
		}
	}
	if !alreadySigned {
		signatures = append(signatures, StepSignature{StepID: stepID, Driver: driver})
	}
	resource.Metadata[metaStepSignatures] = signatures

	return m.persist(name, resourceType, resource)
}

// computeRestartIndex returns the position of the first step whose recorded
// signature no longer matches the current pipeline definition. A fully matching
// prefix yields len(signatures), i.e. execution resumes after the prefix.
func computeRestartIndex(signatures []StepSignature, steps []types.Step) int {
	for i, sig := range signatures {
		if i >= len(steps) || steps[i].ID != sig.StepID || steps[i].Driver != sig.Driver {
			return i
		}
	}
	return len(signatures)
}

// PreparePipelineResume validates a resume request and rewrites execution
// metadata so the run continues from the first step whose result cannot be
// trusted. The pipeline definition is supplied by the caller; this method only
// mutates resource state, all under the per-resource mutation lock.
func (m *ResourceModel) PreparePipelineResume(name, resourceType string, pipeline *types.Pipeline) (ResumePlan, error) {
	unlock := m.lockMutation(name, resourceType)
	defer unlock()

	resource, err := m.FindOne(name, resourceType)
	if err != nil {
		return ResumePlan{}, err
	}
	if resource.Metadata == nil {
		resource.Metadata = make(map[string]interface{})
	}
	if resource.Metadata["deleting"] == true {
		return ResumePlan{}, ErrResourceDeleting
	}

	var failure PipelineFailure
	if err := decodeMetadataValue(resource.Metadata[metaPipelineFailure], &failure); err != nil || failure.StepID == "" {
		return ResumePlan{}, ErrNoPipelineFailure
	}

	currentVersion, err := strconv.Atoi(metaString(resource.Metadata[metaVersion]))
	failedVersion, parseErr := strconv.Atoi(failure.Version)
	if err != nil || parseErr != nil || currentVersion > failedVersion {
		return ResumePlan{}, ErrFailureVersionStale
	}

	event, ok := resource.Metadata[metaCurrentEvent].(string)
	if !ok || event == "" {
		return ResumePlan{}, errors.New("missing pipeline operation for the failed run")
	}

	var signatures []StepSignature
	if err := decodeMetadataValue(resource.Metadata[metaStepSignatures], &signatures); err != nil {
		return ResumePlan{}, fmt.Errorf("failed to decode step signatures: %v", err)
	}
	restartIndex := computeRestartIndex(signatures, pipeline.Steps)
	if restartIndex >= len(pipeline.Steps) {
		return ResumePlan{}, ErrResumeStepRemoved
	}

	failedStepIndex := -1
	for i, step := range pipeline.Steps {
		if step.ID == failure.StepID {
			failedStepIndex = i
			break
		}
	}
	if failedStepIndex < 0 || failedStepIndex < restartIndex {
		return ResumePlan{}, ErrResumeStepRemoved
	}

	var records []StepResultRecord
	if err := decodeMetadataValue(resource.Metadata[metaStepResults], &records); err != nil {
		return ResumePlan{}, fmt.Errorf("failed to decode step results: %v", err)
	}
	if restartIndex < len(records) {
		records = records[:restartIndex]
	}
	// Rebuild the driver-keyed view from the trusted prefix only, so results
	// from repeated driver names cannot leak in from discarded steps.
	rebuiltResults := make(map[string]interface{})
	for _, record := range records {
		rebuiltResults[record.Driver] = record.Result
	}
	resource.Metadata[metaDriverResults] = rebuiltResults
	resource.Metadata[metaStepResults] = records
	if restartIndex < len(signatures) {
		signatures = signatures[:restartIndex]
	}
	resource.Metadata[metaStepSignatures] = signatures

	runID := uuid.New().String()
	delete(resource.Metadata, metaPipelineFailure)
	resource.Metadata[metaCurrentRun] = runID
	resource.Metadata[metaCurrentStep] = pipeline.Steps[restartIndex].ID

	if err := m.persist(name, resourceType, resource); err != nil {
		return ResumePlan{}, err
	}
	return ResumePlan{
		Resource:     resource,
		RunID:        runID,
		ResumeStepID: pipeline.Steps[restartIndex].ID,
		ResumeEvent:  event,
		RestartIndex: restartIndex,
	}, nil
}
