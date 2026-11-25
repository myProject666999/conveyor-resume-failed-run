package models_test

import (
	"encoding/json"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/open-ug/conveyor/internal/models"
	"github.com/open-ug/conveyor/pkg/types"
)

type fakeClock struct {
	now time.Time
}

func (f fakeClock) Now() time.Time { return f.now }

// decodeMeta round-trips a metadata value (map[string]interface{} after JSON)
// into a typed target.
func decodeMeta(value, target interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, target)
}

var resourceCounter int64

func uniqueName() string {
	return "app-" + strconv.FormatInt(atomic.AddInt64(&resourceCounter, 1), 10)
}

func gitopsPipeline() *types.Pipeline {
	return &types.Pipeline{Name: "gitops", Resource: "gitop", Steps: []types.Step{
		{ID: "1", Name: "Build", Driver: "buildpacks"},
		{ID: "2", Name: "Status", Driver: "status"},
		{ID: "3", Name: "Deploy", Driver: "docker"},
		{ID: "4", Name: "Status", Driver: "status"},
	}}
}

// failedResource drives a resource through two successful steps and a failure
// at step 3, returning the model, the stored resource and the old run id.
func failedResource(t *testing.T, db *badger.DB, clock models.Clock) (*models.ResourceModel, types.Resource, string) {
	t.Helper()
	model := models.NewResourceModel(db)
	model.Clock = clock
	resource := types.Resource{
		ID: "res-" + strconv.FormatInt(atomic.AddInt64(&resourceCounter, 1), 10), Name: uniqueName(), Resource: "gitop", Pipeline: "gitops",
		Metadata: map[string]interface{}{"label": "keep"},
	}
	data, _ := json.Marshal(resource)
	if err := model.Insert(resource.Name, resource.Resource, data); err != nil {
		t.Fatal(err)
	}
	runID := "run-original"
	if _, err := model.StartPipelineRun(resource.Name, resource.Resource, runID, "update", "1"); err != nil {
		t.Fatal(err)
	}
	if err := model.RecordStepResult(resource.Name, resource.Resource, "1", "buildpacks", map[string]interface{}{"image": "abc"}); err != nil {
		t.Fatal(err)
	}
	if err := model.RecordStepResult(resource.Name, resource.Resource, "2", "status", map[string]interface{}{"state": "built"}); err != nil {
		t.Fatal(err)
	}
	// The failed step still lands in the driver-keyed view, as the engine does.
	if err := model.SaveDriverResult(resource.Name, resource.Resource, "docker", map[string]interface{}{"success": false}); err != nil {
		t.Fatal(err)
	}
	if err := model.MarkPipelineFailure(resource.Name, resource.Resource, runID, "3", "deploy failed"); err != nil {
		t.Fatal(err)
	}
	stored, err := model.FindOne(resource.Name, resource.Resource)
	if err != nil {
		t.Fatal(err)
	}
	return model, stored, runID
}

func TestMarkPipelineFailureWritesFullRecord(t *testing.T) {
	db := setupTestDB(t)
	fixed := time.Date(2026, 11, 26, 10, 0, 0, 0, time.UTC)
	model, stored, runID := failedResource(t, db, fakeClock{now: fixed})

	var failure models.PipelineFailure
	if err := decodeMeta(stored.Metadata["pipeline_failure"], &failure); err != nil {
		t.Fatal(err)
	}
	if failure.RunID != runID || failure.StepID != "3" || failure.Message != "deploy failed" {
		t.Fatalf("unexpected failure record: %+v", failure)
	}
	if failure.Version != "1" {
		t.Fatalf("failure version snapshot = %q, want 1", failure.Version)
	}
	if failure.FailedAt != fixed.Format(time.RFC3339Nano) {
		t.Fatalf("failed_at = %q, want injected clock time", failure.FailedAt)
	}

	// A late failure from a superseded run must not overwrite the marker.
	if err := model.MarkPipelineFailure(stored.Name, stored.Resource, "stale-run", "9", "late"); err != nil {
		t.Fatal(err)
	}
	again, _ := model.FindOne(stored.Name, stored.Resource)
	var unchanged models.PipelineFailure
	if err := decodeMeta(again.Metadata["pipeline_failure"], &unchanged); err != nil {
		t.Fatal(err)
	}
	if unchanged.RunID != runID {
		t.Fatalf("stale run overwrote failure marker: %+v", unchanged)
	}
}

func TestPrepareResumeSentinelMatrix(t *testing.T) {
	db := setupTestDB(t)
	clock := fakeClock{now: time.Now()}
	pipeline := gitopsPipeline()

	t.Run("no failure on fresh resource", func(t *testing.T) {
		model := models.NewResourceModel(db)
		model.Clock = clock
		res := types.Resource{Name: "fresh", Resource: "gitop", Metadata: map[string]interface{}{}}
		data, _ := json.Marshal(res)
		if err := model.Insert(res.Name, res.Resource, data); err != nil {
			t.Fatal(err)
		}
		if _, err := model.PreparePipelineResume(res.Name, res.Resource, pipeline); !errors.Is(err, models.ErrNoPipelineFailure) {
			t.Fatalf("want ErrNoPipelineFailure, got %v", err)
		}
	})

	t.Run("deleting resource", func(t *testing.T) {
		model, stored, _ := failedResource(t, db, clock)
		if _, err := model.BeginDeletion(stored.Name, stored.Resource); err != nil {
			t.Fatal(err)
		}
		if _, err := model.PreparePipelineResume(stored.Name, stored.Resource, pipeline); !errors.Is(err, models.ErrResourceDeleting) {
			t.Fatalf("want ErrResourceDeleting, got %v", err)
		}
	})

	t.Run("stale version after PUT", func(t *testing.T) {
		model, stored, _ := failedResource(t, db, clock)
		if _, err := model.Update(stored.Name, stored.Resource, stored); err != nil {
			t.Fatal(err)
		}
		if _, err := model.PreparePipelineResume(stored.Name, stored.Resource, pipeline); !errors.Is(err, models.ErrFailureVersionStale) {
			t.Fatalf("want ErrFailureVersionStale, got %v", err)
		}
	})

	t.Run("unparseable version fails closed", func(t *testing.T) {
		model, stored, _ := failedResource(t, db, clock)
		tampered, _ := model.FindOne(stored.Name, stored.Resource)
		tampered.Metadata["version"] = "not-a-number"
		raw, _ := json.Marshal(tampered)
		if err := db.Update(func(txn *badger.Txn) error {
			return txn.Set([]byte("/resources/gitop/"+stored.Name), raw)
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := model.PreparePipelineResume(stored.Name, stored.Resource, pipeline); !errors.Is(err, models.ErrFailureVersionStale) {
			t.Fatalf("want ErrFailureVersionStale for bad version, got %v", err)
		}
	})
}

func TestPrepareResumeKeepsTrustedPrefix(t *testing.T) {
	db := setupTestDB(t)
	clock := fakeClock{now: time.Now()}
	model, stored, oldRun := failedResource(t, db, clock)

	plan, err := model.PreparePipelineResume(stored.Name, stored.Resource, gitopsPipeline())
	if err != nil {
		t.Fatal(err)
	}
	if plan.RunID == oldRun || plan.RunID == "" {
		t.Fatalf("resume must use a fresh run id, got %q", plan.RunID)
	}
	if plan.ResumeStepID != "3" || plan.ResumeEvent != "update" || plan.RestartIndex != 2 {
		t.Fatalf("unexpected plan: %+v", plan)
	}

	current, _ := model.FindOne(stored.Name, stored.Resource)
	if current.Metadata["pipeline_failure"] != nil {
		t.Fatal("pipeline_failure was not cleared")
	}
	if current.Metadata["current_pipeline_run"] != plan.RunID {
		t.Fatal("current run id not updated")
	}
	if current.Metadata["current_pipeline_step"] != "3" {
		t.Fatalf("current step = %v, want 3", current.Metadata["current_pipeline_step"])
	}
	results := current.Metadata["driverresults"].(map[string]interface{})
	if _, ok := results["buildpacks"]; !ok {
		t.Fatal("buildpacks result was lost")
	}
	if _, ok := results["status"]; !ok {
		t.Fatal("status result was lost")
	}
	if _, ok := results["docker"]; ok {
		t.Fatal("failed step docker result must be discarded")
	}
	signatures := mustSignatures(t, current.Metadata["pipeline_step_signatures"])
	if len(signatures) != 2 {
		t.Fatalf("signatures = %d, want 2 retained", len(signatures))
	}
}

func TestPrepareResumeFromFirstStepFailure(t *testing.T) {
	db := setupTestDB(t)
	model := models.NewResourceModel(db)
	model.Clock = fakeClock{now: time.Now()}
	resource := types.Resource{ID: "first-fail", Name: uniqueName(), Resource: "gitop", Pipeline: "gitops", Metadata: map[string]interface{}{}}
	data, _ := json.Marshal(resource)
	if err := model.Insert(resource.Name, resource.Resource, data); err != nil {
		t.Fatal(err)
	}
	if _, err := model.StartPipelineRun(resource.Name, resource.Resource, "run-1", "create", "1"); err != nil {
		t.Fatal(err)
	}
	if err := model.SaveDriverResult(resource.Name, resource.Resource, "buildpacks", map[string]interface{}{"success": false}); err != nil {
		t.Fatal(err)
	}
	if err := model.MarkPipelineFailure(resource.Name, resource.Resource, "run-1", "1", "build failed"); err != nil {
		t.Fatal(err)
	}

	plan, err := model.PreparePipelineResume(resource.Name, resource.Resource, gitopsPipeline())
	if err != nil {
		t.Fatalf("first-step failure should still be resumable: %v", err)
	}
	if plan.RestartIndex != 0 || plan.ResumeStepID != "1" || plan.ResumeEvent != "create" {
		t.Fatalf("want restart from step 1 with create event, got %+v", plan)
	}
	current, _ := model.FindOne(resource.Name, resource.Resource)
	if results := current.Metadata["driverresults"].(map[string]interface{}); len(results) != 0 {
		t.Fatalf("no trusted prefix should remain, got %v", results)
	}
	if sigs := mustSignatures(t, current.Metadata["pipeline_step_signatures"]); len(sigs) != 0 {
		t.Fatalf("want no retained signatures, got %v", sigs)
	}
}

func TestPrepareResumeRollsBackWhenPipelineChanged(t *testing.T) {
	db := setupTestDB(t)
	clock := fakeClock{now: time.Now()}

	t.Run("change step 2 driver restarts at step 2", func(t *testing.T) {
		model, stored, _ := failedResource(t, db, clock)
		changed := gitopsPipeline()
		changed.Steps[1].Driver = "notify"
		plan, err := model.PreparePipelineResume(stored.Name, stored.Resource, changed)
		if err != nil {
			t.Fatal(err)
		}
		if plan.RestartIndex != 1 || plan.ResumeStepID != "2" {
			t.Fatalf("want restart at step 2, got %+v", plan)
		}
		current, _ := model.FindOne(stored.Name, stored.Resource)
		results := current.Metadata["driverresults"].(map[string]interface{})
		if _, ok := results["buildpacks"]; !ok {
			t.Fatal("step 1 result must be retained")
		}
		if len(results) != 1 {
			t.Fatalf("only step 1 result should remain, got %v", results)
		}
	})

	t.Run("change only step 4 still resumes at step 3", func(t *testing.T) {
		model, stored, _ := failedResource(t, db, clock)
		changed := gitopsPipeline()
		changed.Steps[3].Driver = "notify"
		plan, err := model.PreparePipelineResume(stored.Name, stored.Resource, changed)
		if err != nil {
			t.Fatal(err)
		}
		if plan.RestartIndex != 2 || plan.ResumeStepID != "3" {
			t.Fatalf("want restart at step 3, got %+v", plan)
		}
	})

	t.Run("failed step removed with matching prefix is rejected", func(t *testing.T) {
		model, stored, _ := failedResource(t, db, clock)
		shrunk := &types.Pipeline{Name: "gitops", Resource: "gitop", Steps: []types.Step{
			{ID: "1", Driver: "buildpacks"}, {ID: "2", Driver: "status"}, {ID: "4", Driver: "status"},
		}}
		if _, err := model.PreparePipelineResume(stored.Name, stored.Resource, shrunk); !errors.Is(err, models.ErrResumeStepRemoved) {
			t.Fatalf("want ErrResumeStepRemoved, got %v", err)
		}
	})
}

func TestPrepareResumeRebuildsRepeatedDriverResults(t *testing.T) {
	db := setupTestDB(t)
	clock := fakeClock{now: time.Now()}
	model := models.NewResourceModel(db)
	model.Clock = clock
	resource := types.Resource{ID: "r", Name: "multi", Resource: "gitop", Metadata: map[string]interface{}{}}
	data, _ := json.Marshal(resource)
	if err := model.Insert(resource.Name, resource.Resource, data); err != nil {
		t.Fatal(err)
	}
	// Five steps; the shared "status" driver runs at step 2 and step 4.
	if _, err := model.StartPipelineRun(resource.Name, resource.Resource, "run-1", "update", "1"); err != nil {
		t.Fatal(err)
	}
	step2Result := map[string]interface{}{"state": "step-2"}
	step4Result := map[string]interface{}{"state": "step-4"}
	for _, rec := range []struct {
		id, driver string
		result     interface{}
	}{
		{"1", "buildpacks", map[string]interface{}{"image": "x"}},
		{"2", "status", step2Result},
		{"3", "docker", map[string]interface{}{"deployed": true}},
		{"4", "status", step4Result},
	} {
		if err := model.RecordStepResult(resource.Name, resource.Resource, rec.id, rec.driver, rec.result); err != nil {
			t.Fatal(err)
		}
	}
	if err := model.MarkPipelineFailure(resource.Name, resource.Resource, "run-1", "5", "webhook failed"); err != nil {
		t.Fatal(err)
	}
	// Changing step 3's driver forces restartIndex to 2, so the prefix keeps
	// step 2's status result and discards step 4's later status result.
	changed := &types.Pipeline{Name: "gitops", Resource: "gitop", Steps: []types.Step{
		{ID: "1", Driver: "buildpacks"}, {ID: "2", Driver: "status"},
		{ID: "3", Driver: "containerd"}, {ID: "4", Driver: "status"}, {ID: "5", Driver: "webhook"},
	}}
	plan, err := model.PreparePipelineResume(resource.Name, resource.Resource, changed)
	if err != nil {
		t.Fatal(err)
	}
	if plan.RestartIndex != 2 {
		t.Fatalf("restart index = %d, want 2", plan.RestartIndex)
	}
	current, _ := model.FindOne(resource.Name, resource.Resource)
	results := current.Metadata["driverresults"].(map[string]interface{})
	status, ok := results["status"].(map[string]interface{})
	if !ok {
		t.Fatalf("status result missing or wrong type: %v", results["status"])
	}
	if status["state"] != "step-2" {
		t.Fatalf("rebuilt status = %v, want the step-2 result, not step-4", status)
	}
}

func TestPrepareResumeIsMutuallyExclusive(t *testing.T) {
	db := setupTestDB(t)
	clock := fakeClock{now: time.Now()}
	_, stored, _ := failedResource(t, db, clock)
	pipeline := gitopsPipeline()

	var wg sync.WaitGroup
	results := make([]error, 2)
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			peer := models.NewResourceModel(db) // separate instance, shared package-level lock
			<-start
			_, results[idx] = peer.PreparePipelineResume(stored.Name, stored.Resource, pipeline)
		}(i)
	}
	close(start)
	wg.Wait()

	successes, conflicts := 0, 0
	for _, err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, models.ErrNoPipelineFailure):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent result: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("want exactly one success and one conflict, got successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestStartPipelineRunAndFinishClearBookkeeping(t *testing.T) {
	db := setupTestDB(t)
	clock := fakeClock{now: time.Now()}
	model, stored, _ := failedResource(t, db, clock)

	if err := model.FinishPipelineRun(stored.Name, stored.Resource); err != nil {
		t.Fatal(err)
	}
	finished, _ := model.FindOne(stored.Name, stored.Resource)
	if finished.Metadata["pipeline_failure"] != nil || finished.Metadata["pipeline_step_signatures"] != nil || finished.Metadata["pipeline_step_results"] != nil {
		t.Fatalf("FinishPipelineRun left bookkeeping behind: %+v", finished.Metadata)
	}
	if _, ok := finished.Metadata["driverresults"].(map[string]interface{}); !ok {
		t.Fatal("driverresults should be retained until the next run")
	}

	// Re-create failure state, then a fresh StartPipelineRun must clear it all.
	if err := model.RecordStepResult(stored.Name, stored.Resource, "1", "buildpacks", map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	if err := model.MarkPipelineFailure(stored.Name, stored.Resource, "run-x", "2", "boom"); err != nil {
		t.Fatal(err)
	}
	if _, err := model.StartPipelineRun(stored.Name, stored.Resource, "run-new", "update", "1"); err != nil {
		t.Fatal(err)
	}
	restarted, _ := model.FindOne(stored.Name, stored.Resource)
	for _, key := range []string{"pipeline_failure", "pipeline_step_signatures", "pipeline_step_results"} {
		if restarted.Metadata[key] != nil {
			t.Fatalf("StartPipelineRun did not clear %s", key)
		}
	}
	if results := restarted.Metadata["driverresults"].(map[string]interface{}); len(results) != 0 {
		t.Fatalf("fresh run inherited driver results: %v", results)
	}
	if restarted.Metadata["current_pipeline_run"] != "run-new" {
		t.Fatal("fresh run id not set")
	}
}

func mustSignatures(t *testing.T, value interface{}) []models.StepSignature {
	t.Helper()
	var signatures []models.StepSignature
	if err := decodeMeta(value, &signatures); err != nil {
		t.Fatal(err)
	}
	return signatures
}
