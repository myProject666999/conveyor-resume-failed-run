package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/gofiber/fiber/v2"
	natsServer "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/open-ug/conveyor/internal/models"
	"github.com/open-ug/conveyor/internal/utils"
	"github.com/open-ug/conveyor/pkg/types"
)

type resumeHarness struct {
	app      *fiber.App
	db       *badger.DB
	nats     *utils.NatsContext
	resource *models.ResourceModel
}

func setupResumeHarness(t *testing.T) resumeHarness {
	t.Helper()
	s, err := natsServer.NewServer(&natsServer.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	t.Cleanup(s.Shutdown)
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS did not start")
	}
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	natsCtx := &utils.NatsContext{NatsCon: nc, JetStream: js}
	if err := natsCtx.InitiateStreams(); err != nil {
		t.Fatal(err)
	}
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })

	handler := NewResourceHandler(natsCtx, db)
	pipeline := &types.Pipeline{Name: "gitops", Resource: "gitop", Steps: []types.Step{
		{ID: "1", Driver: "buildpacks"}, {ID: "2", Driver: "status"},
		{ID: "3", Driver: "docker"}, {ID: "4", Driver: "status"},
	}}
	if err := handler.PipelineModel.CreatePipeline(pipeline); err != nil {
		t.Fatal(err)
	}

	app := fiber.New()
	app.Post("/resources/:type/:name/resume", handler.ResumeResource)
	return resumeHarness{app: app, db: db, nats: natsCtx, resource: handler.ResourceModel}
}

func (h resumeHarness) failedResource(t *testing.T, name string) {
	t.Helper()
	res := types.Resource{ID: name + "-id", Name: name, Resource: "gitop", Pipeline: "gitops", Metadata: map[string]interface{}{}}
	raw, _ := json.Marshal(res)
	if err := h.resource.Insert(name, res.Resource, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := h.resource.StartPipelineRun(name, res.Resource, "run-1", "update", "1"); err != nil {
		t.Fatal(err)
	}
	if err := h.resource.RecordStepResult(name, res.Resource, "1", "buildpacks", map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	if err := h.resource.RecordStepResult(name, res.Resource, "2", "status", map[string]interface{}{}); err != nil {
		t.Fatal(err)
	}
	if err := h.resource.MarkPipelineFailure(name, res.Resource, "run-1", "3", "boom"); err != nil {
		t.Fatal(err)
	}
}

func (h resumeHarness) postResume(name string) (*http.Response, map[string]interface{}) {
	req := httptest.NewRequest(http.MethodPost, "/resources/gitop/"+name+"/resume", nil)
	resp, err := h.app.Test(req, -1)
	if err != nil {
		return resp, nil
	}
	body, _ := io.ReadAll(resp.Body)
	var parsed map[string]interface{}
	_ = json.Unmarshal(body, &parsed)
	return resp, parsed
}

func TestResumeHandlerAccepted(t *testing.T) {
	h := setupResumeHarness(t)
	h.failedResource(t, "resumable")

	resp, body := h.postResume("resumable")
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("want 202, got %d body=%v", resp.StatusCode, body)
	}
	runID, _ := body["runid"].(string)
	if runID == "" || runID == "run-1" {
		t.Fatalf("resume must return a fresh run id, got %v", body)
	}

	// The resume event reached the pipeline stream.
	consumer, err := h.nats.JetStream.CreateOrUpdateConsumer(t.Context(), "pipeline-engine", jetstream.ConsumerConfig{
		Name: "resume-check", AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatal(err)
	}
	batch, err := consumer.FetchNoWait(10)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for range batch.Messages() {
		count++
	}
	if count == 0 {
		t.Fatal("no resume event was published")
	}

	// A second resume now conflicts because the first consumed the failure marker.
	resp2, body2 := h.postResume("resumable")
	if resp2.StatusCode != http.StatusConflict || body2["reason"] != "no_failure" {
		t.Fatalf("second resume want 409 no_failure, got %d %v", resp2.StatusCode, body2)
	}
}

func TestResumeHandlerConflictReasons(t *testing.T) {
	t.Run("no failure marker", func(t *testing.T) {
		h := setupResumeHarness(t)
		res := types.Resource{Name: "plain", Resource: "gitop", Pipeline: "gitops", Metadata: map[string]interface{}{}}
		raw, _ := json.Marshal(res)
		if err := h.resource.Insert("plain", res.Resource, raw); err != nil {
			t.Fatal(err)
		}
		resp, body := h.postResume("plain")
		if resp.StatusCode != http.StatusConflict || body["reason"] != "no_failure" {
			t.Fatalf("want 409 no_failure, got %d %v", resp.StatusCode, body)
		}
	})

	t.Run("resource deleting", func(t *testing.T) {
		h := setupResumeHarness(t)
		h.failedResource(t, "deleting")
		if _, err := h.resource.BeginDeletion("deleting", "gitop"); err != nil {
			t.Fatal(err)
		}
		resp, body := h.postResume("deleting")
		if resp.StatusCode != http.StatusConflict || body["reason"] != "deleting" {
			t.Fatalf("want 409 deleting, got %d %v", resp.StatusCode, body)
		}
	})

	t.Run("stale version", func(t *testing.T) {
		h := setupResumeHarness(t)
		h.failedResource(t, "stale")
		stored, _ := h.resource.FindOne("stale", "gitop")
		if _, err := h.resource.Update("stale", "gitop", stored); err != nil {
			t.Fatal(err)
		}
		resp, body := h.postResume("stale")
		if resp.StatusCode != http.StatusConflict || body["reason"] != "version_stale" {
			t.Fatalf("want 409 version_stale, got %d %v", resp.StatusCode, body)
		}
	})

	t.Run("missing resource is 404", func(t *testing.T) {
		h := setupResumeHarness(t)
		resp, _ := h.postResume("ghost")
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("want 404, got %d", resp.StatusCode)
		}
	})
}

func TestResumeHandlerConcurrentRequestsAreExclusive(t *testing.T) {
	h := setupResumeHarness(t)
	h.failedResource(t, "concurrent")

	statuses := make([]int, 2)
	reasons := make([]string, 2)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			resp, body := h.postResume("concurrent")
			statuses[idx] = resp.StatusCode
			if reason, ok := body["reason"].(string); ok {
				reasons[idx] = reason
			}
		}(i)
	}
	close(start)
	wg.Wait()

	accepted, conflicts := 0, 0
	for i, status := range statuses {
		switch status {
		case http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			if reasons[i] != "no_failure" {
				t.Fatalf("conflict reason = %q, want no_failure", reasons[i])
			}
			conflicts++
		default:
			t.Fatalf("unexpected status %d", status)
		}
	}
	if accepted != 1 || conflicts != 1 {
		t.Fatalf("want exactly one 202 and one 409, got accepted=%d conflicts=%d", accepted, conflicts)
	}
}
