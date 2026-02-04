package cheek

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestLoadLogs(t *testing.T) {
	db, err := OpenDB("./tmp.sqlite3")
	if err != nil {
		t.Fatal(err)
	}
	cfg := NewConfig()
	cfg.DB = db

	l := NewLogger("debug", nil, os.Stdout, os.Stdout)

	j := &JobSpec{
		Cron:    "* * * * *",
		Name:    "test",
		Command: []string{"echo", "bar"},
		cfg:     cfg,
		log:     l,
	}

	_, err = j.ToYAML(false)
	if err != nil {
		t.Fatal(err)
	}

	_ = j.execCommandWithRetry(context.Background(), "test", nil)

	// log loading goes on job name basis
	// let's recreate and see if we can load logs

	j = &JobSpec{
		Name: "test",
		cfg:  cfg,
		log:  l,
	}

	j.loadRunsFromDb(10, false)

	assert.Greater(t, len(j.Runs), 0)
}

func TestJobRun(t *testing.T) {
	j := &JobSpec{
		Cron:    "* * * * *",
		Name:    "test",
		Command: []string{"echo", "bar"},
		cfg:     NewConfig(),
	}

	jobRun := JobRun{}

	// Execute command and get result
	jr := j.execCommand(context.Background(), jobRun, "test")

	// Dereference the pointer and compare the value
	assert.Equal(t, *jr.Status, 0)
}

func TestSpecialCron(t *testing.T) {
	j := &JobSpec{
		Cron:    "@10minutes",
		Name:    "test",
		Command: []string{"echo", "bar"},
		cfg:     NewConfig(),
	}

	if err := j.ValidateCron(); err != nil {
		t.Fatal(err)
	}

	jobRun := JobRun{}                                        // Create a JobRun instance
	jr := j.execCommand(context.Background(), jobRun, "test") // Pass JobRun instance and "test"
	assert.Equal(t, *jr.Status, 0)
}

func TestInvalidCron(t *testing.T) {
	j := &JobSpec{
		Cron:    "INVALID",
		Name:    "test",
		Command: []string{"echo", "bar"},
		cfg:     NewConfig(),
	}

	assert.Error(t, j.ValidateCron())

	j = &JobSpec{
		Cron:    "@1minutes",
		Name:    "test",
		Command: []string{"echo", "bar"},
		cfg:     NewConfig(),
	}

	assert.Error(t, j.ValidateCron())
}

func TestJobWithEnvVars(t *testing.T) {
	jobSpec := []byte(`
cron: "* * * * *"
command: env
env: 
  foo: bar
  coffee: bar
`)

	j := JobSpec{}
	err := yaml.Unmarshal(jobSpec, &j)
	if err != nil {
		log.Fatal(err)
	}

	if err := j.ValidateCron(); err != nil {
		t.Fatal(err)
	}

	_, ok := j.Env["foo"]
	if !ok {
		t.Fatal("should contain foo")
	}

	jobRun := JobRun{}                                        // Create a JobRun instance
	jr := j.execCommand(context.Background(), jobRun, "test") // Pass JobRun instance and "test"

	jr.flushLogBuffer()

	assert.Contains(t, jr.Log, "foo=bar")
}

func TestStdErrOut(t *testing.T) {
	cfg := NewConfig()
	cfg.SuppressLogs = true

	j := &JobSpec{
		Cron:    "* * * * *",
		Name:    "test",
		Command: []string{"sh", "-c", "echo stdout; echo stderr 1>&2"},
		//  1>&2 sends to stderr
		cfg: cfg,
	}

	jobRun := JobRun{}                                        // Create a JobRun instance
	jr := j.execCommand(context.Background(), jobRun, "test") // Pass JobRun instance and "test"
	jr.flushLogBuffer()
	assert.Contains(t, jr.Log, "stdout")
	assert.Contains(t, jr.Log, "stderr")
}

func TestFailingLog(t *testing.T) {
	cfg := NewConfig()
	cfg.SuppressLogs = true

	j := &JobSpec{
		Cron:    "* * * * *",
		Name:    "test",
		Command: []string{"this fails"},
		//  1>&2 sends to stderr
		cfg: cfg,
	}

	jobRun := JobRun{}                                        // Create a JobRun instance
	jr := j.execCommand(context.Background(), jobRun, "test") // Pass JobRun instance and "test"
	jr.flushLogBuffer()
	assert.Contains(t, jr.Log, "this fails")
}

func TestJobRunNoCommand(t *testing.T) {
	j := &JobSpec{
		Cron: "* * * * *",
		Name: "test",
		cfg:  NewConfig(),
	}

	jobRun := JobRun{}                                        // Create a JobRun instance
	jr := j.execCommand(context.Background(), jobRun, "test") // Pass JobRun instance and "test"
	assert.NotEqual(t, jr.Status, 0)
}

func TestJobNonZero(t *testing.T) {
	j := &JobSpec{
		Cron: "* * * * *",
		Name: "test",
		Command: []string{
			"la", "--moo",
		},
		cfg: NewConfig(),
	}

	jobRun := JobRun{}                                        // Create a JobRun instance
	jr := j.execCommand(context.Background(), jobRun, "test") // Pass JobRun instance and "test"
	assert.NotEqual(t, jr.Status, 0)
}

func TestJobRunInvalidSchedule(t *testing.T) {
	s := Schedule{}
	j := &JobSpec{
		Cron:    "MooIAmACow",
		Name:    "Bertha",
		Command: []string{"ls"},
		cfg:     NewConfig(),
	}
	s.Jobs = map[string]*JobSpec{}
	s.Jobs["Bertha"] = j

	assert.Error(t, s.initialize())
	// fix cron but add invalid ref
	s.Jobs["Bertha"].Cron = "* * * * *"
	s.Jobs["Bertha"].OnSuccess.TriggerJob = []string{"IDontExist"}

	assert.Error(t, s.initialize())
}

func TestOnEventWebhook(t *testing.T) {
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
		}
		// mirror this
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintln(w, string(body))
	}))

	defer testServer.Close()

	j := &JobSpec{
		Command: []string{"echo"},
		cfg:     NewConfig(),
		OnSuccess: OnEvent{
			NotifyWebhook: []string{testServer.URL},
		},
	}
	jobRun := JobRun{}                                        // Create a JobRun instance
	jr := j.execCommand(context.Background(), jobRun, "test") // Pass JobRun instance and "test"
	j.OnEvent(&jr)
}

func TestStringArray(t *testing.T) {
	type testCase struct {
		yamlString         string
		expectedStatus     int
		expectedLogContent string
	}

	for _, scenario := range []testCase{
		{
			yamlString:     `command: echo foo`,
			expectedStatus: 0, expectedLogContent: "foo",
		},
		{
			yamlString: `command:
- echo
- foo`,
			expectedStatus: 0, expectedLogContent: "foo",
		},
	} {
		j := JobSpec{}
		err := yaml.Unmarshal([]byte(scenario.yamlString), &j)
		if err != nil {
			t.Fatal(err)
		}

		j.cfg = NewConfig()
		jobRun := JobRun{}                                        // Create a JobRun instance
		jr := j.execCommand(context.Background(), jobRun, "test") // Pass JobRun instance and "test"

		jr.flushLogBuffer()
		assert.Equal(t, *jr.Status, scenario.expectedStatus)
		assert.Contains(t, jr.Log, scenario.expectedLogContent)
	}
}

func TestStandaloneJobRun(t *testing.T) {
	b := new(tsBuffer)
	log := NewLogger("debug", nil, b, os.Stdout)
	cfg := NewConfig()

	jr, err := RunJob(log, cfg, "../testdata/jobs1.yaml", "bar")
	assert.NoError(t, err)
	assert.Contains(t, b.String(), "\"job\":\"bar\",\"trigger\":\"manual\"")
	assert.Contains(t, jr.Log, "bar_foo")
}

func TestOnRetriesExhausted(t *testing.T) {
	// Test that on_retries_exhausted fires when all retries fail
	retriesExhaustedTriggered := false
	errorTriggeredCount := 0

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Parse the webhook payload to determine which event triggered it
		var payload map[string]interface{}
		if err := json.Unmarshal(body, &payload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		// Check if this is a retries exhausted webhook (based on the URL path or body content)
		switch r.URL.Path {
		case "/retries-exhausted":
			retriesExhaustedTriggered = true
		case "/error":
			errorTriggeredCount++
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	j := &JobSpec{
		Name:    "failing-job",
		Command: []string{"false"}, // command that always fails
		Retries: 2,                 // will try 3 times total (initial + 2 retries)
		cfg:     NewConfig(),
		log:     NewLogger("debug", nil, os.Stdout, os.Stdout),
		OnError: OnEvent{
			NotifyWebhook: []string{testServer.URL + "/error"},
		},
		OnRetriesExhausted: OnEvent{
			NotifyWebhook: []string{testServer.URL + "/retries-exhausted"},
		},
	}

	// Execute the job with retries
	jr := j.execCommandWithRetry(context.Background(), "test", nil)

	// Verify the job failed after all retries
	assert.Equal(t, 1, *jr.Status)

	// Give webhooks time to complete
	time.Sleep(100 * time.Millisecond)

	// Verify on_error was triggered 3 times (once for each failed attempt)
	assert.Equal(t, 3, errorTriggeredCount, "on_error should fire after each failed attempt")

	// Verify on_retries_exhausted was triggered once
	assert.True(t, retriesExhaustedTriggered, "on_retries_exhausted should fire once when all retries are exhausted")
}

func TestOnRetriesExhaustedNoRetries(t *testing.T) {
	// Test that on_retries_exhausted does NOT fire when retries = 0
	retriesExhaustedTriggered := false

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/retries-exhausted" {
			retriesExhaustedTriggered = true
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	j := &JobSpec{
		Name:    "failing-job-no-retries",
		Command: []string{"false"}, // command that always fails
		Retries: 0,                 // no retries configured
		cfg:     NewConfig(),
		log:     NewLogger("debug", nil, os.Stdout, os.Stdout),
		OnRetriesExhausted: OnEvent{
			NotifyWebhook: []string{testServer.URL + "/retries-exhausted"},
		},
	}

	// Execute the job
	jr := j.execCommandWithRetry(context.Background(), "test", nil)

	// Verify the job failed
	assert.Equal(t, 1, *jr.Status)

	// Give webhooks time to complete
	time.Sleep(100 * time.Millisecond)

	// Verify on_retries_exhausted was NOT triggered (since no retries were configured)
	assert.False(t, retriesExhaustedTriggered, "on_retries_exhausted should NOT fire when retries = 0")
}

func TestOnRetriesExhaustedSuccess(t *testing.T) {
	// Test that on_retries_exhausted does NOT fire when job eventually succeeds
	retriesExhaustedTriggered := false

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/retries-exhausted" {
			retriesExhaustedTriggered = true
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	j := &JobSpec{
		Name:    "succeeding-job",
		Command: []string{"true"}, // command that always succeeds
		Retries: 2,
		cfg:     NewConfig(),
		log:     NewLogger("debug", nil, os.Stdout, os.Stdout),
		OnRetriesExhausted: OnEvent{
			NotifyWebhook: []string{testServer.URL + "/retries-exhausted"},
		},
	}

	// Execute the job
	jr := j.execCommandWithRetry(context.Background(), "test", nil)

	// Verify the job succeeded
	assert.Equal(t, StatusOK, *jr.Status)

	// Give webhooks time to complete
	time.Sleep(100 * time.Millisecond)

	// Verify on_retries_exhausted was NOT triggered (since job succeeded)
	assert.False(t, retriesExhaustedTriggered, "on_retries_exhausted should NOT fire when job succeeds")
}

func TestRetryContextInWebhooks(t *testing.T) {
	// Test that retry context information is included in webhook payloads
	var webhookPayloads []map[string]interface{}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var payload map[string]interface{}
		if err := json.Unmarshal(body, &payload); err == nil {
			webhookPayloads = append(webhookPayloads, payload)
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	j := &JobSpec{
		Name:    "retry-context-job",
		Command: []string{"false"}, // command that always fails
		Retries: 1,                 // will try 2 times total
		cfg:     NewConfig(),
		log:     NewLogger("debug", nil, os.Stdout, os.Stdout),
		OnError: OnEvent{
			NotifyWebhook: []string{testServer.URL},
		},
		OnRetriesExhausted: OnEvent{
			NotifyWebhook: []string{testServer.URL},
		},
	}

	// Execute the job with retries
	_ = j.execCommandWithRetry(context.Background(), "test", nil)

	// Give webhooks time to complete
	time.Sleep(200 * time.Millisecond)

	// Should have 3 webhook calls: 2 on_error + 1 on_retries_exhausted
	assert.Equal(t, 3, len(webhookPayloads), "Expected 3 webhook calls")

	// Check the final payload (should be the retries exhausted one)
	finalPayload := webhookPayloads[len(webhookPayloads)-1]

	// Verify retry context fields are present
	assert.Equal(t, float64(1), finalPayload["retry_attempt"], "Final retry attempt should be 1")
	assert.Equal(t, true, finalPayload["retries_exhausted"], "retries_exhausted should be true")
	assert.Equal(t, "retry-context-job", finalPayload["name"], "Job name should be correct")

	// Check earlier payloads don't have retries_exhausted = true
	for i := 0; i < len(webhookPayloads)-1; i++ {
		payload := webhookPayloads[i]
		retriesExhausted, exists := payload["retries_exhausted"]
		if exists {
			assert.False(t, retriesExhausted.(bool), "retries_exhausted should be false for non-final attempts")
		}
	}
}

func TestWorkingDir(t *testing.T) {
	b := new(tsBuffer)
	log := NewLogger("debug", nil, b, os.Stdout)
	cfg := NewConfig()

	jr, err := RunJob(log, cfg, "../testdata/readme_example.yaml", "other_workingdir")
	assert.NoError(t, err)
	assert.Contains(t, jr.Log, "/testdata")
}

func TestJobWithBashEval(t *testing.T) {
	b := new(tsBuffer)
	log := NewLogger("debug", nil, b, os.Stdout)
	cfg := NewConfig()

	j := &JobSpec{
		Cron: "* * * * *",
		Name: "test",
		Command: []string{
			"bash", "-c", "MY_VAR=$(date +%Y-%m); echo $MY_VAR $FOO",
		},
		// add random Env to check if it passes through
		Env: map[string]secret{
			"FOO": "BAR",
		},
		cfg: NewConfig(),
	}

	j.log = log
	j.cfg = cfg

	jobRun := JobRun{}                                        // Create a JobRun instance
	jr := j.execCommand(context.Background(), jobRun, "test") // Pass JobRun instance and "test"
	jr.flushLogBuffer()

	currentYearMonth := time.Now().Format("2006-01")
	assert.Contains(t, jr.Log, currentYearMonth)
	assert.Contains(t, jr.Log, "BAR")
}

// TestExecCommandStartError simulates a scenario where cmd.Start() fails
func TestExecCommandStartError(t *testing.T) {
	// Create a sample JobRun instance
	jobRun := JobRun{
		LogEntryId:  1,
		Status:      nil,
		logBuf:      bytes.Buffer{},
		Log:         "",
		Name:        "TestJob",
		TriggeredAt: time.Now(),
		TriggeredBy: "manual",
		Triggered:   []string{"manual"},
		Duration:    0,
	}

	// Create a sample JobSpec instance with a command that will fail
	jobSpec := JobSpec{
		Name:    "TestJob",
		Command: stringArray{"nonexistent-command"},
		cfg: Config{
			SuppressLogs: false,
		},
	}

	// Run the execCommand method with the JobSpec and JobRun
	trigger := "manual"
	result := jobSpec.execCommand(context.Background(), jobRun, trigger)

	// Assertions
	assert.NotNil(t, result.Status, "Expected job run status to be set")
	assert.Equal(t, StatusError, *result.Status, "Expected StatusError when cmd.Start fails")
	assert.Contains(t, result.Log, "job unable to start", "Expected log to contain failure message")
	assert.NotEmpty(t, result.Log, "Expected log to contain some content")
	assert.NotNil(t, result.Duration, "Expected duration to be set")
	assert.GreaterOrEqual(t, result.Duration.Milliseconds(), int64(0), "Expected positive duration")
}

func TestExecCommandExitError(t *testing.T) {
	// Create a sample JobRun instance
	jobRun := JobRun{
		LogEntryId:  1,
		Status:      nil,
		logBuf:      bytes.Buffer{},
		Log:         "",
		Name:        "TestJob",
		TriggeredAt: time.Now(),
		TriggeredBy: "manual",
		Triggered:   []string{"manual"},
		Duration:    0,
	}

	// Create a sample JobSpec instance with a command that will fail
	jobSpec := JobSpec{
		Name:    "TestJob",
		Command: stringArray{"false"}, // Use a command that always exits with code 1
		cfg: Config{
			SuppressLogs: false,
		},
	}

	// Run the execCommand method with the JobSpec and JobRun
	trigger := "manual"
	result := jobSpec.execCommand(context.Background(), jobRun, trigger)

	// Assertions
	assert.NotNil(t, result.Status, "Expected job run status to be set")
	assert.Equal(t, 1, *result.Status, "Expected StatusError when cmd.Wait fails with non-zero exit code")
	assert.Contains(t, result.Log, "Exit code:", "Expected log to contain exit status message")
	assert.NotEmpty(t, result.Log, "Expected log to contain some content")
	assert.NotNil(t, result.Duration, "Expected duration to be set")
	assert.GreaterOrEqual(t, result.Duration.Milliseconds(), int64(0), "Expected positive duration")
}

func TestMarshalSecret(t *testing.T) {
	secrets := map[string]secret{"foo": "bar"}

	yamlResult, err := yaml.Marshal(secrets)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, string(yamlResult), "foo: '***'\n")

	jsonResult, err := json.Marshal(secrets)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, string(jsonResult), `{"foo":"***"}`)
}

func TestTriggeredByJobRunContext(t *testing.T) {
	// Test that TriggeredByJobRun context is properly passed to triggered jobs
	var webhookPayload map[string]interface{}

	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() { _ = r.Body.Close() }()
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if err := json.Unmarshal(body, &webhookPayload); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.WriteHeader(http.StatusOK)
	}))
	defer testServer.Close()

	// Create a parent job that will trigger a child job
	parentJob := &JobSpec{
		Name:    "parent-job",
		Command: []string{"echo", "parent output"},
		cfg:     NewConfig(),
		log:     NewLogger("debug", nil, os.Stdout, os.Stdout),
	}

	// Create a child job that will be triggered by the parent
	childJob := &JobSpec{
		Name:    "child-job",
		Command: []string{"echo", "child output"},
		cfg:     NewConfig(),
		log:     NewLogger("debug", nil, os.Stdout, os.Stdout),
		OnSuccess: OnEvent{
			NotifyWebhook: []string{testServer.URL},
		},
	}

	// Set up a mock schedule with both jobs
	schedule := &Schedule{
		Jobs: map[string]*JobSpec{
			"parent-job": parentJob,
			"child-job":  childJob,
		},
		loc: time.UTC,
	}

	// Link jobs to the schedule
	parentJob.globalSchedule = schedule
	childJob.globalSchedule = schedule

	// Configure parent job to trigger child job on success
	parentJob.OnSuccess = OnEvent{
		TriggerJob: []string{"child-job"},
	}

	// Execute the parent job
	_ = parentJob.execCommandWithRetry(context.Background(), "manual", nil)

	// Give time for the triggered job to complete
	time.Sleep(100 * time.Millisecond)

	// Verify webhook was called with child job data
	assert.NotNil(t, webhookPayload, "Webhook should have been called")
	assert.Equal(t, "child-job", webhookPayload["name"], "Child job name should be correct")
	assert.Equal(t, "job[parent-job]", webhookPayload["triggered_by"], "Child job should be triggered by parent")

	// Verify the parent job context is included
	triggeredByJobRun, exists := webhookPayload["triggered_by_job_run"]
	assert.True(t, exists, "triggered_by_job_run should be present")
	assert.NotNil(t, triggeredByJobRun, "triggered_by_job_run should not be nil")

	// Cast to map to access fields
	parentContext := triggeredByJobRun.(map[string]interface{})
	assert.Equal(t, "parent-job", parentContext["name"], "Parent job name should be correct")
	assert.Equal(t, "manual", parentContext["triggered_by"], "Parent job trigger should be correct")
	assert.Equal(t, float64(0), parentContext["status"], "Parent job should have succeeded")
	assert.Contains(t, parentContext["log"], "parent output", "Parent job log should contain expected output")
}

func TestLogRetentionPeriodParsing(t *testing.T) {
	tests := []struct {
		name        string
		durationStr string
		shouldError bool
		expectedErr string
	}{
		{
			name:        "valid duration - hours",
			durationStr: "24 hours",
			shouldError: false,
		},
		{
			name:        "valid duration - days",
			durationStr: "7 days",
			shouldError: false,
		},
		{
			name:        "valid duration - weeks",
			durationStr: "2 weeks",
			shouldError: false,
		},
		{
			name:        "valid duration - months",
			durationStr: "3 months",
			shouldError: false,
		},
		{
			name:        "valid duration - minutes",
			durationStr: "90 minutes",
			shouldError: false,
		},
		{
			name:        "valid duration - complex",
			durationStr: "1 hour and 30 minutes",
			shouldError: false,
		},
		{
			name:        "invalid duration",
			durationStr: "invalid",
			shouldError: true,
			expectedErr: "invalid log_retention_period",
		},
		{
			name:        "zero duration",
			durationStr: "0 seconds",
			shouldError: true,
			expectedErr: "must be positive",
		},
		{
			name:        "negative duration fails",
			durationStr: "-1 hour",
			shouldError: true,
			expectedErr: "must be positive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			yamlContent := fmt.Sprintf(`
jobs:
  test-job:
    command: echo "test"
    cron: "* * * * *"
    log_retention_period: %s
`, tc.durationStr)

			// Write to temp file
			tmpfile, err := os.CreateTemp("", "test-*.yaml")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.Remove(tmpfile.Name()) }()

			if _, err := tmpfile.Write([]byte(yamlContent)); err != nil {
				t.Fatal(err)
			}
			if err := tmpfile.Close(); err != nil {
				t.Fatal(err)
			}

			// Try to load schedule
			log := NewLogger("debug", nil, os.Stdout)
			cfg := NewConfig()
			_, err = loadSchedule(log, cfg, tmpfile.Name())

			if tc.shouldError {
				assert.Error(t, err)
				if tc.expectedErr != "" {
					assert.Contains(t, err.Error(), tc.expectedErr)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLogRetentionIntegration(t *testing.T) {
	// Create test database
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	cfg := NewConfig()
	cfg.DB = db

	// Create a job with log retention
	j := &JobSpec{
		Name:                 "test-retention",
		Command:              []string{"echo", "test"},
		logRetentionDuration: 1 * time.Hour, // Retain logs for 1 hour
		cfg:                  cfg,
		log:                  NewLogger("debug", nil),
	}

	// Insert some old log entries (2 hours ago)
	oldTime := time.Now().Add(-2 * time.Hour)
	_, err = db.Exec(`
		INSERT INTO log (job, triggered_at, triggered_by, duration, status, message) 
		VALUES (?, ?, ?, ?, ?, ?)
	`, j.Name, oldTime, "test", 100, 0, "old log entry 1")
	assert.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO log (job, triggered_at, triggered_by, duration, status, message) 
		VALUES (?, ?, ?, ?, ?, ?)
	`, j.Name, oldTime.Add(-30*time.Minute), "test", 200, 0, "old log entry 2")
	assert.NoError(t, err)

	// Insert a recent log entry (30 minutes ago)
	recentTime := time.Now().Add(-30 * time.Minute)
	_, err = db.Exec(`
		INSERT INTO log (job, triggered_at, triggered_by, duration, status, message) 
		VALUES (?, ?, ?, ?, ?, ?)
	`, j.Name, recentTime, "test", 150, 0, "recent log entry")
	assert.NoError(t, err)

	// Check initial count
	var initialCount int
	err = db.Get(&initialCount, "SELECT COUNT(*) FROM log WHERE job = ?", j.Name)
	assert.NoError(t, err)
	assert.Equal(t, 3, initialCount)

	// Run log cleanup
	j.cleanupOldLogs()

	// Check final count - should have deleted the 2 old entries, kept the recent one
	var finalCount int
	err = db.Get(&finalCount, "SELECT COUNT(*) FROM log WHERE job = ?", j.Name)
	assert.NoError(t, err)
	assert.Equal(t, 1, finalCount)

	// Verify the remaining entry is the recent one
	var remainingMessage string
	err = db.Get(&remainingMessage, "SELECT message FROM log WHERE job = ?", j.Name)
	assert.NoError(t, err)
	assert.Equal(t, "recent log entry", remainingMessage)
}

func TestLogRetentionNoDuration(t *testing.T) {
	// Test that cleanup doesn't run when no duration is set
	db, err := OpenDB(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	cfg := NewConfig()
	cfg.DB = db

	j := &JobSpec{
		Name:    "test-no-retention",
		Command: []string{"echo", "test"},
		// No logRetentionDuration set
		cfg: cfg,
		log: NewLogger("debug", nil),
	}

	// Insert a log entry
	_, err = db.Exec(`
		INSERT INTO log (job, triggered_at, triggered_by, duration, status, message) 
		VALUES (?, ?, ?, ?, ?, ?)
	`, j.Name, time.Now().Add(-2*time.Hour), "test", 100, 0, "old log entry")
	assert.NoError(t, err)

	// Run log cleanup - should do nothing
	j.cleanupOldLogs()

	// Verify entry still exists
	var count int
	err = db.Get(&count, "SELECT COUNT(*) FROM log WHERE job = ?", j.Name)
	assert.NoError(t, err)
	assert.Equal(t, 1, count)
}

func TestLogRetentionNoDB(t *testing.T) {
	// Test that cleanup doesn't crash when no DB connection
	j := &JobSpec{
		Name:                 "test-no-db",
		Command:              []string{"echo", "test"},
		logRetentionDuration: 1 * time.Hour,
		cfg:                  NewConfig(), // No DB set
		log:                  NewLogger("debug", nil),
	}

	// This should not panic
	j.cleanupOldLogs()
}
