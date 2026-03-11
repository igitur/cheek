package cheek

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
)

func TestScheduleRun(t *testing.T) {
	// rough test
	// just tries to see if we can get to a job trigger
	// and to see that exit signals are received correctly
	viper.Set("port", "9999")
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	b := new(tsBuffer)
	logger := NewLogger("debug", nil, b, os.Stdout)

	go func() {
		err := RunSchedule(logger, Config{DBPath: "tmpdb.sqlite3"}, "../testdata/jobs1.yaml")
		if err != nil {
			panic(err)
		}
	}()

	time.Sleep(6 * time.Second)
	spew.Dump(b.String())
	if err := proc.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}

	time.Sleep(6 * time.Second)
	assert.Contains(t, b.String(), "Job triggered")
	assert.Contains(t, b.String(), "Shutting down scheduler due to context cancellation")

	// check that job gets triggered by other job
	assert.Contains(t, b.String(), "\"trigger\":\"job[foo]")
}

func TestTZInfo(t *testing.T) {
	s := Schedule{
		Jobs:       map[string]*JobSpec{},
		TZLocation: "Africa/Bangui",
		log:        zerolog.Logger{},
		cfg:        NewConfig(),
	}
	if err := s.initialize(); err != nil {
		t.Fatal(err)
	}
	time1 := s.now()

	s = Schedule{
		Jobs:       map[string]*JobSpec{},
		TZLocation: "Europe/Amsterdam",
		log:        zerolog.Logger{},
		cfg:        NewConfig(),
	}
	if err := s.initialize(); err != nil {
		t.Fatal(err)
	}

	time2 := s.now()
	assert.NotEqual(t, time1.Sub(time2).Hours(), 0.0)
}

func TestDisableConcurrentExecution(t *testing.T) {
	// Test that when disable_concurrent_execution is true, only one instance runs
	// and when false, multiple instances can run concurrently

	// Create a test schedule with jobs that take 3 seconds but run every second
	testScheduleYAML := `
jobs:
  concurrent_disabled:
    command: 
      - sh
      - -c
      - "echo 'start_non_concurrent'; sleep 10; echo 'end_non_concurrent'"
    cron: "* * * * * *"  # every second
    disable_concurrent_execution: true
  concurrent_enabled:
    command: 
      - sh
      - -c
      - "echo 'start_concurrent'; sleep 10; echo 'end_concurrent'"
    cron: "* * * * * *"  # every second
    disable_concurrent_execution: false
`

	// Write test schedule to temp file
	tmpFile, err := os.CreateTemp("", "test_schedule_*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	if _, err := tmpFile.WriteString(testScheduleYAML); err != nil {
		t.Fatal(err)
	}
	_ = tmpFile.Close()

	// Create buffer to capture logs
	b := new(tsBuffer)
	logger := NewLogger("debug", nil, b, os.Stdout)

	// Load the schedule
	s, err := loadSchedule(logger, Config{}, tmpFile.Name())
	if err != nil {
		t.Fatal(err)
	}

	// Create a done channel to signal scheduler completion
	done := make(chan struct{})

	// Run the scheduler with completion signaling
	go func() {
		defer close(done)
		s.Run()
	}()

	time.Sleep(5 * time.Second)

	// Send interrupt signal to stop scheduler
	proc, err := os.FindProcess(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	if err := proc.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}

	// Wait for scheduler to fully shut down
	<-done

	// Collect job logs from individual jobs
	var allJobLogs strings.Builder

	// Access job runs that are stored in memory (without DB dependency)
	for _, job := range s.Jobs {

		for _, run := range job.Runs {
			spew.Dump(run)
			allJobLogs.WriteString(run.Log)
		}
	}

	completeLogOutput := allJobLogs.String()

	// Count occurrences of start messages
	nonConcurrentStarts := strings.Count(completeLogOutput, "start_non_concurrent")
	concurrentStarts := strings.Count(completeLogOutput, "start_concurrent")

	// With disable_concurrent_execution: true, we should see exactly 1 start
	// because subsequent executions are blocked while the first 3-second job is running
	assert.Equal(t, 1, nonConcurrentStarts, "Expected exactly 1 start for non-concurrent job")

	// With disable_concurrent_execution: false, we should see more than 1 start
	// because jobs can overlap (8 seconds runtime with 3-second jobs starting every second)
	assert.Greater(t, concurrentStarts, 1, "Expected more than 1 start for concurrent job")
}

func TestScheduleGlobalLogRetentionPeriod(t *testing.T) {
	tests := []struct {
		name           string
		yamlContent    string
		shouldError    bool
		expectedErr    string
		expectDuration time.Duration
	}{
		{
			name: "valid global retention period - days",
			yamlContent: `
tz_location: UTC
log_retention_period: 30 days
jobs:
  test_job:
    command: echo "test"
    cron: "* * * * *"
`,
			shouldError:    false,
			expectDuration: 30 * 24 * time.Hour,
		},
		{
			name: "valid global retention period - weeks",
			yamlContent: `
tz_location: UTC
log_retention_period: 2 weeks
jobs:
  test_job:
    command: echo "test"
    cron: "* * * * *"
`,
			shouldError:    false,
			expectDuration: 14 * 24 * time.Hour,
		},
		{
			name: "valid global retention period - months",
			yamlContent: `
tz_location: UTC
log_retention_period: 3 months
jobs:
  test_job:
    command: echo "test"
    cron: "* * * * *"
`,
			shouldError:    false,
			expectDuration: 90 * 24 * time.Hour, // 3 months ≈ 90 days
		},
		{
			name: "no global retention period",
			yamlContent: `
tz_location: UTC
jobs:
  test_job:
    command: echo "test"
    cron: "* * * * *"
`,
			shouldError:    false,
			expectDuration: 0,
		},
		{
			name: "invalid global retention period",
			yamlContent: `
tz_location: UTC
log_retention_period: invalid
jobs:
  test_job:
    command: echo "test"
    cron: "* * * * *"
`,
			shouldError: true,
			expectedErr: "invalid log_retention_period for schedule",
		},
		{
			name: "zero global retention period",
			yamlContent: `
tz_location: UTC
log_retention_period: 0 seconds
jobs:
  test_job:
    command: echo "test"
    cron: "* * * * *"
`,
			shouldError: true,
			expectedErr: "must be positive",
		},
		{
			name: "negative global retention period",
			yamlContent: `
tz_location: UTC
log_retention_period: -1 hour
jobs:
  test_job:
    command: echo "test"
    cron: "* * * * *"
`,
			shouldError: true,
			expectedErr: "must be positive",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Write test schedule to temp file
			tmpFile, err := os.CreateTemp("", "test_schedule_*.yaml")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.Remove(tmpFile.Name()) }()

			if _, err := tmpFile.WriteString(tc.yamlContent); err != nil {
				t.Fatal(err)
			}
			_ = tmpFile.Close()

			// Load the schedule
			logger := NewLogger("debug", nil, os.Stdout)
			schedule, err := loadSchedule(logger, Config{}, tmpFile.Name())

			if tc.shouldError {
				assert.Error(t, err)
				if tc.expectedErr != "" {
					assert.Contains(t, err.Error(), tc.expectedErr)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expectDuration, schedule.logRetentionDuration)

				// Verify schedule fields are set correctly
				assert.NotNil(t, schedule.Jobs)
				assert.NotNil(t, schedule.loc)

				// Check that job exists
				job, exists := schedule.Jobs["test_job"]
				assert.True(t, exists)
				assert.NotNil(t, job)
			}
		})
	}
}

func TestScheduleGlobalLogRetentionPeriodInheritance(t *testing.T) {
	tests := []struct {
		name           string
		yamlContent    string
		jobName        string
		expectDuration time.Duration
	}{
		{
			name: "job inherits global retention period",
			yamlContent: `
tz_location: UTC
log_retention_period: 30 days
jobs:
  job1:
    command: echo "test1"
    cron: "* * * * *"
  job2:
    command: echo "test2"
    cron: "0 * * * *"
`,
			jobName:        "job1",
			expectDuration: 30 * 24 * time.Hour,
		},
		{
			name: "job overrides global retention period",
			yamlContent: `
tz_location: UTC
log_retention_period: 30 days
jobs:
  job1:
    command: echo "test1"
    cron: "* * * * *"
    log_retention_period: 7 days
  job2:
    command: echo "test2"
    cron: "0 * * * *"
`,
			jobName:        "job1",
			expectDuration: 7 * 24 * time.Hour,
		},
		{
			name: "mixed inheritance and overrides",
			yamlContent: `
tz_location: UTC
log_retention_period: 30 days
jobs:
  inherits_job:
    command: echo "inherits"
    cron: "* * * * *"
  overrides_job:
    command: echo "overrides"
    cron: "0 * * * *"
    log_retention_period: 14 days
`,
			jobName:        "inherits_job",
			expectDuration: 30 * 24 * time.Hour,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Write test schedule to temp file
			tmpFile, err := os.CreateTemp("", "test_schedule_*.yaml")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = os.Remove(tmpFile.Name()) }()

			if _, err := tmpFile.WriteString(tc.yamlContent); err != nil {
				t.Fatal(err)
			}
			_ = tmpFile.Close()

			// Load the schedule
			logger := NewLogger("debug", nil, os.Stdout)
			schedule, err := loadSchedule(logger, Config{}, tmpFile.Name())
			assert.NoError(t, err)

			// Check specific job
			job, exists := schedule.Jobs[tc.jobName]
			assert.True(t, exists)
			assert.NotNil(t, job)
			assert.Equal(t, tc.expectDuration, job.logRetentionDuration)

			// Verify schedule has global duration
			if strings.Contains(tc.yamlContent, "log_retention_period:") &&
				!strings.Contains(tc.yamlContent, "log_retention_period: invalid") {
				assert.Greater(t, schedule.logRetentionDuration, time.Duration(0))
			}
		})
	}
}

func TestScheduleGlobalLogRetentionPeriodWithExistingFeatures(t *testing.T) {
	// Test that global retention period doesn't break existing schedule features
	testScheduleYAML := `
tz_location: UTC
log_retention_period: 30 days
jobs:
  foo:
    command: date
    cron: "* * * * *"
    on_success:
      trigger_job:
        - bar
  bar:
    command: 
      - echo
      - $foo
    env:
      foo: bar
    cron: "* * * * *"
  coffee:
    command: this fails
    cron: "* * * * *"
    retries: 3
    log_retention_period: 7 days
    on_error:
      notify_webhook:
        - https://example.com/webhook
`

	// Write test schedule to temp file
	tmpFile, err := os.CreateTemp("", "test_schedule_*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	if _, err := tmpFile.WriteString(testScheduleYAML); err != nil {
		t.Fatal(err)
	}
	_ = tmpFile.Close()

	// Load the schedule
	logger := NewLogger("debug", nil, os.Stdout)
	schedule, err := loadSchedule(logger, Config{}, tmpFile.Name())
	assert.NoError(t, err)

	// Verify schedule has global duration
	assert.Equal(t, 30*24*time.Hour, schedule.logRetentionDuration)

	// Verify jobs are loaded correctly
	assert.Len(t, schedule.Jobs, 3)

	// Check specific jobs
	fooJob, exists := schedule.Jobs["foo"]
	assert.True(t, exists)
	assert.Equal(t, 30*24*time.Hour, fooJob.logRetentionDuration) // Inherits global

	barJob, exists := schedule.Jobs["bar"]
	assert.True(t, exists)
	assert.Equal(t, 30*24*time.Hour, barJob.logRetentionDuration) // Inherits global

	coffeeJob, exists := schedule.Jobs["coffee"]
	assert.True(t, exists)
	assert.Equal(t, 7*24*time.Hour, coffeeJob.logRetentionDuration) // Overrides global

	// Verify job triggers are set up
	assert.Contains(t, fooJob.OnSuccess.TriggerJob, "bar")

	// Verify environment variables
	assert.Equal(t, "bar", string(barJob.Env["foo"]))

	// Verify retries
	assert.Equal(t, 3, coffeeJob.Retries)

	// Verify webhooks
	assert.Len(t, coffeeJob.OnError.NotifyWebhook, 1)
	assert.Equal(t, "https://example.com/webhook", coffeeJob.OnError.NotifyWebhook[0])
}
