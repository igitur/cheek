---
title: Log Retention
---

# Log Retention

Cheek supports automatic cleanup of old log entries to prevent database bloat. Configure retention periods at the schedule level (global default) or job level (overrides).

## Global Configuration

Set a default retention period for all jobs:

```yaml
tz_location: UTC
log_retention_period: 30 days
jobs:
  job1:
    command: echo "hello"
    cron: "* * * * *"
```

## Job-Level Configuration

Jobs can override the global setting:

```yaml
tz_location: UTC
log_retention_period: 30 days  # global default
jobs:
  important_job:
    command: critical-task
    cron: "0 * * * *"
    log_retention_period: 7 days  # override
```

## Inheritance Rules

- If a job specifies `log_retention_period`, it uses that duration
- If not specified, job inherits from global schedule setting
- If neither is set, no automatic cleanup occurs

## Supported Duration Formats

Uses human-readable formats:
- Days: `30 days`, `1 day`
- Weeks: `2 weeks`, `1 week`  
- Months: `3 months`, `1 month`
- Hours: `24 hours`, `1 hour`
- Minutes: `90 minutes`, `30 minutes`
- Complex: `1 hour and 30 minutes`

## Validation

- Must be positive duration (> 0)
- Invalid formats are rejected at startup
- Zero or negative values cause configuration errors

## How It Works

- Cleanup runs automatically after each job execution
- Deletes log entries older than the configured duration
- Uses efficient database queries with indexes
- Only affects jobs with retention configured