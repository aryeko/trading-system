# S-10: Scheduler Integration (Cron / GitHub Actions)

**Last updated:** 2025-09-21

## Outcome
- Automate daily and rebalance-day runs using cron (self-hosted) and/or GitHub Actions, ensuring jobs execute post-close in the configured market timezone.

## Deliverables
- Scheduler configuration files (e.g., `ops/schedule.yml`) describing cron expressions, timezones, and commands to run.
- GitHub Actions workflow or reference cron setup that triggers `ts run daily` and `ts run rebalance` with appropriate parameters.
- CLI command `ts scheduler plan --config configs/sample-config.yml` that prints upcoming run times and referenced commands for the next N days.

## Functional Requirements
- Account for market holidays and weekends; allow overrides via config (manual skip list or calendar integration).
- Support manual backfill via `ts run daily --asof <date>` by exposing documentation and sample gh-action `workflow_dispatch` inputs.
- Provide notifications/log forwarding when scheduled jobs fail (reuse notifier or integrate with external alerting service).
- Keep secrets (API keys, Slack webhooks) out of repository; instruct operators on storing them in GitHub secrets or environment variables.

## CLI Additions
- `poetry run ts scheduler plan --config <path> --days 5` outputs the next scheduled run times with timezone conversions.
- `poetry run ts scheduler deploy --platform github` validates CI workflow syntax and prints instructions for enabling scheduled triggers.

## Verification
1. Run `ts scheduler plan` with a sample config; verify the schedule aligns with market close plus buffer (e.g., 18:15 America/New_York).
2. Trigger the GitHub Actions workflow manually (workflow_dispatch) and confirm it executes `ts run daily` successfully.
3. For local cron, use `dry-run` mode that logs command invocation instead of executing, then inspect logs for correctness.
4. Document rollback/disable procedure and test it (e.g., disable workflow and verify no further runs occur).

## Dependencies
- S-09 (CLI orchestration).

## Notes
- Keep scheduler configuration declarative so production and staging environments can share the same template with different variables.
