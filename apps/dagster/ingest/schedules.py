"""Schedules for automated job execution"""

from dagster import ScheduleDefinition

from .ops import cleanup_data_job

# Run cleanup every Sunday at 2 AM
cleanup_schedule = ScheduleDefinition(
    job=cleanup_data_job,
    cron_schedule="0 2 * * 0",  # Minute Hour Day Month DayOfWeek (0 = Sunday)
    name="weekly_data_cleanup",
    description="Purge local data directory every Sunday at 2 AM to free disk space",
)
