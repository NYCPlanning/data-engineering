"""Operations for data lifecycle management"""

from dagster import OpExecutionContext, job, op


@op
def purge_data_directory(context: OpExecutionContext):
    """Purge the local data directory to free up space.

    This removes all cached data from dcpy's local data path.
    Runs on a schedule to prevent disk space issues.
    """
    from dcpy.lifecycle.ops import purge_data_dir

    context.log.info("Starting data directory purge...")

    try:
        purge_data_dir()
        context.log.info("Data directory purged successfully")
        return "success"
    except Exception as e:
        context.log.error(f"Failed to purge data directory: {e}")
        raise


@job(description="Clean up local data directory to free disk space")
def cleanup_data_job():
    """Job that purges the local data directory"""
    purge_data_directory()
