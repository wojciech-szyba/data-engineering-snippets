from dagster import run_status_sensor, DagsterRunStatus, RunRequest


@run_status_sensor(
    name="...",
    monitored_jobs=[...],  # Jobs to monitor
    run_status=DagsterRunStatus.SUCCESS,    # Trigger only on success
    request_jobs=...
)
def sensor(context):
    partition_key = context.dagster_run.tags.get("dagster/partition")
    # Trigger the downstream job with an optional run config
    return [RunRequest(job_name=f"{entity}_job", run_key=f"{entity}_triggered_by_{context.dagster_run.run_id}",
                       partition_key=partition_key) for entity in ...]

