from dagster import op, graph, OpExecutionContext, DagsterRunStatus
from dagster._core.storage.pipeline_run import RunsFilter
from datetime import datetime, timedelta, timezone
from dagster_graphql import DagsterGraphQLClient, DagsterGraphQLClientError

DAGSTER_POSTGRES_DB = 'db_name'
DAGSTER_POSTGRES_USER = 'username'
DAGSTER_POSTGRES_PASSWORD = 'password'
DAGSTER_POSTGRES_HOST = 'host'
REPOSITORY_LOCATION_NAME = '...'
DAGSTER_API = '...''


@op(
    config_schema={"start_date": str, "end_date": str, "is_partitioned": int, "retry_only_canceled_jobs": int, "job_priority": int,
                   "job_name": str})
def retry_failed_runs(context: OpExecutionContext):
    # Initialize the Dagster instance
    instance = context.instance

    # Fetch failed jobs from particular time range
    start_date = datetime.strptime(context.op_config["start_date"], '%Y-%m-%d %H:%M:%S').replace(
                tzinfo=timezone(offset=timedelta()))
    end_date = datetime.strptime(context.op_config["end_date"], '%Y-%m-%d %H:%M:%S').replace(
                tzinfo=timezone(offset=timedelta()))

    is_partitioned = context.op_config["is_partitioned"]
    retry_only_canceled_jobs = context.op_config["retry_only_canceled_jobs"]
    job_priority = context.op_config["job_priority"]
    job_name = context.op_config["job_name"]

    if job_priority:
        job_priority_tag = job_priority
    else:
        job_priority_tag = 0

    if retry_only_canceled_jobs == 1:
        failed_runs = instance.get_runs(
            filters=RunsFilter(created_before=end_date,
                               updated_after=start_date,
                               job_name=job_name if job_name else None,
                               statuses=[DagsterRunStatus.CANCELED])
        )
    else:
        failed_runs = instance.get_runs(
            filters=RunsFilter(created_before=end_date,
                               updated_after=start_date,
                               job_name=job_name if job_name else None,
                               statuses=[DagsterRunStatus.FAILURE])
        )

    client = DagsterGraphQLClient(DAGSTER_API, timeout=100)
    for run in failed_runs:
        run_config = run.run_config if run.run_config else {}
        tags = run.tags if run.tags else {}
        try:
            partition = run.tags.get("dagster/partition")
            context.log.warning(f"Run ID: {run.run_id}, Partition: {partition}")
        except Exception as e:
            context.log.error(f'Error while retriving job run partition:  {str(e)}')
        if run_config:
            if tags:
                priority = run.tags.get("dagster/priority", "7")
                if job_priority_tag:
                    priority = job_priority_tag
                k8s_config = run.tags.get("dagster-k8s/config", {})
                tags_filtered = {
                            'dagster/priority': priority,
                            'dagster-k8s/config': k8s_config
                }
                if partition:
                    if partition not in ['null', 'None']:
                        tags_filtered['dagster/partition'] = partition

                if is_partitioned and 'dagster/partition' not in tags_filtered:
                    continue
                else:
                    client.submit_job_execution(
                            run.job_name,
                            repository_location_name=REPOSITORY_LOCATION_NAME,
                            run_config=run_config,
                            tags=tags_filtered
                    )
            else:
                client.submit_job_execution(
                    run.job_name,
                    repository_location_name=REPOSITORY_LOCATION_NAME,
                    run_config=run_config
                )
        else:
            client.submit_job_execution(
                run.job_name,
                repository_location_name=REPOSITORY_LOCATION_NAME
            )


@graph()
def retry_failed_runs_graph():
    retry_failed_runs()


retry_failed_runs_graph_job = retry_failed_runs_graph.to_job(
    name="retry_failed_runs_graph_job"
)
