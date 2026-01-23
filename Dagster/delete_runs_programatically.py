from dagster import op, graph, OpExecutionContext, DagsterRunStatus
from dagster._core.storage.pipeline_run import RunsFilter
from datetime import datetime, timedelta


@op
def delete_dagster_runs(context: OpExecutionContext):
    # Initialize the Dagster instance
    instance = context.instance

    # Define the cutoff date for retention
    cutoff_date = datetime.now() - timedelta(days=30)

    # Fetch runs older than the cutoff date
    old_runs = instance.get_run_records(filters=RunsFilter(created_before=cutoff_date,
                                                           statuses=[DagsterRunStatus.CANCELED,
                                                                     DagsterRunStatus.FAILURE]))

    for run_record in old_runs:
        run_id = run_record.dagster_run.run_id
        context.log.info(f'Deleting run id: {run_id}')
        instance.delete_run(run_id)


@graph()
def delete_dagster_runs_graph():
    delete_dagster_runs()


delete_dagster_runs_graph_job = delete_dagster_runs_graph.to_job(
    name="delete_dagster_runs_graph_job"
)
