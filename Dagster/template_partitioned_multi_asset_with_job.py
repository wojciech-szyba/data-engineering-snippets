from dagster import AssetKey, multi_asset, MetadataValue, AssetExecutionContext, Output, RetryPolicy, Backoff, \
    AssetOut, in_process_executor, define_asset_job
from dagster_docker import docker_executor
from dagster_k8s import k8s_job_executor

ASSET_GROUP_NAME = '...'
TABLES_LIST = []
PARTITION_DEFINITION = ...
DEPLOYMENT_ENV = 'production'

executor_by_env = {
    "local":  in_process_executor,
    "test": docker_executor,
    "production":  k8s_job_executor
}


@multi_asset(group_name=ASSET_GROUP_NAME,
             compute_kind='parquet',
             description="Processing",
             can_subset=True,
             outs={
                   f'{ASSET_GROUP_NAME}_multi_asset_template_{table_name}': AssetOut(
                                is_required=False,
                                key_prefix='xyz',
                                tags={"integration_name": ASSET_GROUP_NAME.capitalize()},
                   ) for table_name in TABLES_LIST
             },
             deps=[
                 AssetKey(['xyz', table_name]) for table_name in TABLES_LIST
             ],
             internal_asset_deps={
                 f'{ASSET_GROUP_NAME}_previous_step", "")}': {
                     AssetKey(['xyz', f'{ASSET_GROUP_NAME}_multi_asset_template_{table_name}'])
                 }
                 for table_name in TABLES_LIST
             },
             name="multi_asset_template",
             retry_policy=RetryPolicy(max_retries=3, delay=0.2, backoff=Backoff.EXPONENTIAL),
             partitions_def=PARTITION_DEFINITION,
             op_tags={
                 "dagster-k8s/config": {
                        "container_config": {
                             "resources": {
                                 "requests": {"cpu": "100m", "memory": "70Mi"},
                                 "limits": {"cpu": "400m", "memory": "300Mi"},
                             }
                         },
                        "job_spec_config": {
                            "ttl_seconds_after_finished": 100
                        }
                 }
             },
             )
def multi_asset_template(context: AssetExecutionContext):
    df = ... # Can be assigned to kwargs in loop i.e. for asset in kwargs --> df = asset
    yield Output(df,
                 metadata={
                     "num_records": len(df),  # Metadata can be any key-value pair
                     "size": df.size,
                     "preview": MetadataValue.md(df.head().to_markdown())
                     # The `MetadataValue` class has useful static methods to build Metadata
                 },
                 output_name=...
                 )

job_name = [
            define_asset_job(name=f"{table_name}_job_name",
                             selection=[f"{'xyz'/...}"],
                             partitions_def=PARTITION_DEFINITION,
                             hooks={...},
                             executor_def=executor_by_env[DEPLOYMENT_ENV],
                             op_retry_policy=RetryPolicy(max_retries=3, delay=10,
                                                         backoff=Backoff.EXPONENTIAL)
                                                        )
            for table_name in TABLES_LIST
]

