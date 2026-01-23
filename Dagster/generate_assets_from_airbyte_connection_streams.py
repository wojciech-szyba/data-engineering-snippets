from itertools import chain
from typing import (
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Union,
)

from dagster import (
    AssetKey,
    AssetOut,
    FreshnessPolicy,
    Output,
    SourceAsset,
    _check as check,
    define_asset_job,
)
from dagster._core.definitions import AssetsDefinition, multi_asset

from dagster._core.definitions.events import CoercibleToAssetKey
from dagster._core.definitions.metadata import MetadataValue
from dagster._core.definitions.metadata.table import TableSchema
from dagster._core.errors import DagsterInvalidDefinitionError

from dagster_airbyte.resources import BaseAirbyteResource
from dagster_airbyte.utils import (
    generate_materializations,
)


TABLES_LIST = [...]
SCHEMA_NAME = '...'
ASSET_GROUP_NAME = '...'
DEPLOYMENT_ENV = '...'

connection_id_by_env = {
    "local": {
        'connection_id': "..."
    },
    "sandbox": {
        'connection_id': "..."
    },
    "production": {
        'connection_id': "..."
    },
}

connection_id = connection_id_by_env[DEPLOYMENT_ENV]


def _table_to_output_name_fn(table: str) -> str:
    return table.replace("-", "_")


def build_airbyte_assets(
    connection_id: str,
    destination_tables: Sequence[str],
    asset_key_prefix: Optional[Sequence[str]] = None,
    group_name: Optional[str] = None,
    normalization_tables: Optional[Mapping[str, Set[str]]] = None,
    deps: Optional[Iterable[Union[CoercibleToAssetKey, AssetsDefinition, SourceAsset]]] = None,
    upstream_assets: Optional[Set[AssetKey]] = None,
    schema_by_table_name: Optional[Mapping[str, TableSchema]] = None,
    freshness_policy: Optional[FreshnessPolicy] = None,
    stream_to_asset_map: Optional[Mapping[str, str]] = None,
) -> Sequence[AssetsDefinition]:
    """Builds a set of assets representing the tables created by an Airbyte sync operation.

    Args:
        connection_id (str): The Airbyte Connection ID that this op will sync. You can retrieve this
            value from the "Connections" tab of a given connector in the Airbyte UI.
        destination_tables (List[str]): The names of the tables that you want to be represented
            in the Dagster asset graph for this sync. This will generally map to the name of the
            stream in Airbyte, unless a stream prefix has been specified in Airbyte.
        normalization_tables (Optional[Mapping[str, List[str]]]): If you are using Airbyte's
            normalization feature, you may specify a mapping of destination table to a list of
            derived tables that will be created by the normalization process.
        asset_key_prefix (Optional[List[str]]): A prefix for the asset keys inside this asset.
            If left blank, assets will have a key of `AssetKey([table_name])`.
        deps (Optional[Sequence[Union[AssetsDefinition, SourceAsset, str, AssetKey]]]):
            A list of assets to add as sources.
        upstream_assets (Optional[Set[AssetKey]]): Deprecated, use deps instead. A list of assets to add as sources.
        freshness_policy (Optional[FreshnessPolicy]): A freshness policy to apply to the assets
        stream_to_asset_map (Optional[Mapping[str, str]]): A mapping of an Airbyte stream name to a Dagster asset.
            This allows the use of the "prefix" setting in Airbyte with special characters that aren't valid asset names.
    """
    if upstream_assets is not None and deps is not None:
        raise DagsterInvalidDefinitionError(
            "Cannot specify both deps and upstream_assets to build_airbyte_assets. Use only deps"
            " instead."
        )

    asset_key_prefix = check.opt_sequence_param(asset_key_prefix, "asset_key_prefix", of_type=str)

    # Generate a list of outputs, the set of destination tables plus any affiliated
    # normalization tables
    tables = chain.from_iterable(
        chain([destination_tables], normalization_tables.values() if normalization_tables else [])
    )
    outputs = {
        table: AssetOut(
            is_required=False,
            key=AssetKey([*asset_key_prefix, table]),
            metadata=(
                {"table_schema": MetadataValue.table_schema(schema_by_table_name[table])}
                if schema_by_table_name
                else None
            ),
            freshness_policy=freshness_policy,
        )
        for table in tables
    }

    internal_deps = {}

    # If normalization tables are specified, we need to add a dependency from the destination table
    # to the affilitated normalization table
    if normalization_tables:
        for base_table, derived_tables in normalization_tables.items():
            for derived_table in derived_tables:
                internal_deps[derived_table] = {AssetKey([*asset_key_prefix, base_table])}

    upstream_deps = deps
    if upstream_assets is not None:
        upstream_deps = list(upstream_assets)

    # All non-normalization tables depend on any user-provided upstream assets
    for table in destination_tables:
        internal_deps[table] = set(upstream_deps) if upstream_deps else set()

    @multi_asset(
        name=f"airbyte_sync_{connection_id[:5]}",
        deps=upstream_deps,
        outs=outputs,
        internal_asset_deps=internal_deps,
        compute_kind="airbyte",
        group_name=group_name,
        can_subset=True,
    )
    def _assets(context, airbyte: BaseAirbyteResource):
        ab_output = airbyte.sync_and_poll(connection_id=connection_id)

        # No connection details (e.g. using Airbyte Cloud) means we just assume
        # that the outputs were produced
        if len(ab_output.connection_details) == 0:
            for table_name in destination_tables:
                yield Output(
                    value=None,
                    output_name=_table_to_output_name_fn(table_name),
                )
                if normalization_tables:
                    for dependent_table in normalization_tables.get(table_name, set()):
                        yield Output(
                            value=None,
                            output_name=_table_to_output_name_fn(dependent_table),
                        )
        else:
            for materialization in generate_materializations(
                ab_output, asset_key_prefix, stream_to_asset_map
            ):
                table_name = materialization.asset_key.path[-1]
                if table_name in destination_tables:
                    yield Output(
                        value=None,
                        output_name=_table_to_output_name_fn(table_name),
                        metadata=materialization.metadata,
                    )
                    # Also materialize any normalization tables affiliated with this destination
                    # e.g. nested objects, lists etc
                    if normalization_tables:
                        for dependent_table in normalization_tables.get(table_name, set()):
                            yield Output(
                                value=None,
                                output_name=_table_to_output_name_fn(dependent_table),
                            )
                else:
                    yield materialization

    return [_assets]


airbyte_assets = build_airbyte_assets(
    connection_id=connection_id['connection_id'],
    destination_tables=[table_name for table_name in TABLES_LIST],
    asset_key_prefix=[SCHEMA_NAME],
    group_name=ASSET_GROUP_NAME
)

job_name = define_asset_job(
    "job_name",
    selection=[
        *[
            f'{SCHEMA_NAME}/{table_name}'
            for table_name in TABLES_LIST],
    ],
    tags={"dagster/priority": "11",
          "dagster-k8s/config": {"job_spec_config": {
                                                        'ttl_seconds_after_finished': 100
                                                    }
                                 }
          }
)
