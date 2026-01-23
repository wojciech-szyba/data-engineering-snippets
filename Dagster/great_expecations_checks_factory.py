import json
from dagster import asset_check, AssetCheckResult, AssetCheckSeverity, AssetKey, resource
import great_expectations as gx


@resource
def gx_context_resource(context):
    return gx.get_context(context_root_dir="...")


# Resource factory (provides GX context)
great_expectations_resource = gx_context_resource.configured({
    "context_root_dir": "..."
})


def create_ge_asset_checks_from_json_file(
        suite_path: str,
        asset_key: AssetKey
):
    with open(suite_path, 'r') as f:
        suite_data = json.load(f)

    checks = []

    for exp_config in suite_data["expectations"]:
        check_name = f"{suite_data['expectation_suite_name']}__{exp_config['expectation_type']}__" \
                     f"{exp_config['kwargs']['column']}"

        @asset_check(
            asset=asset_key,
            name=check_name,
            blocking=False,
            compute_kind="great_expectations",
            resource_defs={
                "great_expectations_context": great_expectations_resource
            }
        )
        def great_expectations_check(context, my_asset) -> AssetCheckResult:
            df = my_asset
            exp_name = context.op_def.name

            *_, expectation_type, expectation_kwargs_column = exp_name.split('__')

            df_ge = gx.from_pandas(df)
            results = df_ge.validate(expectation_suite=suite_path)

            try:
                statistics = results.statistics
            except:
                statistics = None

            results = [expectation for expectation in results['results'] if
                       expectation['expectation_config']['expectation_type'] == expectation_type and
                       expectation['expectation_config']['kwargs']['column'] == expectation_kwargs_column
                       ]

            return AssetCheckResult(
                passed=results[0].success,
                severity=AssetCheckSeverity.WARN if results[0].success else AssetCheckSeverity.ERROR,
                metadata={
                    "expectation_type": exp_config["expectation_type"],
                    "suite_name": suite_data["expectation_suite_name"],
                    "statistics": statistics
                }
            )

        checks.append(ge_check)
    return checks


