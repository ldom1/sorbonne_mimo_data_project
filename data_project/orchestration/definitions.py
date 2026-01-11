from dagster import Definitions, load_assets_from_modules

from data_project.orchestration.defs import assets, jobs

defs = Definitions(
    assets=load_assets_from_modules([assets]),
    jobs=[jobs.data_project_workflow],
    asset_checks=[
        assets.dg_check_jointure,
        assets.dg_check_generate_analytics,
    ],
)
