import dagster as dg

from data_project.orchestration.defs.assets import (
    dg_check_generate_analytics,
    dg_check_jointure,
    dg_download_data,
    dg_generate_analytics,
    dg_process_caract,
    dg_process_jointure,
    dg_process_lieux,
    dg_process_usagers,
    dg_process_vehicules,
)

data_project_workflow = dg.define_asset_job(
    name="data_project_workflow",
    selection=[
        dg_download_data,
        dg_process_caract,
        dg_process_lieux,
        dg_process_vehicules,
        dg_process_usagers,
        dg_process_jointure,
        dg_generate_analytics,
        dg_check_jointure,
        dg_check_generate_analytics,
    ],
)
