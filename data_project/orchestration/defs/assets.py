import dagster as dg

from data_project.api.get_data import get_data
from data_project.core.analysis import generate_paris_map
from data_project.core.config import (
    CARACT_FILE_RAW,
    CARACT_FILE_SILVER,
    DATA_GOLD,
    LIEUX_FILE_RAW,
    LIEUX_FILE_SILVER,
    PARIS_MAP_PATH,
    RAW_DATA_PATH,
    USAGERS_FILE_RAW,
    USAGERS_FILE_SILVER,
    VEHICULES_FILE_RAW,
    VEHICULES_FILE_SILVER,
)
from data_project.core.processing import (
    process_caract,
    process_lieux,
    process_usagers,
    process_vehicules,
)
from data_project.core.processing.join import process_jointure
from data_project.core.utils import get_metadata, read_data, write_data


@dg.asset(description="Téléchargement des données brutes")
def dg_download_data(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info("Downloading data from data.gouv.fr...")
    nb_success, nb_errors = get_data()
    context.log.info(f"Successfully downloaded {nb_success} files")
    context.log.info(f"Failed to download {nb_errors} files")
    context.log.info(f"Data downloaded and saved to {RAW_DATA_PATH}.")
    return dg.MaterializeResult(
        metadata={
            "nb_success": nb_success,
            "nb_errors": nb_errors,
        }
    )


@dg.asset(description="Traitement des données caract", deps=[dg_download_data])
def dg_process_caract(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(f"Processing caract data from {CARACT_FILE_RAW}...")
    df = read_data(path=CARACT_FILE_RAW)
    df_processed = process_caract(df=df)
    write_data(df=df_processed, path=CARACT_FILE_SILVER)
    context.log.info(f"Caract data processed and saved to {CARACT_FILE_SILVER}.")

    return dg.MaterializeResult(
        metadata=get_metadata(
            df=df_processed,
            input_path=CARACT_FILE_RAW,
            output_path=CARACT_FILE_SILVER,
        ),
        value=df_processed,
    )


@dg.asset(description="Traitement des données lieux", deps=[dg_download_data])
def dg_process_lieux(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(f"Processing lieux data from {LIEUX_FILE_RAW}...")
    df = read_data(path=LIEUX_FILE_RAW)
    df_processed = process_lieux(df)
    write_data(df=df_processed, path=LIEUX_FILE_SILVER)
    context.log.info(f"Lieux data processed and saved to {LIEUX_FILE_SILVER}.")
    return dg.MaterializeResult(
        metadata=get_metadata(
            df=df_processed,
            input_path=LIEUX_FILE_RAW,
            output_path=LIEUX_FILE_SILVER,
        ),
        value=df_processed,
    )


@dg.asset(description="Traitement des données usagers", deps=[dg_download_data])
def dg_process_usagers(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(f"Processing usagers data from {USAGERS_FILE_RAW}...")
    df = read_data(path=USAGERS_FILE_RAW)
    df_processed = process_usagers(df)
    write_data(df=df_processed, path=USAGERS_FILE_SILVER)
    context.log.info(f"Usagers data processed and saved to {USAGERS_FILE_SILVER}.")
    return dg.MaterializeResult(
        metadata=get_metadata(
            df=df_processed,
            input_path=USAGERS_FILE_RAW,
            output_path=USAGERS_FILE_SILVER,
        ),
        value=df_processed,
    )


@dg.asset(description="Traitement des données vehicules", deps=[dg_download_data])
def dg_process_vehicules(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    context.log.info(f"Processing vehicules data from {VEHICULES_FILE_RAW}...")
    df = read_data(path=VEHICULES_FILE_RAW)
    df_processed = process_vehicules(df)
    write_data(df=df_processed, path=VEHICULES_FILE_SILVER)
    context.log.info(f"Vehicules data processed and saved to {VEHICULES_FILE_SILVER}.")
    return dg.MaterializeResult(
        metadata=get_metadata(
            df=df_processed,
            input_path=VEHICULES_FILE_RAW,
            output_path=VEHICULES_FILE_SILVER,
        ),
        value=df_processed,
    )


@dg.asset(description="Jointure des données caract, lieux, vehicules et usagers")
def dg_process_jointure(
    context: dg.AssetExecutionContext,
    dg_process_vehicules: dg.MaterializeResult,
    dg_process_usagers: dg.MaterializeResult,
    dg_process_lieux: dg.MaterializeResult,
    dg_process_caract: dg.MaterializeResult,
) -> dg.MaterializeResult:
    context.log.info(
        "Processing jointure of caract, lieux, vehicules and usagers data..."
    )

    try:
        df = process_jointure(
            dg_process_caract,
            dg_process_lieux,
            dg_process_vehicules,
            dg_process_usagers,
        )
    except Exception as e:
        context.log.error(
            f"Error processing jointure of caract, lieux, vehicules and usagers data: {e}"
        )
        context.log.error(f"With: dg_process_caract={dg_process_caract}")
        context.log.error(f"With: dg_process_lieux={dg_process_lieux}")
        context.log.error(f"With: dg_process_vehicules={dg_process_vehicules}")
        context.log.error(f"With: dg_process_usagers={dg_process_usagers}")
        raise e
    write_data(df=df, path=DATA_GOLD)
    context.log.info(f"Jointure data processed and saved to {DATA_GOLD}.")
    return dg.MaterializeResult(
        metadata=get_metadata(
            df=df, input_path=CARACT_FILE_SILVER, output_path=DATA_GOLD
        ),
        value=df,
    )


@dg.asset(description="Génération de la carte des accidents parisiens")
def dg_generate_analytics(
    context: dg.AssetExecutionContext, dg_process_jointure: dg.MaterializeResult
) -> dg.MaterializeResult:
    context.log.info("Generating analytics and visualizations...")
    try:
        generate_paris_map(df=dg_process_jointure, output_path=PARIS_MAP_PATH)
    except Exception as e:
        context.log.error(f"Error generating analytics and visualizations: {e}")
        context.log.error(f"With: dg_process_jointure={dg_process_jointure}")
        raise e
    context.log.info(f"Paris accidents map generated and saved to {PARIS_MAP_PATH}.")
    return dg.MaterializeResult(metadata={"map_path": str(PARIS_MAP_PATH)})
