from data_project.core.config import (
    CARACT_FILE_RAW,
    CARACT_FILE_SILVER,
    DATA_GOLD,
    LIEUX_FILE_RAW,
    LIEUX_FILE_SILVER,
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
from data_project.core.utils import read_data, write_data


def process_data() -> None:
    caract_df = read_data(CARACT_FILE_RAW)
    lieux_df = read_data(LIEUX_FILE_RAW)
    vehicules_df = read_data(VEHICULES_FILE_RAW)
    usagers_df = read_data(USAGERS_FILE_RAW)

    write_data(process_caract(caract_df), CARACT_FILE_SILVER)
    write_data(process_lieux(lieux_df), LIEUX_FILE_SILVER)
    write_data(process_vehicules(vehicules_df), VEHICULES_FILE_SILVER)
    write_data(process_usagers(usagers_df), USAGERS_FILE_SILVER)

    gold_df = process_jointure(
        read_data(CARACT_FILE_SILVER),
        read_data(LIEUX_FILE_SILVER),
        read_data(VEHICULES_FILE_SILVER),
        read_data(USAGERS_FILE_SILVER),
    )
    write_data(gold_df, DATA_GOLD)
