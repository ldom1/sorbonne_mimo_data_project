import pandas as pd
import pandera.pandas as pa
from loguru import logger
from pandera.typing import Series

from data_project.core.processing.caract import CaractOutputSchema
from data_project.core.processing.lieux import LieuxOutputSchema
from data_project.core.processing.usagers import UsagersOutputSchema
from data_project.core.processing.vehicules import VehiculesOutputSchema


class JointureOutputSchema(pa.DataFrameModel):
    Num_Acc: Series[str]
    jour: Series[int]
    mois: Series[int]
    an: Series[int]
    heure: Series[str]
    minute: Series[str]
    luminosite: Series[str]
    departement: Series[str]
    commune: Series[str]
    agglomeration: Series[str]
    intersection: Series[str]
    conditions_atmospheriques: Series[str]
    type_collision: Series[str]
    adresse: Series[str] = pa.Field(nullable=True)
    latitude: Series[float]
    longitude: Series[float]
    datetime: Series[pd.Timestamp]
    categorie_route: Series[str]
    voie: Series[str] = pa.Field(nullable=True)
    numero_route: Series[float] = pa.Field(nullable=True)
    voie_secondaire: Series[str] = pa.Field(nullable=True)
    regime_circulation: Series[str]
    nombre_voies: Series[float] = pa.Field(nullable=True)
    voie_reservee: Series[str]
    profil: Series[str]
    presence_pieton: Series[str]
    numero_point_rencontre: Series[float] = pa.Field(nullable=True)
    plan: Series[str]
    largeur_trottoir: Series[float] = pa.Field(nullable=True)
    largeur_route: Series[float] = pa.Field(nullable=True)
    surface: Series[str]
    infrastructure: Series[str]
    situation: Series[str]
    vitesse_max_autorisee: Series[float] = pa.Field(nullable=True)
    id_vehicule: Series[str]
    num_veh: Series[str]
    sens_circulation: Series[str]
    categorie_vehicule: Series[str]
    obstacle_fixe: Series[str]
    obstacle_mobile: Series[str]
    point_choc_initial: Series[str]
    manoeuvre_principale: Series[str]
    type_motorisation: Series[str]
    nombre_occupants_tc: Series[int]
    id_usager: Series[str]
    place: Series[float] = pa.Field(nullable=True)
    categorie_usager: Series[str]
    gravite: Series[str]
    sexe: Series[str]
    age: Series[float] = pa.Field(nullable=True)
    trajet: Series[str]
    equipement_secu_1: Series[str]
    equipement_secu_2: Series[str]
    equipement_secu_3: Series[str]
    localisation_pieton: Series[str]
    action_pieton: Series[str] = pa.Field(nullable=True)
    etat_pieton: Series[str]

    class Config:
        coerce = True

    @pa.dataframe_check
    @classmethod
    def check_unique_combination(cls, df: pd.DataFrame) -> Series[bool]:
        return ~df[["Num_Acc", "id_vehicule", "id_usager"]].duplicated()


def process_jointure(
    caract_df: pd.DataFrame,
    lieux_df: pd.DataFrame,
    vehicules_df: pd.DataFrame,
    usagers_df: pd.DataFrame,
) -> pd.DataFrame:
    """Join the caract, lieux, vehicules and usagers data based on the join procedure described in the documentation"""
    logger.info("Joining caract, lieux, vehicules and usagers data...")
    # Validate schemas
    caract_df = CaractOutputSchema.validate(caract_df)
    lieux_df = LieuxOutputSchema.validate(lieux_df)
    vehicules_df = VehiculesOutputSchema.validate(vehicules_df)
    usagers_df = UsagersOutputSchema.validate(usagers_df)

    # Join data
    df = pd.merge(caract_df, lieux_df, on="Num_Acc", how="left")
    df = pd.merge(df, vehicules_df, on="Num_Acc", how="left")
    df = pd.merge(df, usagers_df, on=["Num_Acc", "id_vehicule", "num_veh"], how="left")

    # Filter out rows with null id_usager to ensure complete records only
    initial_count = len(df)
    df = df.dropna(subset=["id_usager"])
    filtered_count = len(df)
    if initial_count != filtered_count:
        logger.warning(
            f"Filtered out {initial_count - filtered_count} rows with null id_usager"
        )

    # Remove duplicates based on Num_Acc + id_vehicule + id_usager
    duplicates_before = len(df)
    df = df.drop_duplicates(
        subset=["Num_Acc", "id_vehicule", "id_usager"], keep="first"
    )
    duplicates_after = len(df)
    if duplicates_before != duplicates_after:
        logger.warning(f"Removed {duplicates_before - duplicates_after} duplicate rows")

    try:
        df = JointureOutputSchema.validate(df)
        logger.success("Data joined successfully")
        return df
    except pa.errors.SchemaErrors as e:
        logger.error(f"Error validating jointure data: {e}")
        raise e
