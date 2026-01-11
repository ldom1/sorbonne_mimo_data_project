import pandas as pd
import pandera.pandas as pa
from loguru import logger
from pandera.typing import Series


class UsagersInputSchema(pa.DataFrameModel):
    Num_Acc: Series[str]
    id_usager: Series[str]
    id_vehicule: Series[str]
    num_veh: Series[str]
    place: Series[int] = pa.Field(ge=-1, le=10)
    catu: Series[int] = pa.Field(ge=1, le=3)
    grav: Series[int] = pa.Field(ge=1, le=4)
    sexe: Series[int] = pa.Field(ge=-1, le=2)
    an_nais: Series[float] = pa.Field(ge=1900, le=2024, nullable=True)
    trajet: Series[int] = pa.Field(ge=-1, le=9)
    secu1: Series[int] = pa.Field(ge=-1, le=9)
    secu2: Series[int] = pa.Field(ge=-1, le=9)
    secu3: Series[int] = pa.Field(ge=-1, le=9)
    locp: Series[int] = pa.Field(ge=-1, le=9)
    actp: Series[str]
    etatp: Series[int] = pa.Field(ge=-1, le=3)

    class Config:
        coerce = True


class UsagersOutputSchema(pa.DataFrameModel):
    Num_Acc: Series[str]
    id_usager: Series[str]
    id_vehicule: Series[str]
    num_veh: Series[str]
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


CATU_MAP = {1: "Conducteur", 2: "Passager", 3: "Piéton"}

GRAV_MAP = {1: "Indemne", 2: "Tué", 3: "Blessé hospitalisé", 4: "Blessé léger"}

SEXE_MAP = {-1: "Non renseigné", 1: "Masculin", 2: "Féminin"}

TRAJET_MAP = {
    -1: "Non renseigné",
    0: "Non renseigné",
    1: "Domicile – travail",
    2: "Domicile – école",
    3: "Courses – achats",
    4: "Utilisation professionnelle",
    5: "Promenade – loisirs",
    9: "Autre",
}

SECU_MAP = {
    -1: "Non renseigné",
    0: "Aucun équipement",
    1: "Ceinture",
    2: "Casque",
    3: "Dispositif enfants",
    4: "Gilet réfléchissant",
    5: "Airbag (2RM/3RM)",
    6: "Gants (2RM/3RM)",
    7: "Gants + Airbag (2RM/3RM)",
    8: "Non déterminable",
    9: "Autre",
}

LOCP_MAP = {
    -1: "Non renseigné",
    0: "Sans objet",
    1: "A + 50 m du passage piéton",
    2: "A – 50 m du passage piéton",
    3: "Sur passage piéton sans signalisation lumineuse",
    4: "Sur passage piéton avec signalisation lumineuse",
    5: "Sur trottoir",
    6: "Sur accotement",
    7: "Sur refuge ou BAU",
    8: "Sur contre allée",
    9: "Inconnue",
}

ACTP_MAP = {
    -1: "Non renseigné",
    0: "Non renseigné ou sans objet",
    1: "Sens véhicule heurtant",
    2: "Sens inverse du véhicule",
    3: "Traversant",
    4: "Masqué",
    5: "Jouant – courant",
    6: "Avec animal",
    9: "Autre",
    "A": "Monte/descend du véhicule",
    "B": "Inconnue",
    10: "Monte/descend du véhicule",
    11: "Inconnue",
}

ETATP_MAP = {-1: "Non renseigné", 1: "Seul", 2: "Accompagné", 3: "En groupe"}


def process(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Processing usagers data from {df.head()}")
    try:
        schema = UsagersInputSchema.validate(df)
    except pa.errors.SchemaErrors as e:
        logger.error(f"Error processing usagers data: {e}")
        raise e

    result = pd.DataFrame()
    result["Num_Acc"] = schema["Num_Acc"]
    result["id_usager"] = schema["id_usager"]
    result["id_vehicule"] = schema["id_vehicule"]
    result["num_veh"] = schema["num_veh"]
    result["place"] = schema["place"].replace(-1, None)
    result["categorie_usager"] = schema["catu"].map(CATU_MAP)
    result["gravite"] = schema["grav"].map(GRAV_MAP)
    result["sexe"] = schema["sexe"].map(SEXE_MAP)
    result["age"] = (pd.Timestamp.now().year - schema["an_nais"]).where(
        schema["an_nais"].notna(), None
    )
    result["trajet"] = schema["trajet"].map(TRAJET_MAP)
    result["equipement_secu_1"] = schema["secu1"].map(SECU_MAP)
    result["equipement_secu_2"] = schema["secu2"].map(SECU_MAP)
    result["equipement_secu_3"] = schema["secu3"].map(SECU_MAP)
    result["localisation_pieton"] = schema["locp"].map(LOCP_MAP)
    actp_clean = pd.to_numeric(schema["actp"], errors="coerce").fillna(schema["actp"])
    result["action_pieton"] = actp_clean.map(ACTP_MAP).fillna("Non renseigné")
    result["etat_pieton"] = schema["etatp"].map(ETATP_MAP)

    return UsagersOutputSchema.validate(result)
