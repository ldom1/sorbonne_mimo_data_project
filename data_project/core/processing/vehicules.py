import pandas as pd
import pandera.pandas as pa
from loguru import logger
from pandera.typing import Series


class VehiculesInputSchema(pa.DataFrameModel):
    Num_Acc: Series[str]
    id_vehicule: Series[str]
    num_veh: Series[str]
    senc: Series[int] = pa.Field(ge=-1)
    catv: Series[int] = pa.Field(ge=-1, le=99)
    obs: Series[int] = pa.Field(ge=-1, le=17)
    obsm: Series[int] = pa.Field(ge=-1, le=9)
    choc: Series[int] = pa.Field(ge=-1, le=9)
    manv: Series[int] = pa.Field(ge=-1, le=26)
    motor: Series[int] = pa.Field(ge=-1, le=6)
    occutc: Series[str] = pa.Field(nullable=True)

    class Config:
        coerce = True


class VehiculesOutputSchema(pa.DataFrameModel):
    Num_Acc: Series[str]
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

    class Config:
        coerce = True


SENC_MAP = {
    -1: "Non renseigné",
    0: "Inconnu",
    1: "PK ou numéro d'itinéraire croissant",
    2: "PK ou numéro d'itinéraire décroissant",
}

CATV_MAP = {
    -1: "Non renseigné",
    0: "Indéterminable",
    1: "Bicyclette",
    2: "Cyclomoteur <50cm3",
    3: "Voiturette",
    4: "VL seul",
    5: "VL + caravane",
    6: "VL + remorque",
    7: "VU seul 1,5T <= PTAC <= 3,5T avec ou sans remorque",
    8: "VU seul 1,5T <= PTAC <= 3,5T avec ou sans remorque",
    9: "PL seul 3,5T <PTCA <= 7,5T",
    10: "PL seul > 7,5T",
    11: "PL > 3,5T + remorque",
    13: "PL seul 3,5T <PTCA <= 7,5T",
    14: "PL seul > 7,5T",
    15: "PL > 3,5T + remorque",
    16: "Tracteur routier seul",
    17: "Tracteur routier + semi-remorque",
    18: "Tracteur routier + semi-remorque",
    19: "Engin spécial",
    20: "Tracteur agricole",
    21: "Tramway",
    30: "Scooter < 50 cm³",
    31: "Motocyclette > 50 cm³ et <= 125 cm³",
    32: "Scooter > 50 cm³ et <= 125 cm³",
    33: "Motocyclette > 125 cm³",
    34: "Scooter > 125 cm³",
    35: "Quad léger <= 50 cm³",
    36: "Quad lourd > 50 cm³",
    37: "Autobus",
    38: "Autocar",
    39: "Train",
    40: "Tramway",
    41: "3RM <= 50 cm³",
    42: "3RM > 50 cm³ <= 125 cm³",
    43: "3RM > 125 cm³",
    50: "EDP à moteur",
    60: "EDP sans moteur",
    80: "VAE",
    99: "Autre véhicule",
}

OBS_MAP = {
    -1: "Non renseigné",
    0: "Sans objet",
    1: "Véhicule en stationnement",
    2: "Arbre",
    3: "Glissière métallique",
    4: "Glissière béton",
    5: "Autre glissière",
    6: "Bâtiment, mur, pile de pont",
    7: "Support de signalisation verticale ou poste d'appel d'urgence",
    8: "Poteau",
    9: "Mobilier urbain",
    10: "Parapet",
    11: "Ilot, refuge, borne haute",
    12: "Bordure de trottoir",
    13: "Fossé, talus, paroi rocheuse",
    14: "Autre obstacle fixe sur chaussée",
    15: "Autre obstacle fixe sur trottoir ou accotement",
    16: "Sortie de chaussée sans obstacle",
    17: "Buse – tête d'aqueduc",
}

OBSM_MAP = {
    -1: "Non renseigné",
    0: "Aucun",
    1: "Piéton",
    2: "Véhicule",
    4: "Véhicule sur rail",
    5: "Animal domestique",
    6: "Animal sauvage",
    9: "Autre",
}

CHOC_MAP = {
    -1: "Non renseigné",
    0: "Aucun",
    1: "Avant",
    2: "Avant droit",
    3: "Avant gauche",
    4: "Arrière",
    5: "Arrière droit",
    6: "Arrière gauche",
    7: "Côté droit",
    8: "Côté gauche",
    9: "Chocs multiples (tonneaux)",
}

MANV_MAP = {
    -1: "Non renseigné",
    0: "Inconnue",
    1: "Sans changement de direction",
    2: "Même sens, même file",
    3: "Entre 2 files",
    4: "En marche arrière",
    5: "A contresens",
    6: "En franchissant le terre-plein central",
    7: "Dans le couloir bus, dans le même sens",
    8: "Dans le couloir bus, dans le sens inverse",
    9: "En s'insérant",
    10: "En faisant demi-tour sur la chaussée",
    11: "Changeant de file à gauche",
    12: "Changeant de file à droite",
    13: "Déporté à gauche",
    14: "Déporté à droite",
    15: "Tournant à gauche",
    16: "Tournant à droite",
    17: "Dépassant à gauche",
    18: "Dépassant à droite",
    19: "Traversant la chaussée",
    20: "Manœuvre de stationnement",
    21: "Manœuvre d'évitement",
    22: "Ouverture de porte",
    23: "Arrêté (hors stationnement)",
    24: "En stationnement (avec occupants)",
    25: "Circulant sur trottoir",
    26: "Autres manœuvres",
}

MOTOR_MAP = {
    -1: "Non renseigné",
    0: "Inconnue",
    1: "Hydrocarbures",
    2: "Hybride électrique",
    3: "Electrique",
    4: "Hydrogène",
    5: "Humaine",
    6: "Autre",
}


def process(df: pd.DataFrame) -> pd.DataFrame:
    logger.info(f"Processing vehicules data from {df.head()}")
    try:
        schema = VehiculesInputSchema.validate(df)
    except pa.errors.SchemaErrors as e:
        logger.error(f"Error processing vehicules data: {e}")
        raise e

    result = pd.DataFrame()
    result["Num_Acc"] = schema["Num_Acc"]
    result["id_vehicule"] = schema["id_vehicule"]
    result["num_veh"] = schema["num_veh"]
    senc_mapped = schema["senc"].copy()
    senc_mapped[~senc_mapped.isin(SENC_MAP.keys())] = -1
    result["sens_circulation"] = senc_mapped.map(SENC_MAP)
    result["categorie_vehicule"] = schema["catv"].map(CATV_MAP)
    result["obstacle_fixe"] = schema["obs"].map(OBS_MAP)
    result["obstacle_mobile"] = schema["obsm"].map(OBSM_MAP)
    result["point_choc_initial"] = schema["choc"].map(CHOC_MAP)
    result["manoeuvre_principale"] = schema["manv"].map(MANV_MAP)
    result["type_motorisation"] = schema["motor"].map(MOTOR_MAP)
    result["nombre_occupants_tc"] = (
        pd.to_numeric(schema["occutc"], errors="coerce").fillna(0).astype(int)
    )

    return VehiculesOutputSchema.validate(result)
