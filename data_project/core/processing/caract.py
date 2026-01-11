import pandas as pd
import pandera.pandas as pa
from loguru import logger
from pandera.typing import Series


class CaractInputSchema(pa.DataFrameModel):
    Num_Acc: Series[str]
    jour: Series[int] = pa.Field(ge=1, le=31)
    mois: Series[int] = pa.Field(ge=1, le=12)
    an: Series[int] = pa.Field(ge=2005, le=2024)
    hrmn: Series[str]
    lum: Series[int] = pa.Field(ge=-1, le=5)
    dep: Series[str]
    com: Series[str]
    agg: Series[int] = pa.Field(ge=-1, le=2)
    int: Series[int] = pa.Field(ge=-1, le=9)
    atm: Series[int] = pa.Field(ge=-1, le=9)
    col: Series[int] = pa.Field(ge=-1, le=7)
    adr: Series[str] = pa.Field(nullable=True)
    lat: Series[str]
    long: Series[str]

    class Config:
        coerce = True


class CaractOutputSchema(pa.DataFrameModel):
    Num_Acc: Series[str]
    jour: Series[int]
    mois: Series[int]
    an: Series[int]
    datetime: Series[pd.Timestamp]
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

    class Config:
        coerce = True


LUMINOSITE_MAP = {
    -1: "Non renseigné",
    1: "Plein jour",
    2: "Crépuscule ou aube",
    3: "Nuit sans éclairage public",
    4: "Nuit avec éclairage public non allumé",
    5: "Nuit avec éclairage public allumé",
}

AGG_MAP = {-1: "Non renseigné", 1: "Hors agglomération", 2: "En agglomération"}

INT_MAP = {
    -1: "Non renseigné",
    0: "Hors intersection",
    1: "Intersection en X",
    2: "Intersection en T",
    3: "Intersection en Y",
    4: "Intersection à plus de 4 branches",
    5: "Giratoire",
    6: "Place",
    7: "Passage à niveau",
    8: "Autre intersection",
    9: "Autre intersection",
}

ATM_MAP = {
    -1: "Non renseigné",
    0: "Normale",
    1: "Pluie légère",
    2: "Pluie forte",
    3: "Neige - grêle",
    4: "Brouillard - fumée",
    5: "Vent fort - tempête",
    6: "Temps éblouissant",
    7: "Temps couvert",
    8: "Autre",
    9: "Autre",
}

COL_MAP = {
    -1: "Non renseigné",
    0: "Sans collision",
    1: "Deux véhicules - frontale",
    2: "Deux véhicules - par l'arrière",
    3: "Deux véhicules - par le côté",
    4: "Trois véhicules et plus - en chaîne",
    5: "Trois véhicules et plus - collisions multiples",
    6: "Autre collision",
    7: "Sans collision",
}


def process(df: pd.DataFrame) -> pd.DataFrame:
    """Process the caract data"""
    logger.info(f"Processing caract data from {df.head()}")
    try:
        schema = CaractInputSchema.validate(df)
    except pa.errors.SchemaErrors as e:
        logger.error(f"Error processing caract data: {e}")
        raise e

    result = pd.DataFrame()
    result["Num_Acc"] = schema["Num_Acc"]
    result["jour"] = schema["jour"]
    result["mois"] = schema["mois"]
    result["an"] = schema["an"]
    result["heure"] = schema["hrmn"].str.split(":").str[0]
    result["minute"] = schema["hrmn"].str.split(":").str[1]
    result["luminosite"] = schema["lum"].map(LUMINOSITE_MAP)
    result["departement"] = schema["dep"]
    result["commune"] = schema["com"]
    result["agglomeration"] = schema["agg"].map(AGG_MAP)
    result["intersection"] = schema["int"].map(INT_MAP)
    result["conditions_atmospheriques"] = schema["atm"].map(ATM_MAP)
    result["type_collision"] = schema["col"].map(COL_MAP)
    result["adresse"] = schema["adr"]
    result["latitude"] = schema["lat"].str.replace(",", ".").astype(float)
    result["longitude"] = schema["long"].str.replace(",", ".").astype(float)

    # Build date and datetime columns
    result["datetime"] = pd.to_datetime(
        result["an"].astype(str)
        + "-"
        + result["mois"].astype(str).str.zfill(2)
        + "-"
        + result["jour"].astype(str).str.zfill(2)
        + " "
        + result["heure"].astype(str).str.zfill(2)
        + ":"
        + result["minute"].astype(str).str.zfill(2)
    )

    return CaractOutputSchema.validate(result)
