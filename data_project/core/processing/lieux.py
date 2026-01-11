import pandas as pd
import pandera.pandas as pa
from loguru import logger
from pandera.typing import Series


class LieuxInputSchema(pa.DataFrameModel):
    Num_Acc: Series[str]
    catr: Series[int] = pa.Field(ge=-1, le=9)
    voie: Series[str] = pa.Field(nullable=True)
    v1: Series[int] = pa.Field(ge=-1)
    v2: Series[str] = pa.Field(nullable=True)
    circ: Series[int] = pa.Field(ge=-1, le=4)
    nbv: Series[int] = pa.Field(ge=-1)
    vosp: Series[int] = pa.Field(ge=-1, le=3)
    prof: Series[int] = pa.Field(ge=-1, le=4)
    pr: Series[int]
    pr1: Series[int]
    plan: Series[int] = pa.Field(ge=-1, le=4)
    lartpc: Series[str] = pa.Field(nullable=True)
    larrout: Series[int] = pa.Field(ge=-1)
    surf: Series[int] = pa.Field(ge=-1, le=9)
    infra: Series[int] = pa.Field(ge=-1, le=9)
    situ: Series[int] = pa.Field(ge=-1, le=9)
    vma: Series[int] = pa.Field(ge=-1)

    class Config:
        coerce = True


class LieuxOutputSchema(pa.DataFrameModel):
    Num_Acc: Series[str]
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

    class Config:
        coerce = True


CATR_MAP = {
    -1: "Non renseigné",
    1: "Autoroute",
    2: "Route nationale",
    3: "Route Départementale",
    4: "Voie communale",
    5: "Hors réseau public",
    6: "Parc de stationnement ouvert à la circulation publique",
    7: "Routes de métropole urbaine",
    9: "Autre",
}

CIRC_MAP = {
    -1: "Non renseigné",
    1: "À sens unique",
    2: "Bidirectionnelle",
    3: "À chaussées séparées",
    4: "À sens unique",
}

VOSP_MAP = {
    -1: "Non renseigné",
    0: "Sans objet",
    1: "Piste cyclable",
    2: "Bande cyclable",
    3: "Voie réservée",
}

PROF_MAP = {
    -1: "Non renseigné",
    1: "Plat",
    2: "Pente",
    3: "Sommet de côte",
    4: "Bas de côte",
}

PR_MAP = {
    -1: "Non renseigné",
    0: "Sans objet",
    1: "Passage piéton",
    2: "Passage piéton à moins de 50 m",
}

PLAN_MAP = {
    -1: "Non renseigné",
    1: "Partie rectiligne",
    2: "En courbe à gauche",
    3: "En courbe à droite",
    4: "En « S »",
}

SURF_MAP = {
    -1: "Non renseigné",
    1: "Normale",
    2: "Mouillée",
    3: "Flaques",
    4: "Inondée",
    5: "Enneigée",
    6: "Boue",
    7: "Verglacée",
    8: "Corps gras - huile",
    9: "Autre",
}

INFRA_MAP = {
    -1: "Non renseigné",
    0: "Aucune",
    1: "Souterrain - tunnel",
    2: "Pont - autopont",
    3: "Bretelle d'échangeur ou de raccordement",
    4: "Voie ferrée",
    5: "Carrefour aménagé",
    6: "Zone piétonne",
    7: "Zone de péage",
    8: "Chantier",
    9: "Autre",
}

SITU_MAP = {
    -1: "Non renseigné",
    0: "Aucune",
    1: "Sur chaussée",
    2: "Sur bande d'arrêt d'urgence",
    3: "Sur accotement",
    4: "Sur trottoir",
    5: "Sur piste cyclable",
    6: "Sur autre voie spéciale",
    8: "Autre",
    9: "Autre",
}


def pre_process(df: pd.DataFrame) -> pd.DataFrame:
    df_clean = df.copy()
    for col in ["nbv", "pr", "pr1", "v1", "larrout", "vma"]:
        if col in df_clean.columns:
            df_clean[col] = df_clean[col].astype(str).str.strip().str.replace(" ", "")
            if col == "nbv":
                df_clean[col] = df_clean[col].replace(["-1", "#VALEURMULTI"], "-1")
            df_clean[col] = (
                pd.to_numeric(df_clean[col], errors="coerce").fillna(-1).astype(int)
            )
    return df_clean


def process(df: pd.DataFrame) -> pd.DataFrame:
    """Process the lieux data"""
    logger.info(f"Processing lieux data from {df.head()}")

    df_clean = pre_process(df)

    try:
        schema = LieuxInputSchema.validate(df_clean)
    except pa.errors.SchemaErrors as e:
        logger.error(f"Error processing lieux data: {e}")
        raise e

    result = pd.DataFrame()
    result["Num_Acc"] = schema["Num_Acc"]
    result["categorie_route"] = schema["catr"].map(CATR_MAP)
    result["voie"] = schema["voie"]
    result["numero_route"] = schema["v1"].replace(-1, None)
    result["voie_secondaire"] = schema["v2"]
    result["regime_circulation"] = schema["circ"].map(CIRC_MAP)
    result["nombre_voies"] = schema["nbv"].replace(-1, None)
    result["voie_reservee"] = schema["vosp"].map(VOSP_MAP)
    result["profil"] = schema["prof"].map(PROF_MAP)
    pr_mapped = schema["pr"].copy()
    pr_mapped[~pr_mapped.isin(PR_MAP.keys())] = -1
    result["presence_pieton"] = pr_mapped.map(PR_MAP)
    pr1_clean = schema["pr1"].copy()
    pr1_clean[pr1_clean < 0] = -1
    result["numero_point_rencontre"] = pr1_clean.replace(-1, None)
    result["plan"] = schema["plan"].map(PLAN_MAP)
    result["largeur_trottoir"] = pd.to_numeric(schema["lartpc"], errors="coerce")
    result["largeur_route"] = schema["larrout"].replace(-1, None)
    result["surface"] = schema["surf"].map(SURF_MAP)
    result["infrastructure"] = schema["infra"].map(INFRA_MAP)
    result["situation"] = schema["situ"].map(SITU_MAP)
    result["vitesse_max_autorisee"] = schema["vma"].replace(-1, None)

    return LieuxOutputSchema.validate(result)
