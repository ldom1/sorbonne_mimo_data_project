from pathlib import Path

import folium
import pandas as pd
from folium.plugins import MarkerCluster
from loguru import logger

from data_project.core.config import DATA_GOLD, PARIS_MAP_PATH
from data_project.core.utils import read_data


def generate_map(
    df: pd.DataFrame,
    output_path: Path,
    city_filter: str = None,
    department_filter: str = None,
    color_by: str = "gravite",
) -> None:
    """Generate a map of the accidents in the city or department"""
    df_filtered = df.copy()

    if city_filter:
        df_filtered = df_filtered[
            df_filtered["commune"].str.contains(city_filter, case=False, na=False)
        ]

    if department_filter:
        df_filtered = df_filtered[df_filtered["departement"] == department_filter]

    df_filtered = df_filtered.dropna(subset=["latitude", "longitude"])

    if df_filtered.empty:
        logger.error("No data to display after filtering")
        logger.error(
            f"Original data: {df[['commune', 'departement', 'latitude', 'longitude']].head()}"
        )
        logger.error(f"City filter: {city_filter}")
        logger.error(f"Department filter: {department_filter}")
        logger.error(f"Filtered data: {df_filtered.head()}")
        raise ValueError("No data to display after filtering")

    center_lat = df_filtered["latitude"].mean()
    center_lon = df_filtered["longitude"].mean()

    m = folium.Map(location=[center_lat, center_lon], zoom_start=12)

    color_map = {
        "Indemne": "green",
        "Blessé léger": "yellow",
        "Blessé hospitalisé": "orange",
        "Tué": "red",
    }

    marker_cluster = MarkerCluster().add_to(m)

    for _, row in df_filtered.iterrows():
        color = color_map.get(row.get(color_by, "Indemne"), "gray")
        popup_text = f"""
        <b>Accident #{row.get("Num_Acc", "N/A")}</b><br>
        Date: {row.get("datetime", "N/A")}<br>
        Gravité: {row.get("gravite", "N/A")}<br>
        Route: {row.get("categorie_route", "N/A")}<br>
        Catégorie usager: {row.get("categorie_usager", "N/A")}<br>
        Age: {row.get("age", "N/A")}<br>
        Luminosité: {row.get("luminosite", "N/A")}
        """
        folium.CircleMarker(
            location=[row["latitude"], row["longitude"]],
            radius=5,
            popup=folium.Popup(popup_text, max_width=300),
            color=color,
            fill=True,
            fillColor=color,
        ).add_to(marker_cluster)

    m.save(str(output_path))


def generate_paris_map(df: pd.DataFrame = None, output_path: Path = None) -> None:
    """Generate a map of the accidents in Paris"""
    if output_path is None:
        output_path = PARIS_MAP_PATH

    if df is None:
        df = read_data(DATA_GOLD)
    generate_map(
        df=df, output_path=output_path, department_filter="75", color_by="gravite"
    )


if __name__ == "__main__":
    df = read_data(DATA_GOLD)
    generate_map(
        df=df, output_path=PARIS_MAP_PATH, department_filter="75", color_by="gravite"
    )
