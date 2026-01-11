from pathlib import Path

import requests
from loguru import logger

from data_project.core.config import RAW_DATA_PATH


def download_file(url: str, output_path: Path) -> None:
    """Download a file from URL to output path."""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)


def get_data(year: int = 2024) -> None:
    """Download 2024 accident data files from data.gouv.fr."""
    # Get dataset to find all 2024 resources
    dataset_slug = "bases-de-donnees-annuelles-des-accidents-corporels-de-la-circulation-routiere-annees-de-2005-a-2024"
    api_url = f"https://www.data.gouv.fr/api/1/datasets/{dataset_slug}/"
    response = requests.get(api_url)
    response.raise_for_status()
    dataset = response.json()

    # Map file patterns to output names
    file_patterns = {
        f"Caract_{year}.csv": f"caract-{year}.csv",
        f"Lieux_{year}.csv": f"lieux-{year}.csv",
        f"Usagers_{year}.csv": f"usagers-{year}.csv",
        f"Vehicules_{year}.csv": f"vehicules-{year}.csv",
    }

    nb_success = 0
    nb_errors = 0
    for resource in dataset.get("resources", []):
        title = resource.get("title", "").lower()
        if str(year) in title and resource.get("format") == "csv":
            for pattern, filename in file_patterns.items():
                if pattern.lower() in title:
                    if (RAW_DATA_PATH / filename).exists():
                        logger.info(
                            f"{pattern} (> {filename}) already exists, skipping..."
                        )
                        nb_success += 1
                        break
                    url = resource.get("url")
                    if url:
                        download_file(url=url, output_path=RAW_DATA_PATH / filename)
                        logger.success(f"Downloaded {pattern} (> {filename})")
                        nb_success += 1
                    break

    nb_errors = len(file_patterns) - nb_success

    return nb_success, nb_errors


if __name__ == "__main__":
    nb_success, nb_errors = get_data()
    print(f"Successfully downloaded {nb_success} files")
    print(f"Failed to download {nb_errors} files")
