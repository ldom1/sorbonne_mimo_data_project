from pathlib import Path

import pandas as pd


def read_data(path: Path) -> pd.DataFrame:
    return pd.read_csv(path, sep=";")


def write_data(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False, header=True, sep=";")


def get_metadata(df: pd.DataFrame, input_path: Path, output_path: Path) -> dict:
    return {
        "rows": df.shape[0],
        "cols": df.shape[1],
        "input_path": str(input_path),
        "output_path": str(output_path),
    }
