from pathlib import Path

import pandas as pd


def load_csv_grid(file_path: str | Path) -> pd.DataFrame:
    """
    Load a CSV grid file into a pandas DataFrame.

    Intended use:
    - backtest parameter grids
    - deterministic worker config tables

    Notes:
    - file must exist
    - file must not be empty
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"CSV grid file not found: {path}")

    df = pd.read_csv(path)

    if df.empty:
        raise ValueError(f"CSV grid file is empty: {path}")

    return df


def load_csv_grid_as_records(file_path: str | Path) -> list[dict]:
    """
    Load CSV grid and return list[dict].

    Useful when worker wants plain Python records instead of DataFrame.
    """
    df = load_csv_grid(file_path)
    return df.to_dict(orient="records")