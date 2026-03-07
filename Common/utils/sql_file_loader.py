from pathlib import Path


def load_sql_file(file_path: str | Path) -> str:
    """
    Load a SQL file as plain text.

    Notes:
    - preserves formatting
    - strips leading/trailing whitespace
    - intended for worker sql_queries/*.txt files
    """
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"SQL file not found: {path}")

    content = path.read_text(encoding="utf-8").strip()

    if not content:
        raise ValueError(f"SQL file is empty: {path}")

    return content