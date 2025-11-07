import logging
from pathlib import Path
import pandas as pd
import sqlite3

log = logging.getLogger(__name__)

def read_csv(path: Path, **kwargs) -> pd.DataFrame:
    """Just a thin wrapper so we can log and reuse."""
    log.debug("Reading CSV: %s", path)
    return pd.read_csv(path, **kwargs)

def write_parquet(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    log.debug("Writing Parquet: %s", path)
    df.to_parquet(path, index=False)

def write_sqlite(df: pd.DataFrame, db_path: Path, table: str, replace: bool = False) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    mode = "replace" if replace else "append"
    log.debug("Writing to SQLite: %s → %s (%s)", db_path, table, mode)
    with sqlite3.connect(db_path) as conn:
        df.to_sql(table, conn, if_exists=mode, index=False)