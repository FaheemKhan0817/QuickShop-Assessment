from pathlib import Path
import sqlite3
import logging
from typing import Optional
import pandas as pd

logger = logging.getLogger(__name__)

def read_csv(path: Path, **read_kwargs) -> pd.DataFrame:
    """
    Simple CSV reader wrapper with logging. Will raise if file not found.
    """
    logger.debug("Reading CSV from %s", path)
    return pd.read_csv(path, **read_kwargs)

def write_parquet(df: pd.DataFrame, path: Path, engine: Optional[str] = "pyarrow") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    logger.debug("Writing Parquet to %s", path)
    # Use to_parquet - requires pyarrow or fastparquet installed
    df.to_parquet(path, engine=engine, index=False)

def write_sqlite(df: pd.DataFrame, db_path: Path, table_name: str, if_exists: str = "replace") -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    logger.debug("Writing DataFrame to SQLite DB %s table %s", db_path, table_name)
    try:
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
    finally:
        conn.close()