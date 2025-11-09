from dataclasses import dataclass
from pathlib import Path
from typing import Literal


@dataclass
class ETLConfig:
    """
    Simple config holder — keeps all the paths and options in one place.
    """

    input_dir: Path
    output_dir: Path
    output_format: Literal["parquet", "sqlite"] = "parquet"
    sqlite_db_name: str = "quickshop_etl.db"
    orders_pattern: str = "orders*.csv"
