from dataclasses import dataclass
from pathlib import Path
from typing import Literal

@dataclass
class ETLConfig:
    input_dir: Path
    output_dir: Path
    output_format: Literal["parquet", "sqlite"] = "parquet"
    sqlite_db_name: str = "quickshop_etl.db"
    products_file: str = "products.csv"
    inventory_file: str = "inventory.csv"
    # Support either a single orders.csv or multiple files like orders_YYYYMMDD.csv
    orders_pattern: str = "orders*.csv"