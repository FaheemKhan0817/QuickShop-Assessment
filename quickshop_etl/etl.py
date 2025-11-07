import logging
import glob
from pathlib import Path
from typing import List, Optional, Dict
from datetime import datetime
import pandas as pd
import re

from .config import ETLConfig
from .io import read_csv, write_parquet, write_sqlite
from .exception import ValidationError, ETLError

log = logging.getLogger(__name__)

# --- Expected column types ---
PRODUCTS_SCHEMA = {
    "product_id": "int",
    "product_name": "str",
    "category": "str",
    "price": "float",
}

INVENTORY_SCHEMA = {
    "product_id": "int",
    "warehouse_id": "str",
    "stock_on_hand": "int",
    "last_restock_date": "date",
}

ORDERS_SCHEMA = {
    "order_id": "int",
    "order_date": "date",
    "user_id": "int",
    "product_id": "int",
    "qty": "int",
    "unit_price": "float",
    "order_status": "str",
    "order_total": "float",  # we'll add this
}

def _check_columns(df: pd.DataFrame, expected: Dict[str, str], name: str) -> None:
    missing = [col for col in expected if col not in df.columns]
    if missing:
        raise ValidationError(f"[{name}] Missing columns: {', '.join(missing)}")

def _cast_column(series: pd.Series, col_type: str, col_name: str):
    try:
        if col_type == "int":
            return pd.to_numeric(series, errors='raise').astype('Int64')
        if col_type == "float":
            return pd.to_numeric(series, errors='raise')
        if col_type == "date":
            return pd.to_datetime(series, errors='raise').dt.date
        if col_type == "str":
            return series.astype(str)
        return series
    except Exception as e:
        raise ValidationError(f"Failed to convert {col_name}: {e}")

def validate_df(df: pd.DataFrame, schema: Dict[str, str], name: str) -> pd.DataFrame:
    """Validate columns and coerce types. Raise ValidationError on failure."""
    _check_columns(df, schema, name)
    df = df.copy()
    for col, typ in schema.items():
        df[col] = _cast_column(df[col], typ, f"{name}.{col}")
    return df

def validate_products(df): return validate_df(df, PRODUCTS_SCHEMA, "products")
def validate_inventory(df): return validate_df(df, INVENTORY_SCHEMA, "inventory")
def validate_orders(df): return validate_df(df, ORDERS_SCHEMA, "orders")

# --- File discovery ---
def _find_order_files(base_dir: Path, pattern: str, start: Optional[datetime], end: Optional[datetime]) -> List[Path]:
    """Find order files, optionally filter by date in filename."""
    matches = sorted(Path(p) for p in glob.glob(str(base_dir / pattern)))
    if not (start or end):
        return matches

    filtered = []
    for path in matches:
        # Look for YYYYMMDD in filename like orders_20251023.csv
        m = re.search(r"(\d{8})", path.stem)
        if m:
            try:
                file_date = datetime.strptime(m.group(1), "%Y%m%d").date()
                if (not start or file_date >= start.date()) and (not end or file_date <= end.date()):
                    filtered.append(path)
            except ValueError:
                pass  # skip bad date
        else:
            filtered.append(path)  # no date? include anyway
    return filtered

# --- Transformation ---
def enrich_orders(orders: pd.DataFrame, products: pd.DataFrame, inventory: pd.DataFrame) -> pd.DataFrame:
    """Join product info, calculate total, keep only completed."""
    orders = orders.copy()
    orders["order_total"] = (orders["qty"] * orders["unit_price"]).round(2)

    # Keep only completed
    before = len(orders)
    orders = orders[orders["order_status"].str.lower() == "completed"]
    log.debug("Filtered completed orders: %d → %d", before, len(orders))

    # Join product details
    prod_cols = ["product_id", "product_name", "category"]
    orders = orders.merge(products[prod_cols], on="product_id", how="left")

    # Join current stock
    orders = orders.merge(inventory[["product_id", "stock_on_hand"]], on="product_id", how="left")

    return orders

# --- Main ETL ---
def run_etl(
    input_dir: Path,
    output_dir: Path,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    output_format: str = "parquet",
    sqlite_db: str = "quickshop_etl.db",
    orders_pattern: str = "orders*.csv",
) -> Path:
    """Load, validate, transform, write."""
    log.info("Starting ETL run")

    # --- Load raw ---
    try:
        products = read_csv(input_dir / "products.csv")
        inventory = read_csv(input_dir / "inventory.csv")
    except FileNotFoundError as e:
        raise ETLError(f"Missing static file: {e}")

    order_files = _find_order_files(input_dir, orders_pattern, start_date, end_date)
    if not order_files:
        raise ETLError("No order files found")

    # --- Combine orders ---
    order_frames = []
    for f in order_files:
        log.debug("Reading orders: %s", f.name)
        df = read_csv(f)
        df["_source"] = f.name
        order_frames.append(df)
    orders_raw = pd.concat(order_frames, ignore_index=True)

    # --- Validate ---
    products = validate_products(products)
    inventory = validate_inventory(inventory)
    orders_raw = validate_orders(orders_raw)

    # --- Date filter (if not already filtered by filename) ---
    if start_date:
        orders_raw = orders_raw[pd.to_datetime(orders_raw["order_date"]) >= start_date]
    if end_date:
        orders_raw = orders_raw[pd.to_datetime(orders_raw["order_date"]) <= end_date]

    # --- Transform ---
    enriched = enrich_orders(orders_raw, products, inventory)

    # --- Write output ---
    output_dir.mkdir(parents=True, exist_ok=True)

    if output_format == "parquet":
        if start_date and end_date and start_date.date() == end_date.date():
            fname = f"orders_{start_date.strftime('%Y-%m-%d')}.parquet"
        elif start_date and end_date:
            fname = f"orders_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.parquet"
        else:
            fname = f"orders_all_{pd.Timestamp.now():%Y%m%d_%H%M%S}.parquet"
        out_path = output_dir / fname
        write_parquet(enriched, out_path)
        log.info("Wrote Parquet: %s", out_path)
        return out_path

    elif output_format == "sqlite":
        db_path = output_dir / sqlite_db
        write_sqlite(products, db_path, "products", replace=True)
        write_sqlite(inventory, db_path, "inventory", replace=True)
        write_sqlite(enriched, db_path, "orders", replace=False)
        log.info("Wrote SQLite DB: %s", db_path)
        return db_path

    else:
        raise ETLError(f"Unknown format: {output_format}")