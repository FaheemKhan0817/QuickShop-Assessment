"""
Behavior:
- Read products.csv and inventory.csv
- Discover order files matching pattern (orders_YYYYMMDD.csv)
- Optionally filter files by filename date range
- Read, validate and concatenate order files
- Optionally filter orders by order_date range
- Compute order_total, keep only completed orders
- Enrich with product and inventory metadata
- Write output to Parquet or SQLite
"""

from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime

import logging
import re
import pandas as pd

from .io import read_csv, write_parquet, write_sqlite
from .exception import ValidationError, ETLError

logger = logging.getLogger(__name__)


# ---- Schemas ----
PRODUCTS_SCHEMA: Dict[str, str] = {
    "product_id": "int",
    "product_name": "str",
    "category": "str",
    "price": "float",
}

INVENTORY_SCHEMA: Dict[str, str] = {
    "product_id": "int",
    "warehouse_id": "str",
    "stock_on_hand": "int",
    "last_restock_date": "date",
}

# NOTE: order_total is not an input column — we compute it later.
ORDERS_SCHEMA: Dict[str, str] = {
    "order_id": "int",
    "order_date": "date",
    "user_id": "int",
    "product_id": "int",
    "qty": "int",
    "unit_price": "float",
    "order_status": "str",
}


# ---- Validation helpers ----
def _ensure_columns(
    df: pd.DataFrame, schema: Dict[str, str], name: str
) -> None:
    missing = [c for c in schema.keys() if c not in df.columns]
    if missing:
        raise ValidationError(
            f"[{name}] missing columns: {', '.join(missing)}"
        )


def _cast_series(
    series: pd.Series, target: str, col_label: str
) -> pd.Series:
    try:
        if target == "int":
            return pd.to_numeric(series, errors="raise").astype("Int64")
        if target == "float":
            return pd.to_numeric(series, errors="raise").astype(float)
        if target == "date":
            # keep pandas datetime64[ns] for easier filtering later
            return pd.to_datetime(series, errors="raise")
        if target == "str":
            return series.astype(str)
        return series
    except Exception as exc:
        raise ValidationError(
            f"Failed to coerce {col_label} to {target}: {exc}"
        ) from exc


def validate_dataframe(
    df: pd.DataFrame, schema: Dict[str, str], name: str
) -> pd.DataFrame:
    """
    Validate presence of required columns and coerce types.
    Returns a new DataFrame with coerced dtypes (or raises
    ValidationError).
    """
    _ensure_columns(df, schema, name)
    out = df.copy()
    for col, typ in schema.items():
        out[col] = _cast_series(out[col], typ, f"{name}.{col}")
    return out


# Convenience wrappers
def validate_products(df: pd.DataFrame) -> pd.DataFrame:
    return validate_dataframe(df, PRODUCTS_SCHEMA, "products")


def validate_inventory(df: pd.DataFrame) -> pd.DataFrame:
    return validate_dataframe(df, INVENTORY_SCHEMA, "inventory")


def validate_orders(df: pd.DataFrame) -> pd.DataFrame:
    return validate_dataframe(df, ORDERS_SCHEMA, "orders")


# ---- File discovery ----
def _discover_order_files(data_dir: Path, pattern: str) -> List[Path]:
    """
    Return a sorted list of Path objects matching the glob pattern
    inside data_dir. Uses Path.glob for consistent pathlib behavior.
    """
    files = sorted(data_dir.glob(pattern))
    logger.debug(
        "discovered %d order files using pattern %s", len(files), pattern
    )
    return files


def _filter_files_by_filename_date(
    files: List[Path],
    start: Optional[datetime],
    end: Optional[datetime],
) -> List[Path]:
    """
    Keep only files whose filename includes an 8-digit YYYYMMDD date
    inside the requested window. If a file doesn't include a date, we
    keep it (conservative choice).
    """
    if not start and not end:
        return files

    filtered: List[Path] = []
    for p in files:
        m = re.search(r"(\d{8})", p.stem)
        if not m:
            # filename does not contain a date; include by default
            filtered.append(p)
            continue

        try:
            file_dt = datetime.strptime(m.group(1), "%Y%m%d")
        except ValueError:
            # invalid date token in filename — skip it
            logger.debug(
                "skipping file with invalid date token: %s", p.name
            )
            continue

        if start and file_dt < start:
            continue
        if end and file_dt > end:
            continue
        filtered.append(p)

    logger.debug("after filename-date filter: %d files remain", len(filtered))
    return filtered


# ---- Transform / Enrich ----
def _compute_order_totals(orders: pd.DataFrame) -> pd.DataFrame:
    # ensure numeric before multiplication
    orders = orders.copy()
    orders["qty"] = pd.to_numeric(
        orders["qty"], errors="raise"
    ).astype(float)
    orders["unit_price"] = pd.to_numeric(
        orders["unit_price"], errors="raise"
    ).astype(float)
    orders["order_total"] = (orders["qty"] * orders["unit_price"]).round(2)
    return orders


def enrich_orders(
    orders: pd.DataFrame,
    products: pd.DataFrame,
    inventory: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate order_total, keep only completed orders, and enrich with
    product and inventory data. Returns a new DataFrame.
    """
    if orders.empty:
        return orders.copy()

    df = _compute_order_totals(orders)

    # keep only completed orders (case-insensitive)
    before = len(df)
    df = df[df["order_status"].str.lower() == "completed"].copy()
    logger.debug("filtered completed orders: %d -> %d", before, len(df))

    # left-join product metadata
    prod_cols = ["product_id", "product_name", "category"]
    df = df.merge(
        products[prod_cols].drop_duplicates(
            subset=["product_id"]
        ),
        on="product_id",
        how="left",
        validate="m:1",
    )

    # left-join inventory (bring current stock_on_hand)
    inv_cols = ["product_id", "stock_on_hand"]
    df = df.merge(
        inventory[inv_cols].drop_duplicates(subset=["product_id"]),
        on="product_id",
        how="left",
        validate="m:1",
    )

    # keep columns in a friendly order
    ordered = [
        "order_id",
        "order_date",
        "user_id",
        "order_status",
        "product_id",
        "product_name",
        "category",
        "qty",
        "unit_price",
        "order_total",
        "stock_on_hand",
    ]
    ordered = [c for c in ordered if c in df.columns]
    others = [c for c in df.columns if c not in ordered]
    df = df[ordered + others]

    return df


# ---- Runner ----
def run_etl(
    input_dir: Path,
    output_dir: Path,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    output_format: str = "parquet",
    sqlite_db_name: str = "quickshop_etl.db",
    orders_pattern: str = "orders*.csv",
) -> Path:
    """
    Orchestrate the ETL run. Returns the path to the created artifact
    (parquet file or sqlite DB).
    """
    logger.info(
        "starting ETL run; input=%s output=%s", input_dir, output_dir
    )

    # Normalize date filters to midnight timestamps if provided
    if start_date is not None:
        start_date = pd.to_datetime(start_date).normalize()
    if end_date is not None:
        end_date = pd.to_datetime(end_date).normalize()

    # Read static reference files
    products_path = Path(input_dir) / "products.csv"
    inventory_path = Path(input_dir) / "inventory.csv"
    if not products_path.exists() or not inventory_path.exists():
        raise ETLError(
            f"products.csv or inventory.csv not found in {input_dir}"
        )

    products_raw = read_csv(products_path)
    inventory_raw = read_csv(inventory_path)

    # Discover order files and optionally filter by filename date token
    all_order_files = _discover_order_files(Path(input_dir), orders_pattern)
    files_to_use = _filter_files_by_filename_date(
        all_order_files, start_date, end_date
    )
    if not files_to_use:
        raise ETLError("no order files found for the requested range")

    # Read and concatenate order CSVs
    frames = []
    for p in files_to_use:
        logger.debug("reading orders file: %s", p.name)
        df = read_csv(p)
        df["_source"] = p.name
        frames.append(df)

    orders_raw = (
        pd.concat(frames, ignore_index=True) if frames
        else pd.DataFrame()
    )

    # Validate and coerce schemas
    products = validate_products(products_raw)
    inventory = validate_inventory(inventory_raw)
    orders = validate_orders(orders_raw)

    # Filter by order_date column if start/end provided
    if start_date is not None:
        orders = orders[orders["order_date"] >= start_date]
    if end_date is not None:
        # treat end_date as inclusive end of day
        orders = orders[
            orders["order_date"]
            <= end_date + pd.Timedelta(days=1) - pd.Timedelta(microseconds=1)
        ]

    # Transform & enrich
    enriched = enrich_orders(orders, products, inventory)

    # Write output
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if output_format == "parquet":
        if start_date and end_date and start_date.date() == end_date.date():
            fname = f"orders_{start_date.strftime('%Y-%m-%d')}.parquet"
        elif start_date and end_date:
            fname = (
                f"orders_{start_date.strftime('%Y%m%d')}"
                f"_to_{end_date.strftime('%Y%m%d')}.parquet"
            )
        else:
            fname = f"orders_{pd.Timestamp.now():%Y%m%d_%H%M%S}.parquet"
        out_path = out_dir / fname
        write_parquet(enriched, out_path)
        logger.info("wrote parquet: %s", out_path)
        return out_path

    elif output_format == "sqlite":
        db_path = out_dir / sqlite_db_name
        # write_sqlite in io.py accepts if_exists argument now
        write_sqlite(products, db_path, "products", if_exists="replace")
        write_sqlite(inventory, db_path, "inventory", if_exists="replace")
        write_sqlite(enriched, db_path, "orders", if_exists="replace")
        logger.info("wrote sqlite database: %s", db_path)
        return db_path

    else:
        raise ETLError(f"unsupported output format: {output_format}")