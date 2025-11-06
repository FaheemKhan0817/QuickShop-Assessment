from pathlib import Path
from typing import Dict, Optional, List
import pandas as pd
import logging
from datetime import datetime
from .exception import ValidationError, ETLError
from .io import read_csv, write_parquet, write_sqlite
from .config import ETLConfig
import glob

logger = logging.getLogger(__name__)

# Expected schemas as column -> pandas dtype hint
EXPECTED_PRODUCTS = {
    "product_id": "int",
    "product_name": "str",
    "category": "str",
    "price": "float",
}

EXPECTED_INVENTORY = {
    "product_id": "int",
    "warehouse_id": "str",
    "stock_on_hand": "int",
    "last_restock_date": "date",
}

EXPECTED_ORDERS = {
    "order_id": "int",
    "order_date": "date",
    "product_id": "int",
    "qty": "int",
    "unit_price": "float",
    "user_id": "int",
    "order_status": "str",
}

def _validate_columns(df: pd.DataFrame, expected: Dict[str, str]) -> None:
    missing = [c for c in expected.keys() if c not in df.columns]
    if missing:
        logger.error("Missing columns: %s", missing)
        raise ValidationError(f"Missing columns: {missing}")

def _coerce_columns(df: pd.DataFrame, expected: Dict[str, str], df_name: str) -> pd.DataFrame:
    import pandas as pd

    _validate_columns(df, expected)

    df = df.copy()
    errors = []
    for col, typ in expected.items():
        try:
            if typ == "int":
                # Use nullable integer dtype
                df[col] = pd.to_numeric(df[col], errors="raise", downcast="integer").astype("Int64")
            elif typ == "float":
                df[col] = pd.to_numeric(df[col], errors="raise").astype(float)
            elif typ == "date":
                df[col] = pd.to_datetime(df[col], errors="raise").dt.normalize()
            elif typ == "str":
                df[col] = df[col].astype(str)
            else:
                df[col] = df[col]
        except Exception as e:
            errors.append(f"{df_name}.{col}: {e}")
    if errors:
        logger.error("Type coercion errors: %s", errors)
        raise ValidationError("Type coercion errors: " + "; ".join(errors))
    return df

def validate_products(df: pd.DataFrame) -> pd.DataFrame:
    return _coerce_columns(df, EXPECTED_PRODUCTS, "products")

def validate_inventory(df: pd.DataFrame) -> pd.DataFrame:
    return _coerce_columns(df, EXPECTED_INVENTORY, "inventory")

def validate_orders(df: pd.DataFrame) -> pd.DataFrame:
    return _coerce_columns(df, EXPECTED_ORDERS, "orders")

def _discover_order_files(input_dir: Path, pattern: str) -> List[Path]:
    """
    Discover order CSV files in input_dir matching pattern.
    Prefer a single 'orders.csv' if present, otherwise glob 'orders*.csv'.
    Returns a list of Path objects. If none found, returns empty list.
    """
    input_dir = Path(input_dir)
    # exact file
    single = input_dir / "orders.csv"
    if single.exists():
        logger.debug("Found single orders.csv at %s", single)
        return [single]

    # glob pattern (e.g., orders_*.csv or orders*.csv)
    glob_pattern = str(input_dir / pattern)
    files = [Path(p) for p in glob.glob(glob_pattern)]
    files_sorted = sorted(files)
    logger.debug("Discovered %d order files with pattern %s", len(files_sorted), glob_pattern)
    return files_sorted

def _read_and_concat_orders(files: List[Path]) -> pd.DataFrame:
    """
    Read multiple order CSVs and concatenate into a single DataFrame.
    Raises ETLError if no files provided.
    """
    if not files:
        raise ETLError("No order files found to read.")
    frames = []
    for p in files:
        logger.debug("Reading orders file: %s", p)
        try:
            df = read_csv(p)
            # keep a column to track source file (optional, useful for debugging)
            df["_source_file"] = p.name
            frames.append(df)
        except Exception as e:
            logger.exception("Failed to read orders file %s: %s", p, e)
            raise ETLError(f"Failed to read orders file {p}: {e}")
    combined = pd.concat(frames, ignore_index=True, sort=False)
    logger.info("Combined %d order records from %d files", len(combined), len(files))
    return combined

def transform_orders(
    orders: pd.DataFrame,
    products: pd.DataFrame,
    inventory: Optional[pd.DataFrame] = None,
    keep_cancelled: bool = False
) -> pd.DataFrame:
    """
    Transform and enrich orders dataset:
    - compute order_total = qty * unit_price
    - filter to completed orders by default
    - join product_name and category
    - optionally join inventory (latest stock_on_hand)
    """
    orders = orders.copy()
    logger.debug("Starting transformation on %d orders", len(orders))

    # Ensure numeric types for calculation; rely on validate_orders to have coerced types where possible
    orders["order_total"] = (orders["qty"].astype(float) * orders["unit_price"].astype(float)).round(2)

    # Filter statuses
    if not keep_cancelled:
        before = len(orders)
        orders = orders[orders["order_status"].str.lower() == "completed"]
        logger.debug("Filtered orders: %d -> %d (keep_completed only)", before, len(orders))

    # Merge product info
    prod_sel = products[["product_id", "product_name", "category", "price"]].drop_duplicates(subset=["product_id"])
    orders = orders.merge(prod_sel, on="product_id", how="left", validate="m:1")
    missing_products = int(orders["product_name"].isna().sum())
    if missing_products:
        logger.warning("There are %d orders with missing product metadata", missing_products)

    # Merge inventory for stock_on_hand (prefer inventory as-is)
    if inventory is not None:
        inv_sel = inventory[["product_id", "stock_on_hand"]].drop_duplicates(subset=["product_id"])
        orders = orders.merge(inv_sel, on="product_id", how="left", validate="m:1")

    # reorder columns for convenience
    cols = [
        "order_id", "order_date", "user_id", "order_status",
        "product_id", "product_name", "category",
        "qty", "unit_price", "order_total"
    ]
    if "stock_on_hand" in orders.columns:
        cols.append("stock_on_hand")
    # preserve any other columns (like _source_file) after these
    cols = [c for c in cols if c in orders.columns] + [c for c in orders.columns if c not in cols]
    orders = orders[cols]
    return orders

def load_and_process(
    input_dir: Path,
    output_dir: Path,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    output_format: str = "parquet",
    sqlite_db_name: str = "quickshop_etl.db",
    orders_pattern: str = "orders*.csv",
) -> Path:
    """
    Main orchestrator: read CSVs, validate, transform, write output.
    Supports multiple orders files named like orders_YYYYMMDD.csv as well as a single orders.csv.
    If start_date / end_date are provided, orders will be filtered by the order_date column.

    Returns path to written file (parquet) or DB path for sqlite.
    """
    try:
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)

        products_path = input_dir / "products.csv"
        inventory_path = input_dir / "inventory.csv"

        if not products_path.exists():
            raise ETLError(f"Products file not found at {products_path}")
        if not inventory_path.exists():
            raise ETLError(f"Inventory file not found at {inventory_path}")

        products = read_csv(products_path)
        inventory = read_csv(inventory_path)

        # discover order files
        order_files = _discover_order_files(input_dir, orders_pattern)
        if not order_files:
            raise ETLError(f"No orders files found in {input_dir} matching pattern {orders_pattern}")

        orders_raw = _read_and_concat_orders(order_files)

        # Validate / coerce schemas
        products = validate_products(products)
        inventory = validate_inventory(inventory)
        orders = validate_orders(orders_raw)

        # filter by date range if provided
        if start_date is not None:
            orders = orders[orders["order_date"] >= pd.to_datetime(start_date).normalize()]
        if end_date is not None:
            orders = orders[orders["order_date"] <= pd.to_datetime(end_date).normalize()]

        transformed = transform_orders(orders, products, inventory)

        # ensure output directory exists
        output_dir.mkdir(parents=True, exist_ok=True)

        if output_format == "parquet":
            # Name deterministic when date filters provided, otherwise use timestamp
            if start_date is not None and end_date is not None and start_date == end_date:
                filename = f"orders_{pd.Timestamp(start_date).strftime('%Y-%m-%d')}.parquet"
            elif start_date is not None and end_date is not None:
                filename = f"orders_{pd.Timestamp(start_date).strftime('%Y%m%d')}_{pd.Timestamp(end_date).strftime('%Y%m%d')}.parquet"
            else:
                filename = f"orders_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.parquet"

            out_path = output_dir / filename
            write_parquet(transformed, out_path)
            logger.info("Wrote parquet to %s", out_path)
            return out_path
        elif output_format == "sqlite":
            db_path = output_dir / sqlite_db_name
            write_sqlite(transformed, db_path, table_name="orders")
            logger.info("Wrote sqlite DB to %s", db_path)
            return db_path
        else:
            raise ETLError(f"Unknown output format: {output_format}")

    except Exception as e:
        logger.exception("ETL failed: %s", e)
        raise