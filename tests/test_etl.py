import pytest
from pathlib import Path
import pandas as pd
import numpy as np
from quickshop_etl.etl import (
    validate_products,
    validate_inventory,
    validate_orders,
    enrich_orders,
    run_etl,
)
from quickshop_etl.exception import ValidationError, ETLError


# --- Sample DataFrames for testing ---
PRODUCTS_SAMPLE = pd.DataFrame(
    {
        "product_id": [1001, 1002],
        "product_name": ["Classic Tee", "Running Shoes"],
        "category": ["Apparel", "Footwear"],
        "price": [19.99, 79.99],
    }
)

INVENTORY_SAMPLE = pd.DataFrame(
    {
        "product_id": [1001, 1002],
        "warehouse_id": ["W1", "W1"],
        "stock_on_hand": [120, 80],
        "last_restock_date": ["2025-10-18", "2025-10-18"],
    }
)

ORDERS_SAMPLE = pd.DataFrame(
    {
        "order_id": [60001, 60002],
        "order_date": ["2025-10-23", "2025-10-23"],
        "product_id": [1001, 1002],
        "qty": [2, 1],
        "unit_price": [19.99, 79.99],
        "user_id": [9047, 9042],
        "order_status": ["completed", "cancelled"],
    }
)


# --- Tests ---


def test_validate_products_success():
    df = validate_products(PRODUCTS_SAMPLE)
    assert set(df.columns) == {
        "product_id",
        "product_name",
        "category",
        "price",
    }
    # product_id should be pandas nullable Int64
    assert df["product_id"].dtype.name == "Int64"
    # price should be a float dtype
    assert pd.api.types.is_float_dtype(df["price"])


def test_validate_inventory_success():
    df = validate_inventory(INVENTORY_SAMPLE)
    assert set(df.columns) == {
        "product_id",
        "warehouse_id",
        "stock_on_hand",
        "last_restock_date",
    }
    assert df["stock_on_hand"].dtype.name == "Int64"
    assert pd.api.types.is_datetime64_any_dtype(df["last_restock_date"])


def test_validate_orders_success():
    df = validate_orders(ORDERS_SAMPLE)
    expected_cols = {
        "order_id",
        "order_date",
        "product_id",
        "qty",
        "unit_price",
        "user_id",
        "order_status",
    }
    assert set(df.columns) >= expected_cols
    assert df["order_id"].dtype.name == "Int64"


def test_enrich_orders_computation_and_filtering():
    # validate and then enrich
    orders = validate_orders(ORDERS_SAMPLE)
    products = validate_products(PRODUCTS_SAMPLE)
    inventory = validate_inventory(INVENTORY_SAMPLE)
    enriched = enrich_orders(orders, products, inventory)

    # Check order_total calculation for order 60001: qty 2 * 19.99 = 39.98
    val = enriched.loc[enriched["order_id"] == 60001, "order_total"].iloc[0]
    assert float(np.round(val, 2)) == 39.98

    # Cancelled order (60002) should be filtered out
    assert 60002 not in enriched["order_id"].values

    # Product info merged correctly
    assert "product_name" in enriched.columns
    assert (
        enriched.loc[enriched["order_id"] == 60001, "product_name"].iloc[0]
        == "Classic Tee"
    )


def test_run_etl_creates_parquet(tmp_path):
    # Prepare input files in a temporary directory
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    PRODUCTS_SAMPLE.to_csv(data_dir / "products.csv", index=False)
    INVENTORY_SAMPLE.to_csv(data_dir / "inventory.csv", index=False)
    ORDERS_SAMPLE.to_csv(data_dir / "orders_20251023.csv", index=False)

    out_dir = tmp_path / "output"

    result_path = run_etl(
        input_dir=data_dir,
        output_dir=out_dir,
        output_format="parquet",
        start_date=None,
        end_date=None,
        orders_pattern="orders*.csv",
    )

    assert Path(result_path).exists()
    df = pd.read_parquet(result_path)

    # Basic expectations
    assert "order_total" in df.columns
    # Only completed order remains
    assert 60001 in df["order_id"].values
    assert 60002 not in df["order_id"].values

    # Verify order_total numerically for all rows
    calc = (df["qty"].astype(float) * df["unit_price"].astype(float)).round(2)
    # Compare elementwise (allow for floating rounding)
    mismatches = df[np.abs(df["order_total"].astype(float) - calc) > 1e-6]
    assert mismatches.shape[0] == 0


def test_run_etl_missing_files_raises(tmp_path):
    # If products or inventory are missing, ETL should raise ETLError
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    out_dir = tmp_path / "output"

    with pytest.raises(ETLError):
        run_etl(input_dir=data_dir, output_dir=out_dir)


def test_invalid_schema_raises_validation_error():
    bad_products = pd.DataFrame({"wrong_col": [1, 2]})
    with pytest.raises(ValidationError):
        validate_products(bad_products)