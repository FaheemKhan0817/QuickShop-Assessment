import pytest
from pathlib import Path
import pandas as pd
from quickshop_etl.etl import (
    validate_products,
    validate_inventory,
    validate_orders,
    transform_orders,
    load_and_process,
)
from quickshop_etl.exception import ValidationError, ETLError
import tempfile

# --- Sample DataFrames for testing ---

PRODUCTS_SAMPLE = pd.DataFrame({
    "product_id": [1001, 1002],
    "product_name": ["Classic Tee", "Running Shoes"],
    "category": ["Apparel", "Footwear"],
    "price": [19.99, 79.99]
})

INVENTORY_SAMPLE = pd.DataFrame({
    "product_id": [1001, 1002],
    "warehouse_id": ["W1", "W1"],
    "stock_on_hand": [120, 80],
    "last_restock_date": ["2025-10-18", "2025-10-18"]
})

ORDERS_SAMPLE = pd.DataFrame({
    "order_id": [60001, 60002],
    "order_date": ["2025-10-23", "2025-10-23"],
    "product_id": [1001, 1002],
    "qty": [2, 1],
    "unit_price": [19.99, 79.99],
    "user_id": [9047, 9042],
    "order_status": ["completed", "cancelled"]
})

# --- Tests ---

def test_validate_products_success():
    df = validate_products(PRODUCTS_SAMPLE)
    assert set(df.columns) == {"product_id", "product_name", "category", "price"}
    assert df["product_id"].dtype.name == "Int64"
    assert df["price"].dtype == float

def test_validate_inventory_success():
    df = validate_inventory(INVENTORY_SAMPLE)
    assert set(df.columns) == {"product_id", "warehouse_id", "stock_on_hand", "last_restock_date"}
    assert df["stock_on_hand"].dtype.name == "Int64"
    assert pd.api.types.is_datetime64_any_dtype(df["last_restock_date"])

def test_validate_orders_success():
    df = validate_orders(ORDERS_SAMPLE)
    assert set(df.columns) == {"order_id", "order_date", "product_id", "qty", "unit_price", "user_id", "order_status"}
    assert df["order_id"].dtype.name == "Int64"

def test_transform_orders_computation_and_filtering():
    orders = validate_orders(ORDERS_SAMPLE)
    transformed = transform_orders(orders, PRODUCTS_SAMPLE)
    
    # Check order_total calculation
    assert transformed.loc[transformed["order_id"] == 60001, "order_total"].iloc[0] == 39.98
    
    # Cancelled orders should be filtered out by default
    assert 60002 not in transformed["order_id"].values
    
    # Product info merged
    assert "product_name" in transformed.columns
    assert transformed.loc[transformed["order_id"] == 60001, "product_name"].iloc[0] == "Classic Tee"

def test_load_and_process_creates_parquet(tmp_path):
    # Prepare temp input files
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    PRODUCTS_SAMPLE.to_csv(data_dir / "products.csv", index=False)
    INVENTORY_SAMPLE.to_csv(data_dir / "inventory.csv", index=False)
    ORDERS_SAMPLE.to_csv(data_dir / "orders_20251023.csv", index=False)
    
    output_dir = tmp_path / "output"
    
    result_path = load_and_process(
        input_dir=data_dir,
        output_dir=output_dir,
        output_format="parquet",
        start_date=None,
        end_date=None,
        orders_pattern="orders*.csv"
    )
    
    # Check file was created
    assert result_path.exists()
    df = pd.read_parquet(result_path)
    assert "order_total" in df.columns
    # Only completed orders remain
    assert 60001 in df["order_id"].values
    assert 60002 not in df["order_id"].values

def test_load_and_process_missing_files(tmp_path):
    # Should raise ETLError if products.csv or inventory.csv missing
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    output_dir = tmp_path / "output"
    
    with pytest.raises(ETLError):
        load_and_process(
            input_dir=data_dir,
            output_dir=output_dir
        )

def test_invalid_schema_raises_validation_error():
    bad_products = pd.DataFrame({"wrong_col": [1, 2]})
    with pytest.raises(ValidationError):
        validate_products(bad_products)
