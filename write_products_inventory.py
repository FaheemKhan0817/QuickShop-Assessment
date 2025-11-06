from pathlib import Path
from quickshop_etl.io import write_sqlite, read_csv

# Paths
input_dir = Path("data")
output_dir = Path("output")
db_path = output_dir / "quickshop_etl.db"

# Read CSVs
products = read_csv(input_dir / "products.csv")
inventory = read_csv(input_dir / "inventory.csv")

# Write to SQLite
write_sqlite(products, db_path, table_name="products")
write_sqlite(inventory, db_path, table_name="inventory")

print(f"Products and Inventory written to {db_path}")
