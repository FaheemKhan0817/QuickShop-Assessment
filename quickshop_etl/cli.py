import argparse
from pathlib import Path
import logging
from datetime import datetime
from .config import ETLConfig
from .etl import load_and_process

logger = logging.getLogger(__name__)

def parse_args():
    p = argparse.ArgumentParser("quickshop_etl")
    p.add_argument("--input-dir", "-i", default="data", help="Input directory with CSV files (products.csv, inventory.csv, orders_YYYYMMDD.csv)")
    p.add_argument("--output-dir", "-o", default="output", help="Output directory for results")
    p.add_argument("--output-format", "-f", choices=["parquet", "sqlite"], default="parquet")
    p.add_argument("--start-date", help="Start date (YYYY-MM-DD) to filter orders")
    p.add_argument("--end-date", help="End date (YYYY-MM-DD) to filter orders")
    p.add_argument("--sqlite-name", default="quickshop_etl.db", help="SQLite DB filename (if sqlite chosen)")
    p.add_argument("--orders-pattern", default="orders*.csv", help="Glob pattern to discover orders files (default: orders*.csv)")
    p.add_argument("--verbose", "-v", action="store_true", help="Enable debug logging")
    return p.parse_args()

def main():
    args = parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None

    config = ETLConfig(
        input_dir=input_dir,
        output_dir=output_dir,
        output_format=args.output_format,
        sqlite_db_name=args.sqlite_name,
        orders_pattern=args.orders_pattern
    )

    logger.info("Running ETL with config: %s", config)
    result = load_and_process(
        input_dir=config.input_dir,
        output_dir=config.output_dir,
        start_date=start_date,
        end_date=end_date,
        output_format=config.output_format,
        sqlite_db_name=config.sqlite_db_name,
        orders_pattern=config.orders_pattern,
    )
    logger.info("ETL finished. Result: %s", result)