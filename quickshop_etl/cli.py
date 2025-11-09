# quickshop_etl/cli.py
"""
Command-line entrypoint for QuickShop ETL.
"""

import argparse
import logging
from pathlib import Path
from datetime import datetime
from .config import ETLConfig
from .etl import run_etl

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="QuickShop ETL — CSV -> Parquet / SQLite")
    p.add_argument("-i", "--input-dir", type=Path, default=Path("data"), help="Folder containing CSV files")
    p.add_argument("-o", "--output-dir", type=Path, default=Path("output"), help="Directory to write outputs")
    p.add_argument("-f", "--output-format", choices=["parquet", "sqlite"], default="parquet", help="Output format")
    p.add_argument("--sqlite-name", default="quickshop_etl.db", help="SQLite filename (when using sqlite)")
    p.add_argument("--start-date", help="Start date filter (YYYY-MM-DD)")
    p.add_argument("--end-date", help="End date filter (YYYY-MM-DD)")
    p.add_argument("--orders-pattern", default="orders*.csv", help="Glob pattern for orders files")
    p.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    return p


def main():
    args = build_parser().parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    # Parse optional dates
    start = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    end = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None

    # Build a simple config object (convenience)
    config = ETLConfig(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        output_format=args.output_format,
        sqlite_db_name=args.sqlite_name,
        orders_pattern=args.orders_pattern
    )

    logger.info("ETL config: %s", config)

    # Call run_etl with the expected parameter name `sqlite_db_name`
    result = run_etl(
        input_dir=config.input_dir,
        output_dir=config.output_dir,
        start_date=start,
        end_date=end,
        output_format=config.output_format,
        sqlite_db_name=config.sqlite_db_name,   # <-- fixed name
        orders_pattern=config.orders_pattern,
    )

    logger.info("Output: %s", result)


if __name__ == "__main__":
    main()
