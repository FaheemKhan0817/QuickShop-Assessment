#!/usr/bin/env python3
import argparse
import logging
from pathlib import Path
from datetime import datetime
from .config import ETLConfig
from .etl import run_etl
log = logging.getLogger(__name__)

def build_parser():
    p = argparse.ArgumentParser(description="QuickShop ETL — CSV → Parquet/SQLite")
    p.add_argument("-i", "--input-dir", type=Path, default="data", help="Folder with CSVs")
    p.add_argument("-o", "--output-dir", type=Path, default="output", help="Where to save results")
    p.add_argument("-f", "--output-format", choices=["parquet", "sqlite"], default="parquet")
    p.add_argument("--sqlite-name", default="quickshop_etl.db", help="SQLite filename")
    p.add_argument("--start-date", help="Filter orders from YYYY-MM-DD")
    p.add_argument("--end-date", help="Filter orders to YYYY-MM-DD")
    p.add_argument("--orders-pattern", default="orders*.csv", help="Glob for order files")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug logs")
    return p

def main():
    args = build_parser().parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    start = datetime.strptime(args.start_date, "%Y-%m-%d") if args.start_date else None
    end = datetime.strptime(args.end_date, "%Y-%m-%d") if args.end_date else None

    config = ETLConfig(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        output_format=args.output_format,
        sqlite_db_name=args.sqlite_name,
        orders_pattern=args.orders_pattern
    )

    log.info("ETL config: %s", config)

    result = run_etl(
        input_dir=config.input_dir,
        output_dir=config.output_dir,
        start_date=start,
        end_date=end,
        output_format=config.output_format,
        sqlite_db=config.sqlite_db_name,
        orders_pattern=config.orders_pattern,
    )

    log.info("Done! Output: %s", result)

if __name__ == "__main__":
    main()