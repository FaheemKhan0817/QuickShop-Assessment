#!/usr/bin/env python3
"""
Run the quickshop_etl CLI. Example:
python run_etl.py --input-dir data --output-dir output --start-date 2025-10-01 --end-date 2025-10-31
"""
from quickshop_etl import cli

if __name__ == "__main__":
    cli.main()
