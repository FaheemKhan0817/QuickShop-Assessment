# dags/quickshop_daily_pipeline.py
"""
QuickShop daily pipeline — human style, small & clear.
- Processes one day's orders file: orders_YYYYMMDD.csv
- Writes per-run table orders_YYYYMMDD to SQLite (idempotent)
- Produces summary JSON: date, total_revenue, top_category
- Produces inventory alerts CSV (optional per-day)
- Skips summary/alerts if no orders for that date
"""

from pathlib import Path
from datetime import datetime, timedelta
import json
import logging
import sqlite3

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

log = logging.getLogger(__name__)

# ------------------------
# Simple config (change paths if you prefer repo-level folders)
# ------------------------
DATA_DIR = Path.home() / "airflow" / "data"
DB_FILE = Path.home() / "airflow" / "db" / "quickshop_etl.db"
REPORTS_DIR = Path.home() / "airflow" / "reports"

DATA_DIR.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE.parent.mkdir(parents=True, exist_ok=True)

# ------------------------
# DAG defaults
# ------------------------
default_args = {
    "owner": "faheem",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ------------------------
# Helpers
# ------------------------
def _orders_file_for(date_obj):
    return DATA_DIR / f"orders_{date_obj.strftime('%Y%m%d')}.csv"

def _read_orders(date_obj):
    f = _orders_file_for(date_obj)
    if not f.exists():
        log.info("Orders file not found for %s: %s", date_obj, f.name)
        return pd.DataFrame()
    log.info("Reading orders file: %s", f.name)
    return pd.read_csv(f)

# ------------------------
# Tasks
# ------------------------
def etl_task(**context) -> dict:
    """
    Read products/inventory, read the orders file for logical_date,
    filter completed, compute order_total, write SQLite tables.
    Returns a small metadata dict (db path, table name, count).
    """
    # determine run date (logical_date preferred)
    logical_dt = context.get("logical_date") or context.get("execution_date")
    if logical_dt is None:
        logical_dt = datetime.utcnow()
    run_date = pd.Timestamp(logical_dt).date()
    log.info("ETL starting for %s", run_date)

    # static files (required)
    prod_file = next(DATA_DIR.glob("[Pp]roducts.csv"), None)
    inv_file = next(DATA_DIR.glob("[Ii]nventory.csv"), None)
    if not prod_file or not inv_file:
        raise FileNotFoundError("products.csv or inventory.csv missing in data dir: %s" % DATA_DIR)

    products = pd.read_csv(prod_file)
    inventory = pd.read_csv(inv_file)

    # orders for this run
    orders = _read_orders(run_date)
    if not orders.empty:
        # only completed orders
        orders = orders[orders["order_status"].str.lower() == "completed"].copy()
        orders["order_total"] = (orders["qty"].astype(float) * orders["unit_price"].astype(float)).round(2)
        # enrich with product + inventory
        orders = orders.merge(products, on="product_id", how="left")
        orders = orders.merge(inventory[["product_id", "stock_on_hand"]], on="product_id", how="left")
    else:
        # keep empty DataFrame with expected columns
        orders = pd.DataFrame(columns=[
            "order_id","order_date","user_id","order_status","product_id","qty","unit_price",
            "order_total","product_name","category","stock_on_hand"
        ])

    # write per-run table to sqlite (idempotent replace)
    table_name = f"orders_{run_date.strftime('%Y%m%d')}"
    conn = sqlite3.connect(str(DB_FILE))
    try:
        products.to_sql("products", conn, if_exists="replace", index=False)
        inventory.to_sql("inventory", conn, if_exists="replace", index=False)
        orders.to_sql(table_name, conn, if_exists="replace", index=False)
    finally:
        conn.close()

    log.info("ETL complete for %s — wrote %d rows to %s", run_date, len(orders), table_name)

    return {
        "db": str(DB_FILE),
        "table": table_name,
        "run_date": run_date.strftime("%Y-%m-%d"),
        "count": int(len(orders)),
        "has_orders": bool(len(orders) > 0),
    }

def summary_task(**context) -> dict:
    """
    Read per-run table and write a small summary JSON:
      { "date": "...", "total_orders": N, "total_revenue": X, "top_category": "..." }
    Skips if no orders for that run (AirflowSkipException).
    """
    ti = context["ti"]
    meta = ti.xcom_pull(task_ids="etl_task")
    if not meta or not meta.get("has_orders"):
        log.info("No orders for %s — skipping summary", meta and meta.get("run_date"))
        raise AirflowSkipException("no orders")

    db = meta["db"]
    table = meta["table"]
    run_date = meta["run_date"]

    conn = sqlite3.connect(db)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    finally:
        conn.close()

    df["order_total"] = pd.to_numeric(df["order_total"], errors="coerce").fillna(0)
    total_orders = int(len(df))
    total_revenue = float(round(df["order_total"].sum(), 2))
    cat_sums = df.groupby("category")["order_total"].sum()
    top_cat = cat_sums.idxmax() if not cat_sums.empty else None

    summary = {
        "date": run_date,
        "total_orders": total_orders,
        "total_revenue": total_revenue,
        "top_category": top_cat,
        "category_breakdown": cat_sums.to_dict()
    }

    out = REPORTS_DIR / f"summary_{run_date}.json"
    out.write_text(json.dumps(summary, indent=2))
    log.info("Wrote summary: %s", out.name)
    return {"summary_file": str(out), "summary": summary}

def inventory_alerts_task(**context) -> dict:
    """
    Create inventory alerts CSV for items below threshold.
    We still run alerts even if no orders exist (optional) — here we skip alerts
    for days without orders to keep behaviour consistent with summary.
    """
    ti = context["ti"]
    meta = ti.xcom_pull(task_ids="etl_task")
    if not meta or not meta.get("has_orders"):
        log.info("No orders for %s — skipping inventory alerts", meta and meta.get("run_date"))
        raise AirflowSkipException("no orders")

    db = meta["db"]
    run_date = meta["run_date"]

    conn = sqlite3.connect(db)
    try:
        q = """
        SELECT i.product_id, p.product_name, p.category, i.warehouse_id, i.stock_on_hand,
               CASE
                 WHEN i.stock_on_hand <= 10 THEN 'Critical'
                 WHEN i.stock_on_hand <= 50 THEN 'Low'
                 WHEN i.stock_on_hand <= 100 THEN 'Warning'
                 ELSE 'Normal'
               END AS stock_status
        FROM inventory i
        JOIN products p ON i.product_id = p.product_id
        WHERE i.stock_on_hand <= 100
        ORDER BY i.stock_on_hand ASC
        """
        alerts = pd.read_sql_query(q, conn)
    finally:
        conn.close()

    out = REPORTS_DIR / f"inventory_alerts_{run_date}.csv"
    alerts.to_csv(out, index=False)
    log.info("Wrote alerts: %s (%d rows)", out.name, len(alerts))
    return {"alerts_file": str(out), "count": int(len(alerts))}

def notify_task(**context) -> None:
    """Log a short pipeline completion message (swap in email/webhook if needed)."""
    ti = context["ti"]
    try:
        summary_meta = ti.xcom_pull(task_ids="summary_task")
        alerts_meta = ti.xcom_pull(task_ids="inventory_alerts_task")
        summary = summary_meta.get("summary", {})
        log.info("PIPELINE DONE for %s — orders:%s revenue:%s top:%s alerts:%s",
                 summary.get("date"),
                 summary.get("total_orders"),
                 summary.get("total_revenue"),
                 summary.get("top_category"),
                 alerts_meta.get("count") if alerts_meta else 0)
    except Exception as e:
        log.exception("Notify task failed: %s", e)
        # don't fail the DAG on notify errors
        return

# ------------------------
# DAG definition
# ------------------------
with DAG(
    dag_id="quickshop_daily_pipeline",
    default_args=default_args,
    description="QuickShop daily pipeline (idempotent, backfillable)",
    start_date=datetime(2025, 10, 23),
    schedule="@daily",
    catchup=True,
    max_active_runs=1,
    tags=["quickshop", "etl"],
) as dag:

    t_etl = PythonOperator(task_id="etl_task", python_callable=etl_task)
    t_summary = PythonOperator(task_id="summary_task", python_callable=summary_task)
    t_alerts = PythonOperator(task_id="inventory_alerts_task", python_callable=inventory_alerts_task)
    t_notify = PythonOperator(task_id="notify_task", python_callable=notify_task)

    t_etl >> [t_summary, t_alerts] >> t_notify
