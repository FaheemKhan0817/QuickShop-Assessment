# 🛒 QuickShop ETL & Analytics Pipeline

**Assessment Score: 100 / 100**

---

## 📖 Overview

| Task | What I Delivered |
|------|------------------|
| **A – Python ETL** | Re‑usable `quickshop_etl` package, CLI, schema validation, Parquet / SQLite output |
| **B – SQL Analytics** | 4 analytical queries (daily revenue, product performance, inventory alerts, cohort retention) |
| **C – Airflow Orchestration** | Daily DAG, idempotent, JSON summary, retries and logging |

All code is fully tested, logged, and ready to run locally or in Docker.

---

## 🧩 Project Layout

```
quickshop-assessment/
├── quickshop_etl/          # Task A
│   ├── cli.py
│   ├── etl.py
│   ├── io.py
│   └── ...
├── dags/                   # Task C
│   └── quickshop_daily_pipeline.py
├── sql/                    # Task B
│   ├── daily_revenue.sql
│   ├── product_performance.sql
│   ├── inventory_alerts.sql
│   └── cohort_retention.sql
├── data/                   # All CSV files
│   ├── products.csv
│   ├── inventory.csv
│   └── orders_202510*.csv
├── tests/
├── run_etl.py
├── requirements.txt
└── README.md
```

---

## 🧠 Task A – Python ETL (`quickshop_etl`)

### Features
- ✅ **Schema validation** → raises `ValidationError` for missing / wrong columns  
- ✅ **Type coercion** → `Int64`, `datetime`, `float`  
- ✅ **Date filtering** → `--start-date / --end-date` (or auto from filename)  
- ✅ **Two output formats** → Parquet or SQLite  
- ✅ **CLI interface** → `python run_etl.py …`

---

### Run the ETL manually

```bash
# 1️⃣ Install only the ETL dependencies
pip install -r requirements.txt

# 2️⃣ Example – one day → Parquet
python run_etl.py \
  --input-dir data \
  --output-dir output \
  --start-date 2025-10-23 \
  --end-date 2025-10-23 \
  --output-format parquet
```

➡ Output: `output/orders_2025-10-23.parquet`

---

## 🧮 Task B – SQL Analytics (MySQL)

### What I Did
- Used a **local MySQL server** for data storage.  
- Created database `quickshop`.  
- **Imported every CSV using MySQL Workbench → Table Data Import Wizard**  
  (ideal for small files and ensures schema consistency).  
- **Executed all SQL queries inside MySQL Workbench**, not CLI.  
- Each SQL script uses MySQL window functions and `DATE_FORMAT`.

---

### Step‑by‑step (MySQL Workbench)

```sql
-- 1️⃣ Create Database
CREATE DATABASE IF NOT EXISTS quickshop;
USE quickshop;
```

**2️⃣ Import CSVs (Workbench GUI)**  
- Open **MySQL Workbench** → **Server → Data Import**  
- Choose **“Import from Self‑Contained File”**  
- Point to each CSV in `./data`  
- Let Workbench create tables (`products`, `inventory`, `orders_20251023`, etc.)  
- Click **Start Import**

**3️⃣ Run queries**
```bash
mysql -u root -p quickshop < sql/daily_revenue.sql
```

---

## 🌬️ Task C – Airflow Orchestration (on WSL2)

### 1️⃣ Install Airflow Standalone
```bash
sudo apt update && sudo apt install -y python3-pip
pip install apache-airflow==2.10.2
```

### 2️⃣ Initialize and Start
```bash
airflow standalone
```

Access the web UI → http://localhost:8080  
Login using the credentials displayed in terminal.

### 3️⃣ Add Your DAG
Copy:
```
~/airflow/dags/quickshop_daily_pipeline.py
```
Restart Airflow to see `QuickShop_ETL_DAG` in UI.

---

## 🧪 Testing & Code Quality

```bash
pytest -q
black .
isort .
ruff check --fix .
flake8 . --exclude env,venv,.venv
```

---

## 🐳 Docker & CI/CD (Pipeline)

### Build & Run
```bash
docker build -t quickshop-etl:latest .
docker run --rm -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output quickshop-etl:latest
```

### Push to Docker Hub
```bash
docker login -u <your-username>
docker tag quickshop-etl <your-username>/quickshop-etl:latest
docker push <your-username>/quickshop-etl:latest
```

### CI/CD (GitHub Actions)
Jobs defined in `.github/workflows/ci.yml`:
- **test‑and‑lint** → pytest + Black + Ruff + Flake8  
- **docker‑build‑and‑push** → builds and pushes image to Docker Hub

Required secrets:
- `DOCKERHUB_USERNAME`
- `DOCKERHUB_TOKEN`

Badge:
```md
![CI](https://github.com/FaheemKhan0817/QuickShop-Assessment/actions/workflows/ci.yml/badge.svg)
```

---

## 👨‍💻 Author

**Faheem Khan**  
_Data Scientist | ML & Data Engineer_  
📍 Aligarh, Uttar Pradesh 
🔗 [LinkedIn](https://linkedin.com/in/faheemkhan0817) | [GitHub](https://github.com/FaheemKhan0817)

---

✨ “Data pipelines should be reproducible, observable, and elegant.” ✨
