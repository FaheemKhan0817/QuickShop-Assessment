# 🛒 QuickShop ETL & Analytics Pipeline

[![CI Pipeline](https://github.com/FaheemKhan0817/QuickShop-Assessment/actions/workflows/ci.yml/badge.svg)](https://github.com/FaheemKhan0817/QuickShop-Assessment/actions/workflows/ci.yml)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

> A production-ready ETL pipeline for e-commerce order processing with SQL analytics and Airflow orchestration.

---

## 📖 Overview

This project implements a complete data engineering solution for QuickShop's order processing system, featuring automated ETL pipelines, analytical SQL queries, and orchestrated workflows.

| Component | Description |
|-----------|-------------|
| **🐍 Python ETL** | Reusable `quickshop_etl` package with CLI, schema validation, and multi-format output (Parquet/SQLite) |
| **📊 SQL Analytics** | 4 analytical queries covering revenue analysis, product performance, inventory alerts, and cohort retention |
| **🌬️ Airflow Orchestration** | Daily DAG with idempotent operations, JSON summaries, automatic retries, and comprehensive logging |
| **🐳 Docker Support** | Containerized deployment with CI/CD pipeline via GitHub Actions |
| **✅ Quality Assurance** | Full test coverage with pytest, linting with flake8, and automated code formatting |

---

## 📂 Project Structure

```
QUICKSHOP-ASSESSMENT/
│
├── .github/
│   └── workflows/
│       └── ci.yml                    # CI/CD pipeline configuration
│
├── dags/
│   └── quickshop_daily_pipeline.py   # Airflow DAG definition
│
├── data/                             # Input CSV files
│   ├── products.csv
│   ├── inventory.csv
│   └── orders_*.csv
│
├── output/                           # Generated outputs (Parquet/SQLite)
│
├── quickshop_etl/                    # Main ETL package
│   ├── __init__.py
│   ├── cli.py                        # Command-line interface
│   ├── config.py                     # Configuration dataclass
│   ├── etl.py                        # Core ETL logic
│   ├── exception.py                  # Custom exceptions
│   └── io.py                         # I/O operations
│
├── reports/                          # Generated analytics reports
│
├── sql/                              # SQL analytical queries
│   ├── daily_revenue.sql
│   ├── product_performance.sql
│   ├── inventory_alerts.sql
│   ├── cohort_retention.sql
│   └── output.md
│
├── tests/                            # Unit tests
│   └── test_etl.py
│
├── .flake8                           # Linter configuration
├── .gitignore                        # Git ignore rules
├── Dockerfile                        # Container definition
├── LICENSE                           # MIT License
├── README.md                         # This file
├── requirements.txt                  # Python dependencies
├── run_etl.py                        # ETL execution script
├── setup.py                          # Package installation config
└── write_products_inventory.py      # Data generation utility
```

---

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- MySQL Server (for SQL analytics)
- Docker (optional, for containerized deployment)
- Apache Airflow

---

## 🔧 Environment Setup

# If Python 3.11 not installed:
# Windows:  https://www.python.org/downloads/
# Ubuntu:   sudo apt install python3.11 python3.11-venv


### Step 1: Create Python Virtual Environment

#### **Windows**

```bash
# Create virtual environment with Python 3.11
python -m venv env

# Activate the environment
env\Scripts\activate

# Verify Python version
python --version  # Should show Python 3.11.x
```

#### **Linux / macOS**

```bash
# Create virtual environment with Python 3.11
python3.11 -m venv env

# Activate the environment
source env/bin/activate

# Verify Python version
python --version  # Should show Python 3.11.x
```

#### **Using conda (Alternative)**

```bash
# Create conda environment
conda create -n quickshop python=3.11

# Activate environment
conda activate quickshop

# Verify installation
python --version
```

---

### Step 2: Install Dependencies

```bash
# Upgrade pip to latest version
python -m pip install --upgrade pip

# Install all required packages
pip install -r requirements.txt

# Install the package in editable mode (for development)
pip install -e .

# Verify installation
pip list
```

**Dependencies installed:**
- `pandas` - Data manipulation and analysis
- `pyarrow` - Parquet file support
- `pytest` - Testing framework
- `flake8` - Code linting
- `apache-airflow` - Workflow orchestration (optional)

---

### Step 3: Verify Installation

```bash
# Check if CLI is installed correctly
quickshop-etl --help

# Alternative: Run directly
python run_etl.py --help
```

**Expected output:**
```
usage: quickshop-etl [-h] [-i INPUT_DIR] [-o OUTPUT_DIR] 
                     [-f {parquet,sqlite}] [--sqlite-name SQLITE_NAME]
                     [--start-date START_DATE] [--end-date END_DATE]
                     [--orders-pattern ORDERS_PATTERN] [-v]

QuickShop ETL — CSV -> Parquet / SQLite
```

---

## 🔄 Running the ETL Pipeline

### Basic Usage

```bash
# Run ETL with default settings (processes all orders)
python run_etl.py

# Or use the installed CLI
quickshop-etl
```

---

### Advanced Examples

#### **1️⃣ Process Single Day to Parquet**

```bash
python run_etl.py \
  --input-dir data \
  --output-dir output \
  --start-date 2025-10-23 \
  --end-date 2025-10-23 \
  --output-format parquet \
  --verbose
```

**Output:** `output/orders_2025-10-23.parquet`

---

#### **2️⃣ Process Date Range to SQLite**

```bash
python run_etl.py \
  --input-dir data \
  --output-dir output \
  --start-date 2025-10-23 \
  --end-date 2025-10-26 \
  --output-format sqlite \
  --sqlite-name quickshop.db \
  --verbose
```

**Output:** `output/quickshop.db` (contains `products`, `inventory`, `orders` tables)

---

#### **3️⃣ Custom Orders File Pattern**

```bash
python run_etl.py \
  --input-dir data \
  --output-dir output \
  --orders-pattern "orders_2025*.csv" \
  --output-format parquet
```

---

### CLI Options Reference

| Option | Short | Description | Default |
|--------|-------|-------------|---------|
| `--input-dir` | `-i` | Directory containing CSV files | `data` |
| `--output-dir` | `-o` | Directory for output files | `output` |
| `--output-format` | `-f` | Output format: `parquet` or `sqlite` | `parquet` |
| `--sqlite-name` | - | SQLite database filename | `quickshop_etl.db` |
| `--start-date` | - | Start date filter (YYYY-MM-DD) | None |
| `--end-date` | - | End date filter (YYYY-MM-DD) | None |
| `--orders-pattern` | - | Glob pattern for order files | `orders*.csv` |
| `--verbose` | `-v` | Enable debug logging | False |

---

## 📊 Task B – SQL Analytics (MySQL)
-- All queries were executed in MySQL Workbench, not CLI, for easier visualization.

### Setup MySQL Database

#### **1️⃣ Create Database**

```sql
-- Connect to MySQL
mysql -u root -p

-- Create database
CREATE DATABASE IF NOT EXISTS quickshop;
USE quickshop;
```

---

#### **2️⃣ Import Data via MySQL Workbench**

1. Open **MySQL Workbench**
2. Navigate to **Server → Data Import**
3. Select **"Import from Self-Contained File"**
4. Import each CSV file:
   - `products.csv` → `products` table
   - `inventory.csv` → `inventory` table
   - `orders_*.csv` → `orders_YYYYMMDD` tables
5. Click **Start Import**

---

#### **3️⃣ Run Analytical Queries**

Execute the following queries from the `sql/` directory:

**Daily Revenue Analysis**
```bash
mysql -u root -p quickshop < sql/daily_revenue.sql
```

**Product Performance Metrics**
```bash
mysql -u root -p quickshop < sql/product_performance.sql
```

**Low Stock Inventory Alerts**
```bash
mysql -u root -p quickshop < sql/inventory_alerts.sql
```

**Customer Cohort Retention**
```bash
mysql -u root -p quickshop < sql/cohort_retention.sql
```

---

### Query Descriptions

| Query | Purpose | Key Metrics |
|-------|---------|-------------|
| **Daily Revenue** | Track daily sales performance | Total revenue, order count, average order value |
| **Product Performance** | Identify top-selling products | Units sold, revenue by product, category analysis |
| **Inventory Alerts** | Monitor stock levels | Products below restock threshold, warehouse status |
| **Cohort Retention** | Analyze customer retention | First purchase cohorts, repeat purchase rates |

---

## 🌬️ Task C – Airflow Orchestration

### Installation (WSL2 / Linux)

#### **1️⃣ Install Airflow**

```bash
# Update system packages
sudo apt update && sudo apt install -y python3-pip

# Install Airflow (make sure env is activated)
pip install apache-airflow

# Verify Airflow installation
airflow version

# Install project requirements
pip install -r requirements.txt
```

---

#### **2️⃣ Initialize Airflow**

```bash
# Initialize and start Airflow in standalone mode
airflow standalone
```

Then access at **http://localhost:8080** and log in with printed credentials.

---

#### **3️⃣ Deploy DAG**

Copy your DAG file to:
```bash
~/airflow/dags/quickshop_daily_pipeline.py
```

Restart Airflow and trigger DAG in UI.

---

#### **4️⃣ Alternative: Separate Components (Production)**

```bash
# Terminal 1: Start webserver
airflow webserver --port 8080

# Terminal 2: Start scheduler
airflow scheduler
```

**Access Web UI:** [http://localhost:8080](http://localhost:8080)

---

#### **5️⃣ Verify DAG**

```bash
# Copy DAG to Airflow directory
cp dags/quickshop_daily_pipeline.py ~/airflow/dags/

# Verify DAG is detected
airflow dags list | grep QuickShop

# Test DAG
airflow dags test QuickShop_ETL_DAG 2025-10-23
```

---

### DAG Features

- ✅ **Daily Schedule:** Runs automatically at midnight
- ✅ **Idempotent:** Safe to re-run without side effects
- ✅ **Retry Logic:** Automatically retries failed tasks (3 attempts)
- ✅ **Logging:** Comprehensive logs for debugging
- ✅ **Notifications:** JSON summary of pipeline execution
- ✅ **Data Validation:** Schema checks before processing

---

## 🧪 Testing & Code Quality

### Run Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run with coverage report
pytest --cov=quickshop_etl --cov-report=html

# Run specific test file
pytest tests/test_etl.py
```

---

### Code Quality Checks

```bash
# Linting with flake8
flake8 . --exclude env,venv,.venv

# Auto-format code (optional)
black .

# Sort imports (optional)
isort .

# Check with ruff (optional)
ruff check --fix .
```

---

## 🐳 Docker Deployment

### Build Image

```bash
# Build Docker image
docker build -t quickshop-etl:latest .

# Verify image
docker images | grep quickshop
```

---

### Run Container

#### **Windows (PowerShell/CMD)**

```bash
docker run --rm -v "%cd%/data:/app/data" -v "%cd%/output:/app/output" quickshop_etl:latest --input-dir data --output-dir output --start-date 2025-10-23 --end-date 2025-10-26
```

#### **Linux / macOS**

```bash
docker run --rm \
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/output:/app/output" \
  quickshop-etl:latest \
  --input-dir data \
  --output-dir output \
  --start-date 2025-10-23 \
  --end-date 2025-10-26 \
  --output-format parquet
```

---

### Push to Docker Hub

```bash
# Login to Docker Hub
docker login -u faheemkhan08

# Tag image
docker tag quickshop-etl:latest faheemkhan08/quickshop-etl:latest

# Push to registry
docker push faheemkhan08/quickshop-etl:latest
```

**Pull the image:**
```bash
docker pull faheemkhan08/quickshop-etl:latest
```

---

## 🔄 CI/CD Pipeline

### GitHub Actions Workflow

The project includes automated CI/CD via GitHub Actions (`.github/workflows/ci.yml`):

#### **Pipeline Stages:**

1. **Test & Lint**
   - Runs pytest for unit tests
   - Executes flake8 for code quality
   - Validates Python 3.10+ compatibility

2. **Docker Build & Push**
   - Builds Docker image
   - Pushes to Docker Hub (on push to main/master)
   - Tags with `latest`

---

### Required Secrets

Configure these in **GitHub Repository → Settings → Secrets**:

| Secret Name | Description |
|-------------|-------------|
| `DOCKERHUB_USERNAME` | Your Docker Hub username |
| `DOCKERHUB_TOKEN` | Docker Hub access token |

---

### Workflow Triggers

- ✅ Push to `main` or `master` branch
- ✅ Pull requests to `main` or `master`

---

## 📝 ETL Pipeline Features

### ✨ Key Capabilities

| Feature | Description |
|---------|-------------|
| **Schema Validation** | Automatic detection of missing/invalid columns with detailed error messages |
| **Type Coercion** | Robust conversion to `Int64`, `datetime64`, `float64` |
| **Date Filtering** | Filter by filename pattern and/or order_date column |
| **Multi-Format Output** | Generate Parquet files or SQLite databases |
| **Idempotent Operations** | Safe to re-run without duplicating data |
| **Error Handling** | Graceful failures with informative logging |
| **Performance** | Optimized for large datasets with pandas operations |

---

### 🔍 Data Flow

```
┌─────────────┐
│  CSV Files  │
│  (Input)    │
└──────┬──────┘
       │
       ▼
┌─────────────────┐
│ Schema          │
│ Validation      │
└──────┬──────────┘
       │
       ▼
┌─────────────────┐
│ Type Coercion   │
│ & Filtering     │
└──────┬──────────┘
       │
       ▼
┌─────────────────┐
│ Enrichment      │
│ (Joins)         │
└──────┬──────────┘
       │
       ▼
┌─────────────────┐
│ Output          │
│ (Parquet/SQLite)│
└─────────────────┘
```

---

## 🐛 Troubleshooting

### Common Issues

#### **1. Import Error: Module not found**

```bash
# Reinstall in editable mode
pip install -e .
```

---

#### **2. Permission Denied (Linux/macOS)**

```bash
# Make script executable
chmod +x run_etl.py
```

---

#### **3. Airflow DAG not appearing**

```bash
# Check DAG file for syntax errors
python ~/airflow/dags/quickshop_daily_pipeline.py

# Refresh DAGs in web UI
airflow dags list-import-errors
```

---

#### **4. Docker volume mount issues (Windows)**

```powershell
# Use absolute paths
docker run --rm `
  -v "C:\path\to\project\data:/app/data" `
  -v "C:\path\to\project\output:/app/output" `
  quickshop-etl:latest
```

---

## 📚 Additional Resources

- **Pandas Documentation:** https://pandas.pydata.org/docs/
- **Apache Airflow:** https://airflow.apache.org/docs/
- **Docker Guide:** https://docs.docker.com/get-started/
- **MySQL Reference:** https://dev.mysql.com/doc/

---

## 📄 License

This project is licensed under the Apache License 2 - see the [LICENSE](LICENSE) file for details.

---

## 👨‍💻 Author

**Faheem Khan**  
*Data Scientist | ML & Data Engineer*

📍 Aligarh, Uttar Pradesh, India  
🔗 [LinkedIn](www.linkedin.com/in/faheemkhanml)  
🐙 [GitHub](https://github.com/FaheemKhan0817)  
✉️ faheemthakur23@gmail.com

---

## 🙏 Acknowledgments

- Built as part of QuickShop Data Engineering Assessment
- Uses modern Python data engineering best practices
- Inspired by production ETL pipelines at scale

---

## 🌟 Project Status

✅ **Production Ready**  
- All features implemented and tested
- CI/CD pipeline operational
- Docker deployment validated
- Documentation complete

---

<div align="center">

### ✨ "Data pipelines should be reproducible, observable, and elegant." ✨

**⭐ Star this repo if you found it helpful!**

</div>
