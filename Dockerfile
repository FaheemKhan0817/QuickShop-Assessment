# -------------------------------
# Simple, reliable Dockerfile for QuickShop ETL also I Can add Airflow part but will increase complexity and take more time and spcae.
# -------------------------------

FROM python:3.10-slim

# Set environment variables for clean logs and no cache
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Set working directory inside the container
WORKDIR /app

# Copy project files into the container
COPY . /app

# Upgrade pip and install dependencies
RUN pip install --upgrade pip setuptools wheel \
 && pip install -r requirements.txt \
 && pip install pytest  # ensure tests can run if needed

# Default command: show CLI help
ENTRYPOINT ["python", "run_etl.py"]
CMD ["--help"]
