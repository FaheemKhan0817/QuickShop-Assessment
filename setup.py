from setuptools import setup, find_packages

# Read requirements from requirements.txt
try:
    with open("requirements.txt", "r") as f:
        requirements = f.read().splitlines()
except FileNotFoundError:
    requirements = []  

setup(
    name="quickshop_etl",
    version="0.1.0",
    author="Faheem Khan",
    author_email="faheemthakur23@gmail.com",
    description="QuickShop ETL and Airflow assessment project",
    packages=find_packages(),
    install_requires=requirements
)
