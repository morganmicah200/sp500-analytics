import os
from dotenv import load_dotenv

load_dotenv()

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
FRED_API_KEY = os.getenv("FRED_API_KEY")

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "your_msql_password_here",
    "database": "sp500_analytics"
}