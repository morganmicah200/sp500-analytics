# S&P 500 Market Analytics Pipeline

I built this project to strengthen my skills in data engineering and explore how Federal Reserve economic decisions actually affect stock market performance. The idea was simple — pull real financial data, store it properly, and see what the numbers say. The results were pretty eye opening.

## What It Does
This is a full ETL pipeline that pulls real S&P 500 market data and Federal Reserve economic indicators from REST APIs, loads them into a MySQL database, and runs analysis to find trends and correlations between market performance and economic conditions. The whole thing runs with a single command.

## Tech Stack
- **Python** — core pipeline logic, data transformation, and analysis
- **MySQL** — storing and querying market and economic data
- **REST APIs** — Alpha Vantage for S&P 500 data, FRED for Federal Reserve indicators
- **Pandas** — data cleaning and manipulation
- **SQLAlchemy** — database connectivity for analytical queries
- **Git** — version control throughout the whole build

## Project Structure
```
sp500-analytics/
│
├── pipeline/
│   ├── fetch_market.py      # Pulls S&P 500 data from Alpha Vantage API
│   ├── fetch_economic.py    # Pulls economic indicators from FRED API
│   ├── transform.py         # Cleans and preps data before loading
│   └── load.py              # Inserts data into MySQL
│
├── analysis/
│   ├── trends.py            # Moving averages and volatility
│   └── correlations.py      # S&P 500 vs economic indicators
│
├── db/
│   └── schema.sql           # MySQL database schema
│
├── main.py                  # Runs the full pipeline end to end
├── config.py                # Loads environment variables
└── requirements.txt         # Python dependencies
```

## How It Works
The pipeline runs in four steps:

1. **Extract** — Fetches 100 days of S&P 500 price data from Alpha Vantage and 100 observations each of the Federal Funds Rate, Consumer Price Index, and Unemployment Rate from FRED
2. **Transform** — Cleans and formats both datasets with Pandas, sorts by date, removes duplicates
3. **Load** — Inserts everything into MySQL with duplicate protection so you can run it repeatedly without issues
4. **Analyze** — Calculates 50-day moving averages, 30-day rolling volatility, and correlations between the S&P 500 and each economic indicator

## What I Found
Running the correlation analysis against real data produced some pretty striking results:

- **S&P 500 vs CPI: +0.9998** — Almost perfect positive correlation. As inflation rises, stock prices follow.
- **S&P 500 vs Federal Funds Rate: -0.9073** — When the Fed raises rates, the market drops. You hear about this all the time in financial news and it's cool to actually see it in the data.
- **S&P 500 vs Unemployment: -0.9907** — The market really doesn't like high unemployment. Strong negative correlation across the board.

## What I Learned
- How to design and build a multi-source ETL pipeline from scratch
- Working with financial REST APIs and handling real world data inconsistencies
- MySQL schema design and best practices for loading time-series data
- The importance of environment variables and keeping credentials out of version control (learned this one the hard way)
- How pandas, SQLAlchemy, and MySQL connector work together

## Getting Started

### Prerequisites
- Python 3.8+
- MySQL 8.0
- Free API key from Alpha Vantage (alphavantage.co)
- Free API key from FRED (fred.stlouisfed.org)

### Installation
1. Clone the repo
```bash
git clone https://github.com/morganmicah200/sp500-analytics.git
cd sp500-analytics
```

2. Set up your virtual environment
```bash
python -m venv venv
venv\Scripts\activate
```

3. Install dependencies
```bash
pip install -r requirements.txt
```

4. Create a `.env` file in the root folder with your credentials
```
ALPHA_VANTAGE_API_KEY=your_key_here
FRED_API_KEY=your_key_here
DB_HOST=localhost
DB_USER=root
DB_PASSWORD=your_password_here
DB_NAME=sp500_analytics
```

5. Create the database and tables
```bash
mysql -u root -p < db/schema.sql
```

6. Run it
```bash
python main.py
```

## Author
Micah Morgan — [GitHub](https://github.com/morganmicah200)