"""
S&P 500 Market Analytics Pipeline
-----------------------------------
This script runs the full data pipeline:
1. Fetches S&P 500 market data from Alpha Vantage API
2. Fetches economic indicator data from FRED API
3. Transforms and cleans both datasets
4. Loads data into MySQL database
5. Runs trend analysis (moving averages, volatility)
6. Runs correlation analysis (S&P 500 vs economic indicators)
"""

from pipeline.fetch_market import fetch_sp500_data
from pipeline.fetch_economic import fetch_economic_data
from pipeline.transform import transform_market_data, transform_economic_data
from pipeline.load import load_market_data, load_economic_data
from analysis.trends import calculate_moving_averages, calculate_volatility, save_analysis_results
from analysis.correlations import calculate_correlations

def run_pipeline():
    print("=" * 50)
    print("S&P 500 ANALYTICS PIPELINE STARTING")
    print("=" * 50)

    # Step 1: Fetch data from APIs
    print("\n[1/4] Fetching data from APIs...")
    market_df = fetch_sp500_data()
    economic_df = fetch_economic_data()

    # Step 2: Transform data
    print("\n[2/4] Transforming data...")
    market_df = transform_market_data(market_df)
    economic_df = transform_economic_data(economic_df)

    # Step 3: Load data into MySQL
    print("\n[3/4] Loading data into MySQL...")
    load_market_data(market_df)
    load_economic_data(economic_df)

    # Step 4: Run analysis
    print("\n[4/4] Running analysis...")
    market_df = calculate_moving_averages(market_df)
    market_df = calculate_volatility(market_df)
    save_analysis_results(market_df)
    calculate_correlations()

    print("\n" + "=" * 50)
    print("PIPELINE COMPLETE")
    print("=" * 50)

if __name__ == "__main__":
    run_pipeline()