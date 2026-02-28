import pandas as pd
from sqlalchemy import create_engine
from config import DB_CONFIG

def get_engine():
    """
    Creates and returns a SQLAlchemy engine for connecting to MySQL.
    Pandas read_sql requires SQLAlchemy instead of raw mysql.connector.
    """
    return create_engine(
        f"mysql+mysqlconnector://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
    )

def load_market_from_db():
    """
    Loads S&P 500 market price data from MySQL database
    and returns it as a pandas DataFrame.
    """
    query = "SELECT trade_date, close_price FROM market_prices ORDER BY trade_date ASC"
    df = pd.read_sql(query, get_engine())
    print(f"Loaded {len(df)} market records from database")
    return df

def load_economic_from_db():
    """
    Loads economic indicator data from MySQL database
    and returns it as a pandas DataFrame.
    """
    query = "SELECT indicator_name, indicator_date, value FROM economic_indicators ORDER BY indicator_date ASC"
    df = pd.read_sql(query, get_engine())
    print(f"Loaded {len(df)} economic records from database")
    return df

def calculate_correlations():
    """
    Calculates the correlation between S&P 500 closing prices
    and each economic indicator (Federal Funds Rate, CPI, Unemployment).
    Correlation ranges from -1 to 1:
    - Close to 1: they move together
    - Close to -1: they move opposite
    - Close to 0: no relationship
    """
    
    # Load both datasets from MySQL
    market_df = load_market_from_db()
    economic_df = load_economic_from_db()
    
    # Convert dates to datetime
    market_df["trade_date"] = pd.to_datetime(market_df["trade_date"])
    economic_df["indicator_date"] = pd.to_datetime(economic_df["indicator_date"])
    
    # Extract year and month for merging since economic data is monthly
    market_df["year_month"] = market_df["trade_date"].dt.to_period("M")
    economic_df["year_month"] = economic_df["indicator_date"].dt.to_period("M")
    
    # Calculate average monthly S&P 500 price
    monthly_market = market_df.groupby("year_month")["close_price"].mean().reset_index()
    
    results = {}
    
    # Calculate correlation for each economic indicator
    for indicator in economic_df["indicator_name"].unique():
        indicator_data = economic_df[economic_df["indicator_name"] == indicator][["year_month", "value"]]
        
        # Merge market and economic data on year_month
        merged = pd.merge(monthly_market, indicator_data, on="year_month")
        
        if len(merged) > 1:
            correlation = merged["close_price"].corr(merged["value"])
            results[indicator] = round(correlation, 4)
            print(f"Correlation between S&P 500 and {indicator}: {correlation:.4f}")
    
    return results

if __name__ == "__main__":
    print("Calculating correlations between S&P 500 and economic indicators...\n")
    results = calculate_correlations()
    
    print("\nSummary:")
    for indicator, correlation in results.items():
        if correlation > 0.5:
            relationship = "Strong positive relationship"
        elif correlation < -0.5:
            relationship = "Strong negative relationship"
        else:
            relationship = "Weak relationship"
        print(f"  {indicator}: {correlation} — {relationship}")