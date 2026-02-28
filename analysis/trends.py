import pandas as pd
from pipeline.fetch_market import fetch_sp500_data
from pipeline.transform import transform_market_data
from pipeline.load import load_market_data
import mysql.connector
from config import DB_CONFIG

def calculate_moving_averages(df):
    """
    Calculates 50-day and 200-day moving averages on the S&P 500
    closing price. Moving averages smooth out price data to identify
    trends over time.
    """
    
    # Calculate 50-day moving average
    df["ma_50"] = df["close_price"].rolling(window=50).mean()
    
    # Calculate 200-day moving average
    df["ma_200"] = df["close_price"].rolling(window=200).mean()
    
    print("Moving averages calculated successfully")
    return df

def calculate_volatility(df):
    """
    Calculates 30-day rolling volatility using standard deviation
    of daily returns. Higher volatility means bigger price swings.
    """
    
    # Calculate daily percentage return
    df["daily_return"] = df["close_price"].pct_change()
    
    # Calculate 30-day rolling standard deviation of returns
    df["volatility_30d"] = df["daily_return"].rolling(window=30).std()
    
    print("Volatility calculated successfully")
    return df

def save_analysis_results(df):
    """
    Saves the most recent moving average and volatility metrics
    into the analysis_results table in MySQL.
    """
    
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    insert_query = """
        INSERT IGNORE INTO analysis_results (analysis_date, metric_name, metric_value)
        VALUES (%s, %s, %s)
    """
    
    # Get the most recent row that has all calculated values
    latest = df.dropna(subset=["ma_50", "volatility_30d"]).iloc[-1]
    
    metrics = [
        (str(latest["trade_date"].date()), "MA_50", round(float(latest["ma_50"]), 4)),
        (str(latest["trade_date"].date()), "Volatility_30d", round(float(latest["volatility_30d"]), 4)),
    ]
    
    for metric in metrics:
        cursor.execute(insert_query, metric)
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Analysis results saved to MySQL")

if __name__ == "__main__":
    # Fetch, transform and analyze market data
    df = fetch_sp500_data()
    df = transform_market_data(df)
    df = calculate_moving_averages(df)
    df = calculate_volatility(df)
    
    print("\nLatest Analysis Results:")
    print(df[["trade_date", "close_price", "ma_50", "volatility_30d"]].tail(5))
    
    save_analysis_results(df)