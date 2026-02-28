import pandas as pd

def transform_market_data(df):
    """
    Cleans and transforms the raw S&P 500 market data.
    - Converts date column to proper datetime format
    - Sorts by date ascending
    - Removes any duplicate dates
    - Resets the index
    """
    
    # Convert trade_date string to a proper datetime object
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    
    # Sort by date ascending (oldest to newest)
    df = df.sort_values("trade_date").reset_index(drop=True)
    
    # Remove any duplicate dates keeping the first occurrence
    df = df.drop_duplicates(subset=["trade_date"])
    
    print(f"Market data transformed: {len(df)} records ready to load")
    return df


def transform_economic_data(df):
    """
    Cleans and transforms the raw FRED economic indicator data.
    - Converts date column to proper datetime format
    - Sorts by indicator name and date ascending
    - Removes any duplicate entries
    - Resets the index
    """
    
    # Convert indicator_date string to a proper datetime object
    df["indicator_date"] = pd.to_datetime(df["indicator_date"])
    
    # Sort by indicator name and date ascending
    df = df.sort_values(["indicator_name", "indicator_date"]).reset_index(drop=True)
    
    # Remove any duplicate entries
    df = df.drop_duplicates(subset=["indicator_name", "indicator_date"])
    
    print(f"Economic data transformed: {len(df)} records ready to load")
    return df


if __name__ == "__main__":
    # Test the transform functions with live data
    from pipeline.fetch_market import fetch_sp500_data
    from pipeline.fetch_economic import fetch_economic_data
    
    print("Testing market data transform...")
    market_df = fetch_sp500_data()
    market_df = transform_market_data(market_df)
    print(market_df.head())
    
    print("\nTesting economic data transform...")
    economic_df = fetch_economic_data()
    economic_df = transform_economic_data(economic_df)
    print(economic_df.head())