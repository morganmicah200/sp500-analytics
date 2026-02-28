import mysql.connector
from config import DB_CONFIG

def load_market_data(df):
    """
    Loads transformed S&P 500 market data into the market_prices
    table in MySQL. Skips duplicate dates to avoid inserting
    the same data twice.
    """
    
    # Connect to MySQL database using credentials from config
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # SQL insert statement - IGNORE skips duplicates
    insert_query = """
        INSERT IGNORE INTO market_prices 
        (trade_date, open_price, high_price, low_price, close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    # Loop through each row in the DataFrame and insert it
    records_inserted = 0
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row["trade_date"],
            row["open_price"],
            row["high_price"],
            row["low_price"],
            row["close_price"],
            row["volume"]
        ))
        records_inserted += 1
    
    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Successfully loaded {records_inserted} market records into MySQL")


def load_economic_data(df):
    """
    Loads transformed FRED economic indicator data into the
    economic_indicators table in MySQL. Skips duplicates.
    """
    
    # Connect to MySQL database using credentials from config
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    # SQL insert statement - IGNORE skips duplicates
    insert_query = """
        INSERT IGNORE INTO economic_indicators
        (indicator_name, indicator_date, value)
        VALUES (%s, %s, %s)
    """
    
    # Loop through each row in the DataFrame and insert it
    records_inserted = 0
    for _, row in df.iterrows():
        cursor.execute(insert_query, (
            row["indicator_name"],
            row["indicator_date"],
            row["value"]
        ))
        records_inserted += 1
    
    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"Successfully loaded {records_inserted} economic records into MySQL")


if __name__ == "__main__":
    # Test loading both datasets into MySQL
    from pipeline.fetch_market import fetch_sp500_data
    from pipeline.fetch_economic import fetch_economic_data
    from pipeline.transform import transform_market_data, transform_economic_data
    
    print("Loading market data...")
    market_df = fetch_sp500_data()
    market_df = transform_market_data(market_df)
    load_market_data(market_df)
    
    print("\nLoading economic data...")
    economic_df = fetch_economic_data()
    economic_df = transform_economic_data(economic_df)
    load_economic_data(economic_df)