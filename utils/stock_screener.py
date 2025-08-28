from vnstock import *
from crawler.stock_OHCLV_crawler import load_data_direct
import pandas as pd
import os

def get_stock_metadata(exchange: str = "HOSE", limit: int = 1700) -> list:
    """
    This function retrieves stock data from the VNStock API.
    It uses the Screener class to fetch stocks listed on the HOSE exchange.
    The limit is set to 1700 to retrieve a comprehensive list of stocks.
    """

    csv_path = "data/{}.csv".format(exchange)
    if os.path.exists(csv_path):
        screener_df = pd.read_csv(csv_path, index_col=0)
    else:
        screener_df = Screener().stock(params={"exchangeName": exchange}, limit=limit)
        screener_df.to_csv(csv_path, index=True)
    
    stock_df = screener_df[["ticker", "market_cap"]]
    stock_df = stock_df.dropna()
    stock_df = stock_df.sort_values("market_cap", ascending=False)

    stock_df.to_csv("data/stock_metadata.csv", index=True)

    return stock_df 

def filter_stocks(stock_df):
    # Filter stocks based on custom criteria (e.g., liquidity, data availability)
    
    stocks = stock_df[:120]["ticker"]

    stock_1w_data = {}
    dfs = []

    for stock in stocks:
        print(stock)
        df = load_data_direct(stock, exchange = "HOSE", interval = "1W")    
        dfs.append(df)
        
    stock_list = []
    benchmark_df = dfs[0][-300:]
    for df in dfs:
        if len(df) >= 300:
            if df[-300:]["volume"].mean() > 100000:
                insight_df = df[-300:]
                if insight_df.index[0] == benchmark_df.index[0]:
                    ticker = df.iloc[0]["ticker"]
                    stock_list.append(ticker)
                    stock_1w_data[ticker] = insight_df

    price_data = {}

    for stock in stock_list:
        price_data[stock] = stock_1w_data[stock]["close"]    
        
    df = pd.DataFrame(price_data)
    df.index = pd.to_datetime(df.index)
    df_price = df.copy()

    return stock_list, df_price
