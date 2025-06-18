from vnstock import *
import numpy as np
import pandas as pd
from datetime import datetime
from utils.date_util import get_most_recent_trade_day

#Crawl the stock list satisfying: marketCap, avgVol and startDate
def stock_info_crawl(marketCapThreshold = 1000, start_data_date = "2022-01-04", avgVolThreshold = 5, avgVol3mThreshold = 3):
    stock_infos = listing_companies().set_index("ticker")
    stock_infos["avgVolVal"] = 0
    params = {
            "exchangeName": "HOSE,HNX,UPCOM",
            "marketCap": (marketCapThreshold, 100000000)
    }

    # Áp dụng bộ lọc với hàm để lấy kết quả

    filtered_stock_df = stock_screening_insights(params, size=1700, drop_lang='vi').set_index("ticker")
    filtered_stocks = np.intersect1d(filtered_stock_df.index.values, stock_infos.index.values)

    #Only keep the stocks published earlier from 2022
    print("Checking if the stock is published for enough time")
    now = datetime.now() 
    end_data_date = get_most_recent_trade_day(now)[0].strftime("%Y-%m-%d")
    
    stocks = []
    for stock in filtered_stocks:    
        print(stock)
        df = stock_historical_data(stock, start_data_date, end_data_date, "1D")
        
        if (df.iloc[0]["time"].strftime("%Y-%m-%d") != start_data_date) or (df.iloc[-1]["time"].strftime("%Y-%m-%d") != end_data_date):
            print("Remove {} because of missing data".format(stock))
            continue

        avg_vol_val = (df["close"] * df["volume"]).mean() / 10 ** 9                
        print("Average volume since 2022: {:.2f} billion".format(avg_vol_val))
        stock_infos.loc[stock, "avgVolValsince2022"] = avg_vol_val

        avg_vol_val_3m = (df["close"][-60:] * df["volume"][-60:]).mean() / 10 ** 9                
        print("Average volume last 3 month: {:.2f} billion".format(avg_vol_val_3m))
        stock_infos.loc[stock, "avgVolVal3m"] = avg_vol_val_3m

        if avg_vol_val < avgVolThreshold or avg_vol_val_3m < avgVol3mThreshold:
            print("Remove {} because of lacking vol".format(stock))
            continue

        stocks.append(stock)
           
            
    filtered_stocks = stocks

    filtered_stock_df = filtered_stock_df.loc[filtered_stocks]
    stock_infos = stock_infos.loc[filtered_stocks]

    stock_info_df = pd.merge(filtered_stock_df, stock_infos, on = "ticker")
    stock_info_df["companyName"] = stock_info_df["organName"]

    print("Stock list: {} stocks".format(len(filtered_stocks)))
    print(filtered_stocks)

    np.save("data/list_stocks.npy", filtered_stocks)
    stock_info_df.to_csv("data/stock_infos.csv")
    print("Data saved to 'data' folder")

if __name__ == '__main__':
    stock_info_crawl()