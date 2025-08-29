from frontend.telegram import send_notification
from utils.stock_screener import get_stock_metadata, filter_stocks
from utils.stock_exchange import is_trading_hour
import os, time
import pandas as pd
from strategies.TSMOM.strategy import calculate_momentum
import numpy as np
from crawler.stock_OHCLV_crawler import crawl_OHCLV

def main():
    os.environ['TZ'] = 'Asia/Ho_Chi_Minh'
    time.tzset()    

    current_day = -1
    current_weekday = -1

    print("Waiting for the start of the day to retrieve stock metadata...")
    while True:
        now = time.localtime()
        if now.tm_mday != current_day:
            print(f"New day detected: {current_day}. Resetting stock metadata retrieval.")
            stock_df = get_stock_metadata()
            print("Stock metadata retrieval completed.")
            current_day = now.tm_mday            
        
        # Detect start of a new week (Monday)
        if current_weekday == -1 or (now.tm_wday == 0 and now.tm_wday != current_weekday):
            print("New week detected. Performing weekly task...")
            stock_df = pd.read_csv("data/stock_metadata.csv", index_col=0)
            stock_list, stock_1w_data = filter_stocks(stock_df)
            print(f"Filtered stocks: {stock_list}")
            if current_weekday == -1:
                stock_momentum_list = calculate_momentum(stock_1w_data, in_week=True)[:8]
            else:
                stock_momentum_list = calculate_momentum(stock_1w_data, in_week=False)[:8]

            np.savetxt("data/momentum_list.txt", np.array(stock_momentum_list), fmt="%s")             

            send_notification(f"Weekly momentum list updated: {', '.join(stock_momentum_list)}")
            current_weekday = now.tm_wday                                    
        else:
            res = is_trading_hour(now)      
            if res:  
                stock_momentum_list = np.loadtxt("data/momentum_list.txt", dtype=str)
                msg = f"[{time.strftime('%Y-%m-%d %H:%M:%S', now)}]\n Current weekly momentum list: {', '.join(stock_momentum_list)}"
                change = 0

                for stock in stock_momentum_list:
                    df = crawl_OHCLV(stock, interval="1W", exchange="HOSE", nbars=3)
                    msg += f"\n{stock}: {int(df['close'].values[-1])} ({df['close'].pct_change().values[-1]*100:.2f}%)"
                    change += df['close'].pct_change().values[-1]*100

                msg += f"\nPortfolio: {change/len(stock_momentum_list):.2f}%"

                next_stock_momentum_list = calculate_momentum(stock_1w_data, in_week=False)[:8]
                msg += f"\nExpected next weekly stock momentum list: {', '.join(next_stock_momentum_list)}"
                change = 0

                for stock in next_stock_momentum_list:
                    df = crawl_OHCLV(stock, interval="1W", exchange="HOSE", nbars=3)
                    msg += f"\n{stock}: {int(df['close'].values[-1])} ({df['close'].pct_change().values[-1]*100:.2f}%)"
                    change += df['close'].pct_change().values[-1]*100

                msg += f"\nPortfolio: {change/len(stock_momentum_list):.2f}%"

                send_notification(msg)
                time.sleep(60)

            

        # elif is_trading_hour(now):
        #     stock_momentum_list = np.loadtxt("data/momentum_list.txt", dtype=str)
        #     msg = f"[{time.strftime('%Y-%m-%d %H:%M:%S', now)}]\n Current weekly stock momentum list: {', '.join(stock_momentum_list)}"
        #     change = 0

        #     for stock in stock_momentum_list:
        #         df = load_data_direct(stock, interval="1W", exchange="HOSE", nbars=3)
        #         msg += f"\n{stock}: {df['close'].values[-1]} ({df['close'].pct_change().values[-1]*100:.2f}%)"
        #         change += df['close'].pct_change().values[-1]*100

        #     msg += f"\nPortfolio: {change/len(stock_momentum_list):.2f}%"
        #     send_notification(msg)
        #     time.sleep(60)
        # else:
        #     # Sleep for 5 seconds before checking again
        #     time.sleep(5)
    

if __name__ == "__main__":
    main()