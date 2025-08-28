import pandas as pd
import os, time
import argparse
from crawler.Tv_loader import TvDatafeed, Interval
from crawler.VND_loader import VndCrawler
from datetime import datetime
from pytz import timezone
from pathlib import Path
import numpy as np

def parse_args():
    parser = argparse.ArgumentParser(description="crawler")
    # parser.add_argument('--source_file', default="data/Vietnam/HOSE_list_stock.csv")
    parser.add_argument('--source_file', default="data/Vietnam/list_stocks.npy")
    parser.add_argument('--info_file', default="data/Vietnam/stock_infos.csv")
    parser.add_argument('--target_folder', default="data/Vietnam/OHCLV")
    parser.add_argument('--interval', default="1day")    
    parser.add_argument('--crawl_new', default=False, type=bool)    

    #Source is tradingview or vnd
    parser.add_argument('--crawl_source', default="tradingview")

    #For tradingview
    parser.add_argument('--nbars', default=5000, type=int)
    parser.add_argument('--username', default="thanhtrunghuynh93")
    parser.add_argument('--password', default="@Manutd93@")
    parser.add_argument('--driver_path', default="chromedriver")    
    
    #For vnd
    parser.add_argument('--start_date', default="2021-02-01 09:00:00")

    return parser.parse_args()

def load_data_direct(ticker, exchange = "HOSE", interval = "1W", nbars = 5000):
    username = 'thanhtrunghuynh93'
    password = '@Manutd93@'
    crawler=TvDatafeed(username, password, chromedriver_path="chromedriver")
    if interval == "1D":
        df = crawler.get_hist(symbol=ticker,exchange=exchange,interval=Interval.in_daily, n_bars = nbars)
    if interval == "1W":
        df = crawler.get_hist(symbol=ticker,exchange=exchange,interval=Interval.in_weekly, n_bars = nbars)
    df["ticker"] = ticker
    return df

def crawl_OHCLV(ticker, exchange = "HOSE", interval = "1W", nbars = 5000, max_retries=3):
    for attempt in range(max_retries):
        try:
            return load_data_direct(ticker, exchange, interval, nbars)
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(1)

# def crawl(stock_infos, stock_list, output_dir, crawl_new, interval, crawl_source, args):

#     if interval == "1hour":
#         itv = Interval.in_1_hour
#     elif interval == "1day":
#         itv = Interval.in_daily
#     else:
#         print("Interval not support")
#         return

#     if crawl_source == "tradingview":
#         crawler = TvDatafeed(args.username, args.password, chromedriver_path=args.driver_path)
#         crawl_range = args.nbars
#     elif crawl_source == "vnd":
#         crawler = VndCrawler()
#         crawl_range = args.start_date
#     else:
#         print("Crawl source not support")
#         return

#     # stock_list.append("VNINDEX")
#     if interval == "1day":
#         crawl_by_day(stock_infos, stock_list, output_dir, crawl_new, crawl_source, crawl_range, crawler)

#         #Crawl VNI
#         start_data_date = datetime.strptime("2017-02-01 09:00:00", '%Y-%m-%d %H:%M:%S')
#         start_data_date = int(start_data_date.timestamp())
#         vnd_crawler = VndCrawler()
#         vni_data = vnd_crawler.get_hist(resolution = "D", from_date = start_data_date)
#         vni_data["symbol"] = "HOSE:VNINDEX"

#         vni_data.to_csv(output_dir + "/VNINDEX_1day.csv")       

#     if interval == "1hour":
#         pass
#         # crawl_by_hour(crawler, stock_infos, stock_list, nbars, output_dir, crawl_new)


# def crawl_by_hour(stock_infos, stock_list, nbars, output_dir, crawl_new = False, crawl_source, crawler, crawl_range):

#     interval = "1hour"
#     now = datetime.now(timezone('Asia/Saigon')).replace(tzinfo=None) 
#     print("Crawling time ", now)

#     for code in stock_list:
#         print(code)
#         if code == "VNINDEX":
#             exchange = "HOSE"
#         else:
#             exchange = stock_infos.loc[code].market
#         start_time = time.time()

#         if Path(output_dir + "/" + code + "_" + interval + ".csv").is_file() and not crawl_new:
            
#             try:
#                 #Load the existing dataset, append and save
#                 data = pd.read_csv(output_dir + "/" + code + "_" + interval + ".csv", index_col = "datetime")
            
#             except:
#                 print("Error: Empty data file, crawl new data")
#                 data = crawler.get_hist(symbol=code,exchange=exchange,interval=Interval.in_1_hour,n_bars = nbars)
#                 print("--- %s seconds ---" % (time.time() - start_time))
#                 print("Create new {} records".format(data.shape[0]))                            
#                 if data is None:        
#                     print("Error")
#                 else:
#                     data.to_csv(output_dir + "/" + code + "_" + interval + ".csv")            
#                 continue

            
#             last_date = datetime.strptime(str(data.tail(1).index.values[0]), '%Y-%m-%d %H:%M:%S')                        
#             difference_day = (now - last_date).days
            
#             #Check if today is weekend
#             if now.weekday() > 4:
#                 difference_day = difference_day - (now.weekday() - 4)
            
#             max_difference_hour = difference_day * 5 + 5

#             try:                
#                 new_data= crawler.get_hist(symbol=code,exchange=exchange,interval=Interval.in_1_hour,n_bars=max_difference_hour)                                        
#                 new_data = new_data[new_data.index > last_date]
#                 print("Found new {} records".format(len(new_data)))
#                 if len(new_data) == 0:
#                     print("The data is already up-to-date")                
#                 else:                    
#                     data = pd.concat((data, new_data), join='outer', copy=True)
#                     data.to_csv(output_dir + "/" + code + "_" + interval + ".csv")            
#                     print("--- %s seconds ---" % (time.time() - start_time))                                
#             except Exception as e:
#                 print("Error")            
#                 print(e)            

#         else:         	
#             try:
#                 data = crawler.get_hist(symbol=code,exchange=exchange,interval=Interval.in_daily,n_bars = nbars)
#                 print("--- %s seconds ---" % (time.time() - start_time))
#                 print("Create new {} records".format(data.shape[0]))                            
#                 if data is None:        
#                     print("Error")
#                 else:
#                     data.to_csv(output_dir + "/" + code + "_" + interval + ".csv")            
    
#             except Exception as e:
#                 print("Error")
#                 print(e)            


def crawl_by_day(stock_infos, stock_list, output_dir, crawl_new, crawl_source, crawl_range, crawler):

    interval = "1day"
    now = datetime.now(timezone('Europe/Berlin')).replace(tzinfo=None) 
    # now = datetime.now(timezone('Asia/Saigon')).replace(tzinfo=None) 
    print("Crawling time ", now)
    num_try = 3

    for code in stock_list:
        print(code)
        if code == "VNINDEX":
            exchange = "HOSE"
        else:
            exchange = stock_infos.loc[code].comGroupCode
        start_time = time.time()

        if Path(output_dir + "/" + code + "_" + interval + ".csv").is_file() and not crawl_new:
            
            try:
                #Load the existing dataset, append and save
                data = pd.read_csv(output_dir + "/" + code + "_" + interval + ".csv", index_col = "datetime")
            except:
                print("Error: Empty data file, crawl new data")

                if crawl_source == "tradingview":
                    data = crawler.get_hist(symbol=code,exchange=exchange,interval=Interval.in_daily,n_bars = crawl_range)
                if crawl_source == "vnd":
                    data = crawler.get_hist(resolution = "D", symbol=code, from_date = "2017-02-01 09:00:00")
                    data["symbol"] = exchange + ":" + code
                    
                print("--- %s seconds ---" % (time.time() - start_time))
                print("Create new {} records".format(data.shape[0]))                            
                if data is None:        
                    print("Error")
                else:
                    data.to_csv(output_dir + "/" + code + "_" + interval + ".csv")            
                continue

            last_date = datetime.strptime(str(data.tail(1).index.values[0]), '%Y-%m-%d %H:%M:%S')    
            last_date = last_date.replace(hour = 0)                    
            difference_day = (now - last_date).days

            #Check if today is weekend
            if now.weekday() > 4:
                difference_day = difference_day - (now.weekday() - 4)

            
            if difference_day == 0:
                for i in range(num_try):
                    try:
                        print("The data is up-to-date, update only the data today")
                        if crawl_source == "tradingview":
                            new_data = crawler.get_hist(symbol=code,exchange=exchange,interval=Interval.in_daily,n_bars=1)                                        
                        if crawl_source == "vnd":
                            from_date = int(last_date.timestamp())
                            new_data = crawler.get_hist(resolution = "D", symbol=code, from_date = from_date)
                            new_data["symbol"] = exchange + ":" + code
                            
                        data.loc[data.index[-1]] = new_data.values[0]                    
                        data.to_csv(output_dir + "/" + code + "_" + interval + ".csv")            
                        print("--- %s seconds ---" % (time.time() - start_time))                                
                        break
                    except:
                        print("Error, try again")            
            else:
                last_date = last_date.replace(hour = 9)                    
                for i in range(num_try):
                    try:
                        if crawl_source == "tradingview":
                            new_data= crawler.get_hist(symbol=code,exchange=exchange,interval=Interval.in_daily,n_bars=difference_day)                                        
                        if crawl_source == "vnd":
                            from_date = int(last_date.timestamp())
                            new_data = crawler.get_hist(resolution = "D", symbol=code, from_date = from_date)
                            new_data["symbol"] = exchange + ":" + code
                                                    
                        new_data = new_data[new_data.index > last_date]

                        print("Found new {} records".format(len(new_data)))
                        if len(new_data) == 0:
                            print("Error")
                        else:                    
                            data = pd.concat((data, new_data), join='outer', copy=True)
                            data.to_csv(output_dir + "/" + code + "_" + interval + ".csv")            
                            print("--- %s seconds ---" % (time.time() - start_time))                  
                            break              
                    except:
                        print("Error, try again")            

        else:
            print("Crawl new data")
            data = crawler.get_hist(symbol=code,exchange=exchange,interval=Interval.in_daily,n_bars = crawl_range)
            print("--- %s seconds ---" % (time.time() - start_time))
            print("Create new {} records".format(data.shape[0]))                            
            if data is None:        
                print("Error")
            else:
                data.to_csv(output_dir + "/" + code + "_" + interval + ".csv")            
            continue

if __name__ == '__main__':

    os.environ['TZ'] = 'Asia/Saigon'
    time.tzset()
    
    args = parse_args()

    stock_list = np.load(args.source_file, allow_pickle = True)
    stock_infos = pd.read_csv(args.info_file, index_col = "ticker")

    crawl(stock_infos, stock_list.tolist(), args.target_folder, args.crawl_new, args.interval, args.crawl_source, args)
    # crawl(crawler, stock_infos, stock_list.tolist(), args.interval, args.nbars, args.target_folder, args.crawl_new)
    
