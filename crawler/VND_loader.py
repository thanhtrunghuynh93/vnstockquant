import requests
import pandas as pd
from datetime import datetime
import numpy as np

class VndCrawler:

    def __init__(self):
        self.headers = {"Host" : "dchart-api.vndirect.com.vn", 
            "Connection": "keep-alive", 
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9", 
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36", 
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "vi-VN,vi;q=0.9"}

    def get_hist(self, resolution = "D", symbol = "VNINDEX", from_date = 1612144800, to_date = None):
        
        if to_date is None:
            to_date = int(datetime.now().timestamp())
        
        if isinstance(from_date, str):
            from_date = datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S')
            from_date = int(from_date.timestamp())
        elif not isinstance(from_date, int):
            print("from_date must be string ('%Y-%m-%d %H:%M:%S') or int (timestamp)")
            return None

        if isinstance(to_date, str):
            to_date = datetime.strptime(to_date, '%Y-%m-%d %H:%M:%S')
            to_date = int(to_date.timestamp())
        elif not isinstance(to_date, int):
            print("to_date must be string ('%Y-%m-%d %H:%M:%S') or int (timestamp)")
            return None
       
        url = "https://dchart-api.vndirect.com.vn/dchart/history?resolution={}&symbol={}&from={}&to={}".format(resolution, symbol, from_date, to_date)
        headers = self.headers
        response = requests.get(url, headers=headers)

        if "t" not in str(response.content):
            print("Crawl failed")
            print(response.content)
            return None

              
        data = response.json()
        #Convert timestamp to datetime
        string_datetimes = []
        for t in data["t"]:
            dt = datetime.fromtimestamp(t)
            dt = dt.replace(hour = 9)       
            st = dt.strftime('%Y-%m-%d %H:%M:%S')
            string_datetimes.append(st)
            
        opens = data["o"] 
        highs = data["h"] 
        lows = data["l"] 
        closes = data["c"] 
        volumes = data["v"]

        if "INDEX" not in symbol:
            opens = np.array(data["o"]) * 1000.0
            highs = np.array(data["h"]) * 1000.0
            lows = np.array(data["l"]) * 1000.0 
            closes = np.array(data["c"]) * 1000.0 
        
        symbols = [symbol] * len(opens)

        data = {"datetime": string_datetimes, "symbol": symbols, "open": opens, "high": highs, "low": lows, "close": closes, "volume": volumes}        
        df = pd.DataFrame(data)
        df['datetime'] = pd.to_datetime(df['datetime'])
        df = df.set_index("datetime")
                
        return df