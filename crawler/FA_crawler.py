import requests
import pandas as pd
import numpy as np
from vnstock import financial_ratio, financial_flow

default_authorization = "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSIsImtpZCI6IkdYdExONzViZlZQakdvNERWdjV4QkRITHpnSSJ9.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4iLCJhdWQiOiJodHRwczovL2FjY291bnRzLmZpcmVhbnQudm4vcmVzb3VyY2VzIiwiZXhwIjoxOTQwMDU3NTgyLCJuYmYiOjE2NDAwNTc1ODIsImNsaWVudF9pZCI6ImZpcmVhbnQudHJhZGVzdGF0aW9uIiwic2NvcGUiOlsib3BlbmlkIiwicHJvZmlsZSIsInJvbGVzIiwiZW1haWwiLCJhY2NvdW50cy1yZWFkIiwiYWNjb3VudHMtd3JpdGUiLCJvcmRlcnMtcmVhZCIsIm9yZGVycy13cml0ZSIsImNvbXBhbmllcy1yZWFkIiwiaW5kaXZpZHVhbHMtcmVhZCIsImZpbmFuY2UtcmVhZCIsInBvc3RzLXdyaXRlIiwicG9zdHMtcmVhZCIsInN5bWJvbHMtcmVhZCIsInVzZXItZGF0YS1yZWFkIiwidXNlci1kYXRhLXdyaXRlIiwidXNlcnMtcmVhZCIsInNlYXJjaCIsImFjYWRlbXktcmVhZCIsImFjYWRlbXktd3JpdGUiLCJibG9nLXJlYWQiLCJpbnZlc3RvcGVkaWEtcmVhZCJdLCJzdWIiOiI0ZTM0MDgxYi0xNzEyLTRhOGQtYTgxOC02NWJlYTg1MWJhY2YiLCJhdXRoX3RpbWUiOjE2NDAwNTc1MzAsImlkcCI6Imlkc3J2IiwibmFtZSI6InRoYW5odHJ1bmdodXluaDkzQGdtYWlsLmNvbSIsInNlY3VyaXR5X3N0YW1wIjoiYzJlNmI0ZGQtZDUwOS00Y2I0LWFmNTYtOGM3MjU0YWYwOTc1IiwicHJlZmVycmVkX3VzZXJuYW1lIjoidGhhbmh0cnVuZ2h1eW5oOTNAZ21haWwuY29tIiwidXNlcm5hbWUiOiJ0aGFuaHRydW5naHV5bmg5M0BnbWFpbC5jb20iLCJmdWxsX25hbWUiOiJUcnVuZ0hUIiwiZW1haWwiOiJ0aGFuaHRydW5naHV5bmg5M0BnbWFpbC5jb20iLCJlbWFpbF92ZXJpZmllZCI6InRydWUiLCJqdGkiOiJhNDhiMmFhNGQxNTliYzZjY2JhN2FlMjYzY2JlOWNjMCIsImFtciI6WyJwYXNzd29yZCJdfQ.YrqVoLmfX7gn38gt4iME-u_tcAl2lsK3ZHcnA9CKS_kLWCwChYql9hl8vZCwfQGEZuIihf4n0fARruetbQIF_rzPbK_N4GaBPtfgbB038QbI-pl1HbjnVA8pGj8LcoTIyLDPfKSMQ92Ewu6fmfzEjqU2IFBNEztNQNsqXtu2d_ougO6raTGzdXQyq1zfUB-Qrtf1YhLW8iOR3x45sBDdyYAsECXSkcgoo3WTfmGakrt-WtR32dkMZBPLcPFGVwM1NXXdItRS1PcL7mw5jmBS5eIudH8vpRarRnheU3X8bY9P8i5-2PIaFZWEmEqDcK7G8B1wk9PvKCSnkjn-KV50lQ"

def getBasicInfo(stock, authorization = default_authorization):
    url = "https://restv2.fireant.vn/symbols/{}/financial-indicators".format(stock)
    headers = {'Accept-Encoding' : 'utf-8'}
    headers["authorization"] = authorization
    response = requests.get(url, headers=headers)
    data = response.json()
    
    return data

def getFundamental(stock, authorization = default_authorization):
    url = "https://restv2.fireant.vn/symbols/{}/fundamental".format(stock)
    headers = {'Accept-Encoding' : 'utf-8'}
    headers["authorization"] = authorization
    response = requests.get(url, headers=headers)
    data = response.json()
    
    return data

def fetchFullFinancialReports(stock, type = 1, current_quarter = "Q4/2022", limit = 24, authorization = default_authorization):

    quarter = int(current_quarter.split("/")[0][1])
    year = int(current_quarter.split("/")[1])
    
    url = "https://restv2.fireant.vn/symbols/{}/full-financial-reports?type={}&year={}&quarter={}&limit={}".format(stock, type, year, quarter, limit)
    headers = {'Accept-Encoding' : 'utf-8'}
    headers["authorization"] = authorization
    response = requests.get(url, headers=headers)
    data = response.json()
    
    return data

def crawlBasicFA(stock_list, authorization = default_authorization):

    needed_fund_cols = ['freeShares', 'sharesOutstanding', 'beta', 'marketCap', 'avgVolume3m', 'insiderOwnership', 'institutionOwnership', 'foreignOwnership']
    needed_FA_cols = ['P/E', 'P/S', 'P/B', 'EPS', 'ROA', 'ROE']
    columns = ["ticker"] + needed_fund_cols + needed_FA_cols

    dats = []

    for stock in stock_list:
        print(stock)
        res = [stock]    
        
        data_fund = getFundamental(stock, authorization)
        data_FA = getBasicInfo(stock, authorization)
        
        for col in needed_fund_cols:        
            if col in data_fund:
                res.append(data_fund[col])
                
        for content in data_FA:        
            if content["shortName"] in columns:
                res.append(content["value"])
                
        if len(res) != len(columns):
            print(len(res))
            print(len(columns))

        dats.append(res)
        
    df = pd.DataFrame(dats, columns = columns)
    df["institutionalOwnership"] = (df["insiderOwnership"] + df["institutionOwnership"] + df["foreignOwnership"])
    df["freeFloat"] = 1 - df["institutionalOwnership"]
    
    return df

if __name__ == '__main__':
    stock_list = np.load("data/list_stocks.npy", allow_pickle = True)
    
    # print("Loading basicFA from Fireant")
    # df = crawlBasicFA(stock_list)
    # df.to_csv("data/basicFA.csv")

    print("Loading income statements from TCBS")    
    for stock in stock_list:

        print(stock)

        quarter_financial_ratio = financial_ratio(stock, 'quarterly', True)
        quarter_income_statement = financial_flow(symbol=stock, report_type='incomestatement', report_range='quarterly', get_all=True)
        quarter_balancesheet = financial_flow(symbol=stock, report_type='balancesheet', report_range='quarterly', get_all=True)
        yearly_financial_ratio = financial_ratio(stock, 'yearly', True)
        yearly_income_statement = financial_flow(symbol=stock, report_type='incomestatement', report_range='yearly', get_all=True)
        yearly_balancesheet = financial_flow(symbol=stock, report_type='balancesheet', report_range='yearly', get_all=True)

        quarter_financial_ratio.to_csv("data/financial_statements/quarterly/{}.csv".format(stock))
        quarter_income_statement.to_csv("data/income_statements/quarterly/{}.csv".format(stock))
        quarter_balancesheet.to_csv("data/balance_sheets/quarterly/{}.csv".format(stock))
        yearly_financial_ratio.to_csv("data/financial_statements/yearly/{}.csv".format(stock))
        yearly_income_statement.to_csv("data/income_statements/yearly/{}.csv".format(stock))
        yearly_balancesheet.to_csv("data/balance_sheets/yearly/{}.csv".format(stock))

        