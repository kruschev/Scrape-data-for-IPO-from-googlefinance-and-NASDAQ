import pandas as pd
from pandas_datareader.google.daily import GoogleDailyReader

class FixedGoogleDailyReader(GoogleDailyReader):
    @property
    def url(self):
        return 'http://finance.google.com/finance/historical'

import datetime
from pandas.tseries.offsets import BDay

from bs4 import BeautifulSoup
import requests

url = "http://www.nasdaq.com/markets/ipos/activity.aspx?tab=pricings&month="


df = pd.read_csv("df.csv", delimiter=";")

for index, row in df.iterrows():
    ipo_date = datetime.datetime.strptime(row['Date'], '%d/%m/%Y') #if year format is 17 instead of 2017 use lowercase y
    row['StartDate'] = ipo_date + BDay(6) #skip six business days
    row['EndDate'] = row['StartDate'] + BDay(240) #scrape volume & std for 240 days after IPO
    df.loc[index, 'StartDate'] = row['StartDate']
    df.loc[index, 'EndDate'] = row['EndDate']

    #Scrape volume & std from googlefinance
    try:
        reader = FixedGoogleDailyReader(symbols=[row['Symbol']], start=row['StartDate'], end=row['EndDate'], chunksize=25, retry_count=3, pause=0.001, session=None)
        df.loc[index, 'Volume'] = reader.read()['Volume'].sum()[0]
        df.loc[index, 'Std'] = reader.read()['Close'].std()[0]
    except:
        print(row['Symbol'] + ' can not be found on GoogleFinance')
        continue

    #Scrape offer amount from nasdaq
    req_link = url + str(ipo_date.year) + "-" + str(ipo_date.month)
    try:
        page = requests.get(req_link)
        soup = BeautifulSoup(page.text, 'html.parser')
        df.loc[index, 'Amount'] = soup.find("td", text=row['Symbol']).find_next_siblings("td", limit=4)[3].text
    except:
        print(row['Symbol'] + ' can not be found on Nasdaq')
        continue
    


df.to_csv("data240.csv")
