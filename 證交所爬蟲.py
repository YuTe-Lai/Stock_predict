import requests
import urllib3.contrib.pyopenssl
from io import StringIO
import pandas as pd
import numpy as np
import datetime
import time
import json
pd.set_option("display.max_columns",500)
import csv



#抓取單日股市資訊
def crawl_price(date):
    # 下載股價
    r = requests.post('http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' + date + '&type=ALLBUT0999')
# 整理資料，變成表格
    df1 = pd.read_csv(StringIO(r.text.replace("=", "")), 
                      header=["證券代號" in l for l in r.text.split("\n")].index(True)-1)

# 整理一些字串：
    df = df1.apply(lambda s: pd.to_numeric(s.astype(str).str.replace(",", "").replace("+", "1").replace("-", "-1"), errors='coerce'))
    df['證券代號'] = df1['證券代號']
    df['證券名稱'] = df1['證券名稱']
    df = df.set_index('證券代號')
    return df
#    break




#抓取多日股市資訊
def crawl_price_nDays(start_date, n_days,allow_continuous_fail_count = 8):
    date = start_date   #開始日期
    n_days = n_days         #天數

    DATA = {}
    fail_count = 0
    allow_continuous_fail_count = allow_continuous_fail_count 
    dd = datetime.datetime.strptime(date,'%Y%m%d')

    str_date = str(dd).split(' ')[0].replace('-','')

    while len(DATA) < n_days:
        print('parsing',str(dd).split(' ')[0])
        try:
            DATA[str(dd).split(' ')[0]] = crawl_price(str(dd).split(' ')[0].replace('-',''))
            print('success!')
            fail_count = 0
        except:
            # 假日爬不到
            print('fail! check the date is holiday')
            fail_count += 1
            if fail_count > allow_continuous_fail_count:
                raise
                break
    
        # 加一天
        dd += datetime.timedelta(days=1)
        time.sleep(12)        
        
    return DATA
    


#抓單日法人買賣超資訊
def fa_human(date):
# 下載法人買賣超
    r = requests.post('https://www.twse.com.tw/fund/T86?response=csv&date=' + date + '&selectType=ALLBUT0999&report-table=-1')

# 整理資料，變成表格
    df1 = pd.read_csv(StringIO(r.text.replace("=", "")), header=["證券代號" in l for l in r.text.split("\n")].index(True))

# 整理一些字串：
    df = df1.apply(lambda s: pd.to_numeric(s.astype(str).str.replace(",", ""), errors='coerce'))
    df['證券代號'] = df1['證券代號']
    df['證券名稱'] = df1['證券名稱']
    df = df.drop(df.index[-8:-1])
    df = df.drop(df.index[-1]) #刪除最後一筆資料
    df = df.set_index('證券代號')


    return df
    
    
    
#抓取多日三大法人買賣超資訊
def fa_human_nDays(start_date, n_days, allow_continuous_fail_count = 5):
    
    date = start_date   #開始日期
    n_days = n_days             #天數

    DATA = {}
    fail_count = 0
    allow_continuous_fail_count = allow_continuous_fail_count 
    dd = datetime.datetime.strptime(date,'%Y%m%d')

    str_date = str(dd).split(' ')[0].replace('-','')

    while len(DATA) < n_days:
        print('parsing',str(dd).split(' ')[0])
        try:
            DATA[str(dd).split(' ')[0]] = fa_human(str(dd).split(' ')[0].replace('-',''))
            print('success!')
            fail_count = 0
        except:
            # 假日爬不到
            print('fail! check the date is holiday')
            fail_count += 1
            if fail_count == allow_continuous_fail_count:
                raise
                break
    
        # 加一天
        dd += datetime.timedelta(days=1)
        time.sleep(12)        
    return DATA


#以收盤價 按時間序列排序
def transform_column(DATA,column_name):
    close = pd.DataFrame({k:d[column_name] for k, d in DATA.items()}).transpose()
    close.index = pd.to_datetime(close.index)
    return close
    
    
    
#stock_DATA = crawl_price_nDays('20201106', 2)
#close_20201106 = transform_column(stock_DATA,'收盤價')
#fa_DATA = fa_human_nDays('20201106',2)
#difference_20201106 = transform_column(fa_DATA,'三大法人買賣超股數')

    
