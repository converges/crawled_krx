import datetime
import sqlite3
#
import requests
import pandas as pd

class Format:
    def to_date(self, date, format):
        if isinstance(date, datetime.datetime):
            return date.strftime(format)
        elif isinstance(date, str):
            date = date.replace("/", "-").replace("_", "-")
            return pd.to_datetime(date).strftime(format)
        else:
            print(f"ErrorMessage: {date} is NOT a supported format.")
            return date
    def to_yyyymmdd(self, date):
        return self.to_date(date, format="%Y%m%d")
    def to_yyyy_mm_dd(self, date):
        return self.to_date(date, format="%Y-%m-%d")

    def to_number(self, num):
        if isinstance(num, int):
            return num
        elif isinstance(num, float):
            return num
        elif isinstance(num, str):
            num = num.replace(",", "_")
            if '.' in num:
                return float(num)
            else:
                return int(num)
        else:
            print(f"ErrorMessage: {num} is NOT a supported format.")
            
def format_types(stock_type):
    if stock_type in ['comm', 'common', '보통주']:
        return 'comm'
    elif stock_type in ['pref', 'preferred', '우선주', '구형우선주', '신형우선주']:
        return 'pref'
    else:
        print(f"ErrorMessage: {stock_type} is NOT a supported format.")
        return None

request_url = 'http://data.krx.co.kr/comm/bldAttendant/getJsonData.cmd'
request_data = {
    'bld': 'dbms/MDC/STAT/standard/MDCSTAT01901',
    'mktId': 'ALL', # 'ALL': 코스피+코스닥+코넥스, 'STK': 코스피, 'KSQ': 코스닥
    'share': '1', # 주식 수 단위(1, 1_000, 1_000_000)
    'csvxls_isNo': 'false'
}

response = requests.post(url=request_url, data=request_data)

# # All columns of the responsed data.
# # keys: response.text cols, values: represented cols in KRX, comments(#) are an example of the data.
# response_cols = {
#     "ISU_CD":"종목코드", #KR7098120009
#     "ISU_SRT_CD":"표준코드", #098120
#     "ISU_NM":"한글종목명", #(주)마이크로컨텍솔루션
#     "ISU_ABBRV":"한글종목약명", #마이크로컨 텍솔
#     "ISU_ENG_NM":"영문종목명", #Micro Contact Solution Co.,Ltd.
#     "LIST_DD":"상장일", #2008/09/23
#     "MKT_TP_NM":"시장구분", #KOSDAQ
#     "SECUGRP_NM":"증권구분", #주권
#     "SECT_TP_NM":"소속부", #벤처기업부
#     "KIND_STKCERT_TP_NM":"주식종류", #보통주
#     "PARVAL":"액면가", #500
#     "LIST_SHRS":"상장주식수" #8,312,766
#     }

# Selected Cols: I need columns below only.
# keys: response.text cols, values: my preferrence.
response_cols = {
    "ISU_SRT_CD": "ticker", # 098120
    "ISU_NM": "kor_name", #(주)마이크로컨텍솔루션
    "ISU_ENG_NM": "eng_name", # Micro Contact Solution Co.,Ltd.
    "LIST_DD": "listed_date", # 2008/09/23
    "MKT_TP_NM": "market", # KOSDAQ
    "KIND_STKCERT_TP_NM": "type", # 보통주
    "LIST_SHRS": "shares_qty" # 8,312,766
    }

# reponse.text -> dataframe.
df = pd.DataFrame(eval(response.text)['OutBlock_1']).loc[:, list(response_cols.keys())].rename(columns=response_cols)

df['ticker'] = df['ticker'].apply(lambda ticker: 'A' + ticker)
df['listed_date'] = df['listed_date'].apply(lambda date: date.replace("/", "-"))
df['shares_qty'] = df['shares_qty'].apply(lambda num: int(num.replace(",","_")))
df['type'] = df['type'].apply(format_types)

df.set_index('ticker', drop=True, inplace=True)

print(df)
print(df.dtypes)

con = sqlite3.connect("../db/krx.db")
df.to_sql("krx_tickers", con, if_exists='replace')