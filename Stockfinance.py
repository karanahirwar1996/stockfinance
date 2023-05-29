def Stockfinance(url):
    import datetime
    import requests
    from bs4 import BeautifulSoup
    import json
    import pandas as pd
    
    res=requests.get(url)
    soup=BeautifulSoup(res.content,features="html.parser")
    script_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if script_tag is None:
        print(f"No script tag with id '__NEXT_DATA__' found on {url}")
        return None
    json_data = json.loads(script_tag.string)
    security_info = json_data['props']['pageProps'].get('securityInfo',None)
    isin = security_info.get('isin', None)
    finance=json_data['props']['pageProps']['securitySummary']['financialSummary'].get('fiscalYearToData',None)
    
    df = pd.DataFrame(finance)
    df['ASIN'] = isin
    df['Growth Rate'] = round(((df['revenue'] - df['revenue'].shift(1)) / df['revenue'].shift(1)) * 100,2)
    df['Profit Growth Rate'] = round(((df['profit'] - df['profit'].shift(1)) / df['profit'].shift(1)) * 100,2)
    sorted_df=df[['ASIN','year','revenue','profit','Growth Rate','Profit Growth Rate']].copy()
    return sorted_df
import pandas as pd
import multiprocessing
import time
import gspread
from gspread_dataframe import set_with_dataframe
from oauth2client.service_account import ServiceAccountCredentials
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('./original-advice-385307-e221975bf7db.json', scope)
client = gspread.authorize(creds)
gs = client.open('AllStockData')
main_sheet=gs.worksheet('Main')
all_record_main=main_sheet.get_all_records()
main_df=pd.DataFrame(all_record_main)
url_list=list(main_df['URL'])
def process_url(url):
    return Stockfinance(url)
if __name__ == '__main__': 
    pool = multiprocessing.Pool()
    results = pool.map(process_url, url_list)
    pool.close()
    pool.join()
    result_df = pd.concat(results, ignore_index=True)
gsnew = client.open('AllStockData')
main_sheet=gsnew.worksheet('Finance')
main_sheet.clear()
set_with_dataframe(main_sheet,result_df)
