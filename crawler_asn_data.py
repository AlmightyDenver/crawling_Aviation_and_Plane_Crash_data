#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#----------------------------------------------------------------------------
# Python = 3.9
# Created By  : DenverAlmighty
# Created Date: 2024-12-30
# Updated Date : 2024-12-30
# version = '1.0.0'
# ---------------------------------------------------------------------------

import sys
import time
import requests
from bs4 import BeautifulSoup
from html_table_parser import parser_functions as parser
import pandas as pd

PATH = '/my/data/path'

def crawling(url):
    response = requests.get(url, headers={'User-agent':'Mozila/5.0'}) 
    try:
        if response.status_code == 200:
            html = response.text
            soup = BeautifulSoup(html, 'html.parser')
            return soup
        
        # response code 반환
        else : 
            print('error', response.status_code)
            sys.exit()
            
    except Exception as e:
        sys.exit()

def main():
    total_df = pd.DataFrame()
    for y in range(1919, 2025):
        base_url = f'https://asn.flightsafety.org/database/year/{y}'
        title_selector = '#contentcolumnfull > div > span'
        soup = crawling(base_url)
        
        title = (soup.select_one(title_selector))
        # title example :
        # 2 occurrences in the ASN safety database
        # 216 occurrences in the ASN safety database; showing occurrence 1 - 100
        row_cnt = int(str(title)[22:].split()[0])
        if row_cnt%100 == 0:
            page_cnt = row_cnt//100
        else: page_cnt = row_cnt//100 + 1

        # print(page_cnt)
        df = pd.DataFrame()
        for p in range(1, page_cnt+1):
            url = base_url + f'/{p}'
            result = crawling(url)
            temp = result.find_all('table')
            table = temp[0]
            p = parser.make2d(temp[0])
            tmp_df=pd.DataFrame(p[1:],columns=p[0][:-1])
            df = pd.concat([df, tmp_df])
        if row_cnt != len(df):
            print(f'Year {y} Error !!!!')
        else:
            print(f'Year {y} OK ')
        
        total_df = pd.concat([total_df, df])
        time.sleep(0.5)
    total_df.to_csv(f'{PATH}/ASN_Safety_Database.csv', sep=',', encoding='utf-8', index=False, header=True)
    
    
if __name__=='__main__':
    main()
