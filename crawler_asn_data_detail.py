#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#----------------------------------------------------------------------------
# Python = 3.9
# Created By  : DenverAlmighty
# Created Date: 2024-12-29
# Updated Date : 2024-12-30
# version = '1.0.0'
# ---------------------------------------------------------------------------

import sys
import time
import re
import requests
from bs4 import BeautifulSoup
from html_table_parser import parser_functions as parser
import pandas as pd

# CHANGE ME
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
            # print(url)
            print('error', response.status_code)
            sys.exit()
            
    except Exception as e:
        # sys.stderr.write(e)
        sys.exit()

def main():
    for y in range(1919, 2025):
        total_df = pd.DataFrame()
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

        print(f'##### Total accident in {y} : {row_cnt}, total page : {page_cnt}')
        
        links = []
        for p in range(1, page_cnt+1):
            url = f'{base_url}/{p}'
            result = crawling(url)
            links = links + result.find_all('span', attrs = {'class':'nobr'})
        
        page_df = pd.DataFrame()
        for link in links:
            link = str(link)
            # print(link)
            try:
                pattern = 'wikibase|database\/[^\"]+'
                if re.findall(pattern, link):
                    detail_urls = re.findall('wikibase\/\d+|database\/[^\"]+', link)[0]
                    detail_page = crawling(f'https://asn.flightsafety.org/{detail_urls}')
                    temp = detail_page.find_all('table')[0]
                    p = parser.make2d(temp)
                    ddf = pd.DataFrame(p)
                    ddf=ddf.transpose()
                    ddf = ddf.rename(columns=ddf.iloc[0]).loc[1:]

                    time.sleep(0.1)
                    page_df = pd.concat([page_df, ddf])
                    
                        
            except TypeError as e:
                print('\n #### pass', link, '\n' )
                pass
            
        total_df = pd.concat([total_df, page_df])
        print('##### total_df : ', len(total_df))

        if row_cnt != len(total_df):
            print(f'Year {y} Error !!!!')
        else:
            print(f'Year {y} OK ')
        
        time.sleep(0.1)
        total_df.to_csv(f'{PATH}/ASN_Safety_Database_detail_{y}.csv', sep=',', encoding='utf-8', index=False, header=True)
    
    
if __name__=='__main__':
    main()
