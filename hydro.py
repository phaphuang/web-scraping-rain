#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import sys
reload(sys)
#sys.setdefaultencoding('tis-620')

import requests
import pandas as pd
import bs4 as bs
import time

#from datetime import datetime
#current_date = datetime.now().date().strftime('%Y%m%d')
#print current_date

# set pandas encoding to utf-8
pd.options.display.encoding = str('utf-8')

def convert(content):
    #print content
    result = ''
    for char in content:
        asciichar = char.encode('ascii',errors="backslashreplace")[2:]
        if asciichar =='':
            utf8char = char.encode('utf-8')
        else:
            try:
                hexchar =  asciichar.decode('hex')
            except:
                #print asciichar
                utf8char = ' '
            try:
                utf8char = hexchar.encode('utf-8')
            except:
                #print hexchar
                utf8char = ' '
            #print utf8char

        result = result + utf8char
        #print result
    return result

user_agent = 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; en-US) AppleWebKit/534.3 (KHTML, like Gecko) Chrome/6.0.472.63 Safari/534.3'
headers = { 'User-Agent' : user_agent }

base_url = "http://hydro-1.net/08HYDRO/HD-03/3-01-DOCS/"
main_url = "http://hydro-1.net/08HYDRO/HD-03/3-01-DOCS/3-01A.html"


new_sess = requests.Session()
req = new_sess.get(main_url, headers=headers)
#req_encoding = req.encoding if 'charset' in req.headers.get('content-type', '').lower() else None
soup = bs.BeautifulSoup(req.text, 'lxml')

links = soup.find_all('a')

# Create Global DataFrame
all_station_df = pd.DataFrame()
temp_df = pd.DataFrame()

file_no = 0

for (i, lks) in enumerate(links[:-1]):
    time.sleep(5)

    try:
        #print(lks.attrs['href'])
        temp_url = lks.attrs['href']

        start_date = temp_url.split('/')[-1].split('.')[0].split('-')[1]
        end_date = temp_url.split('/')[-1].split('.')[0].split('-')[-1]
        end_year = temp_url.split('/')[-1].split('-')[0][:2]
        end_month = temp_url.split('/')[-1].split('-')[0][2:]
        end_month = str(int(end_month))
        if (start_date > end_date):
            if (end_month == '1'):
                start_month = '12'
                start_year = str(int(start_year) - 1)
            else:
                start_month = str(int(end_month) - 1)
                start_year = end_year
        else:
            start_month = str(int(end_month))
            start_year = end_year
        print('From '+'20' + start_year + '-' + start_month + '-' + start_date + ' to ' + '20' + end_year + '-' + end_month + '-' + end_date)
        sub_url = base_url + temp_url

        print(sub_url)

        req_table = new_sess.get(sub_url, headers=headers)
        #req_table_encoding = req_table.encoding if 'charset' in req_table.headers.get('content-type', '').lower() else None
        soup_table = bs.BeautifulSoup(req_table.text, 'lxml')

        # Check charset for different encoding
        charset = soup_table.find_all('meta')[0].attrs['content'].split('=')[1]
        print(charset)

        station_table = soup_table.find_all('tr')

        days_list = station_table[7].find_all('td')
        #print(date_list)
        days = [d.text.strip() for d in station_table[7].find_all('td')[1:8]]
        print(days)

        for (j, st) in enumerate(station_table[8:-1]):

            station_row = st.find_all('td')
            if((st.attrs['height'] == '22'
                or st.attrs['height'] == '23'
                or st.attrs['height'] == '24'
                or st.attrs['height'] == '25'
                or st.attrs['height'] == '29')
                and (station_row[0].text.strip() != "")):

                if (charset == 'windows-874'):
                    sys.setdefaultencoding('tis-620')
                    #pd.options.display.encoding = str('tis-620')
                    station_name = station_row[1].text.strip()
                    station_name = convert(station_name)
                    station_name = station_name.encode('tis-620')
                else:
                    sys.setdefaultencoding('utf-8')
                    station_name = station_row[1].text.strip()
                    station_name = station_name.encode('utf-8')

                #print(station_name)

                column_name = ['date', station_name]
                #print(column_name)
                station_df = pd.DataFrame(columns=column_name)

                # Check if start new year or new month
                first_break = False
                for (d, day) in enumerate(days):
                    if (d == 0):
                        station_df = station_df.append({'date': '20' + start_year + '-' + start_month + '-' + day, station_name: station_row[7].text.strip()}, ignore_index=True)
                    # Check if date range are not in same month and year
                    else :
                        if ((days[d] > days[d-1]) and (first_break == False)):
                            station_df = station_df.append({'date': '20' + start_year + '-' + start_month + '-' + day, station_name: station_row[7 + (d-1)].text.strip()}, ignore_index=True)
                        else:
                            station_df = station_df.append({'date': '20' + end_year + '-' + end_month + '-' + day, station_name: station_row[7 + (d-1)].text.strip()}, ignore_index=True)
                            first_break = True

                station_df.set_index('date', inplace=True)

                if (j == 0):
                    temp_df = station_df
                else:
                    temp_df = pd.concat([temp_df, station_df], axis=1)

        print(temp_df)

        if (i == 0):
            all_station_df = temp_df
        else:
            all_station_df = pd.concat([all_station_df, temp_df], axis=0)

        # Reset Temp DataFrame
        temp_df = pd.DataFrame()

        #print(all_station_df)

        # Sort index
        #all_station_df.index = pd.to_datetime(all_station_df.index)
        #all_station_df.sort_index(inplace=True, ascending=False)

        # Save to csv file
        all_station_df.to_csv('result/rain_station_' + str(file_no) + '.csv', encoding='utf-8')
        file_no = file_no + 1

    except Exception as e:
        print(e)
        #pass
