from datetime import date
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import datetime
import re

x = datetime.datetime(2020, 5, 17)

# TODO: Grab all the links on the Turnstile page

def is_turnstile_link(href):
    return href and re.compile("data/nyct/turnstile/").search(href)

ridership_page = requests.get("http://web.mta.info/developers/turnstile.html")
ridership_soup = BeautifulSoup(ridership_page.content, "html.parser")
weekly_links = ridership_soup.findAll('a', href=is_turnstile_link)


ridership_df = pd.DataFrame(index=pd.read_csv('../processed_data/booth_to_complex.csv')['Booth'])
#ridership_df.set_index('Booth', inplace=True)

month_dict = {'January' : 1, 'February' : 2, 'March' : 3, 'April' : 4, 'May' : 5, 'June' : 6, 'July' : 7, 'August': 8, 'September' : 9, 'October' : 10, 'November' : 11, 'December' : 12}

#frames = []

stop = 0

for weekly_link in weekly_links:
    if stop > 3:
        break;.
    date_split = [x.strip() for x in weekly_link.text.split(",")]
    if len(date_split) == 3 and int(date_split[2]) >= 2019:
        df = pd.read_csv('http://web.mta.info/developers/' + weekly_link['href'])[['C/A', 'SCP', 'DATE', 'TIME', 'ENTRIES']]

        # add types & split DATE
        df['DATETIME'] = pd.to_datetime(df['DATE'] + ' ' + df['TIME'], infer_datetime_format=True)
        df = df[['C/A', 'SCP', 'DATETIME', 'ENTRIES']]
        df["C/A"] = df["C/A"].astype(str)
        df["ENTRIES"] = df["ENTRIES"].astype(int)

        df = df.loc[df.groupby(['C/A', 'SCP'])['DATETIME'].idxmin()].reset_index(drop=True)
        df = df.groupby(['C/A']).agg({'ENTRIES': 'sum', 'DATETIME': 'min'}).set_index('C/A')

        week_number = df["DATETIME"].mean().date().isocalendar()[1]
        week = week_number * df["DATETIME"].mean().date().isocalendar()[0] + week_number

        df[week] = df['ENTRIES']

        ridership_df.join(df[week], how='left')
        stop += 1




        #df['WEEK'] = df["DATETIME"].mean().date().isocalendar()[1]
        #df['YEAR'] = df["DATETIME"].mean().date().isocalendar()[0]



        #frames.append(df[['C/A', 'ENTRIES', 'WEEK', 'YEAR']])

print(ridership_df)

#for frame in frames:
    #ridership_df[]
#ridership_df = pd.concat(frames)
#ridership_df.to_csv('ridership_out.csv', index=False)


#def rs(x):
    #return x.max() - x.min()

#rs.__name__ = 'RIDERSHIP'

#ridership_df = ridership_df.append(df[['STATION', 'RIDERSHIP', 'WEEK_START']].groupby(['STATION']).agg({'WEEK_START' : 'first', 'RIDERSHIP': rs, 'STATION' : 'first'}))



# Have covid data from 07/17/2021 to 8/08/2020
# So we want 07/17/2021 to 8/08/2019
# SELECT * from ridership_per_station where datetime(WEEK_START) > datetime('2019-08-08')
# SELECT *, julianday(WEEK_START) from ridership_per_station where datetime(WEEK_START) > datetime('2019-08-08')
# SELECT * from ridership_per_station as r1 JOIN ridership_perstation as r2 ON

# CREATE TABLE cool_table AS SELECT * from ridership_per_station as r1 JOIN ridership_per_station as r2 ON r1.STATION = r2.STATION
# AND (julianday(datetime(r1.WEEK_START, '-1 year')) - julianday(r2.WEEK_START)) BETWEEN -1 AND 1

# SELECT STATION, WEEK_START, (RIDERSHIP - RIDERSHIP:1)/((RIDERSHIP + RIDERSHIP:1)/2) from cool_table

#CREATE TABLE coolerr_table as SELECT STATION, WEEK_START, (RIDERSHIP - "RIDERSHIP:1")/CAST(((RIDERSHIP + "RIDERSHIP:1")/2) as REAL) as perdif, RIDERSHIP as r1, "RIDERSHIP:1" as r2 from cool_table

#SELECT * from coolerr_table ORDER BY perdif DESC
