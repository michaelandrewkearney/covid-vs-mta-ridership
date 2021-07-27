from datetime import date
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine
import datetime

x = datetime.datetime(2020, 5, 17)

# TODO: Grab all the links on the Turnstile page 
ridership_page = requests.get("http://web.mta.info/developers/turnstile.html")
ridership_soup = BeautifulSoup(ridership_page.content, "html.parser")
weekly_links = ridership_soup.findAll('a', href = True)

ridership_df = pd.DataFrame(columns = ['STATION', 'RIDERSHIP', 'WEEK_START'])
month_dict = {'January' : 1, 'February' : 2, 'March' : 3, 'April' : 4, 'May' : 5, 'June' : 6, 'July' : 7, 'August': 8, 'September' : 9, 'October' : 10, 'November' : 11, 'December' : 12}

for weekly_link in weekly_links: 
    date_split = [x.strip() for x in weekly_link.text.split(",")]
    if len(date_split) == 3 and int(date_split[2]) >= 2018 and int(date_split[2]) <= 2019:
        df = pd.read_csv('http://web.mta.info/developers/' + weekly_link['href'])

        # add types & split DATE
        df["STATION"] = df["STATION"].astype(str)
        df["RIDERSHIP"] = df["ENTRIES"].astype(int)
        
        month = month_dict[date_split[1].split(" ")[0]]
        day = int(date_split[1].split(" ")[1])
        year = int(date_split[2])
        df["WEEK_START"] = datetime.datetime(year, month, day)

        def rs(x):
            return x.max() - x.min()

        rs.__name__ = 'RIDERSHIP'

        ridership_df = ridership_df.append(df[['STATION', 'RIDERSHIP', 'WEEK_START']].groupby(['STATION']).agg({'WEEK_START' : 'first', 'RIDERSHIP': rs, 'STATION' : 'first'}))

disk_engine = create_engine('sqlite:///final_project.db')
ridership_df.to_sql('old_ridership', disk_engine, if_exists='replace', index=False)

print(ridership_df.head())

