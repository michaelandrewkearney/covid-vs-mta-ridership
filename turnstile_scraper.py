from datetime import date
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine

# TODO: Grab all the links on the Turnstile page 
ridership_page = requests.get("http://web.mta.info/developers/turnstile.html")
ridership_soup = BeautifulSoup(ridership_page.content, "html.parser")
weekly_links = ridership_soup.findAll('a', href = True)

ridership_df = pd.DataFrame(columns = ['STATION', 'DAY', 'MONTH', 'YEAR', 'ENTRIES', 'EXITS', 'DATE'])

for weekly_link in weekly_links: 
    date_split = [x.strip() for x in weekly_link.text.split(",")]
    if len(date_split) == 3 and int(date_split[2]) >= 2019:
        df = pd.read_csv('http://web.mta.info/developers/' + weekly_link['href'])

        # add types & split DATE
        df["STATION"] = df["STATION"].astype(str)
        df["ENTRIES"] = df["ENTRIES"].astype(int)
        df["EXITS"] = df['EXITS                                                               '].astype(int)
        df["MONTH"] = date_split[1].split(" ")[0]
        df["DAY"] = int(date_split[1].split(" ")[1])
        df["YEAR"] = int(date_split[2])
        
        ridership_df = ridership_df.append(df[['STATION', 'DAY', 'MONTH', 'YEAR', 'ENTRIES', 'EXITS', 'DATE']], ignore_index = True)

disk_engine = create_engine('sqlite:///final_project.db')
ridership_df.to_sql('ridership', disk_engine, if_exists='replace', index=False)

print(ridership_df.head())

