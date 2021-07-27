import pandas as pd
import sqlite3

#'data-by-modzcta.csv' taken from:
# https://github.com/nychealth/coronavirus-data/blob/master/totals/data-by-modzcta.csv

#Goals
"""
Covid Cases:
    Get csv from file and convert to Dataframe
    Clean data
    Formate data to be stored into the sql
"""

# Run in Main Directory - python percent_data/get_percent_data.py
data_path = 'percent_data\data-by-modzcta.csv'
sql_path = 'final_project.db'
data = pd.read_csv(data_path)

# New Dataframe 
df = []
df = pd.DataFrame([], columns = ["ZIP", "COVID_CASE_PERCENT"])

# There are two columns from the csv file we are interested in: MODIFIED_ZCTA, COVID_CASE_RATE
# MODIFIED_ZCTA is just the grouped ZIP that represents the geographic region we are interested in
# COVID_CASE_RATE is the number of COVID Cases per 100,000 people, we will be converting that to a percentage
df["ZIP"] = data["MODIFIED_ZCTA"].astype(str)
df["COVID_CASE_PERCENT"] = data["COVID_CASE_RATE"].astype(float)

# Since rate is per 100,000
# Converting it to percentage needs to divide the rate by 1000
# Rounded up to the 5th decimal place
df["COVID_CASE_PERCENT"] = df["COVID_CASE_PERCENT"].apply(lambda rate: round(rate / 1000, 5))
# print(df[:5])

# Stores the frame into the sql table
conn = sqlite3.connect(sql_path)
df.to_sql('covid_percent', conn, if_exists='replace', index=False)
conn.close()


# SQL statements used after for future analysis -> join for demographics and caserate anova test
# CREATE TABLE demo_covid_table3 as SELECT c.ZIP, COVID_CASE_PERCENT, MED_INCOME from covid_percent as c LEFT JOIN 
# (SELECT ZIP, CAST(ROUND(AVG(MED_INCOME)) AS INT) as MED_INCOME from incomes GROUP BY ZIP) as ic ON ic.ZIP = c.ZIP