import pandas as pd

#DATA:
#https://github.com/nychealth/coronavirus-data/blob/master/trends/caserate-by-modzcta.csv

#Goals
"""
Covid Cases:
    Get csv from file
    Use Pandas get transpose of csv
    Clean data
    Fix headers
    New dataframe where a new row for each week col
    Export to sql
"""

data = pd.read_csv('covid_data/caserate.csv').T
data.columns = data.iloc[0]
data.index = map(lambda x: x.replace("CASERATE_", ""), data.index)
data = data[7:]

df = []
for irow, row in data.iterrows():
    for icol in range(0, len(row)):
        df.append([irow, row.index[icol], row[icol]])

df = pd.DataFrame(df)

df.columns = ["ZIP", "DATE", "PCT"]
df["PCT"] = df["PCT"].astype(float)
df["MONTH"] = df["DATE"].str[0:2].astype(int)
df["DAY"] = df["DATE"].str[3:5].astype(int)
df["YEAR"] = df["DATE"].str[6:].astype(int)

final = df[["ZIP", "MONTH", "DAY", "YEAR", "PCT"]]
print(final.head())