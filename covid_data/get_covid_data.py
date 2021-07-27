import pandas as pd
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt

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

# tranpose for zips, set col and row headers, and cut off non zipcode entries
data = pd.read_csv('covid_data/caserate.csv').T
data.columns = data.iloc[0]
data.index = map(lambda x: x.replace("CASERATE_", ""), data.index)
data = data[7:]

# create dataframe for all all possible zip & week combinations
df = []
for irow, row in data.iterrows():
    for icol in range(0, len(row)):
        #SELECT * from covid_cases WHERE PCT < 513
        #filter out outliers 
        if (row[icol] < 513):
            df.append([irow, row.index[icol], row[icol]])

df = pd.DataFrame(df)

# add types for sql & split DATE
df.columns = ["ZIP", "DATE", "CASERATE"]
df["ZIP"] = df["ZIP"].astype(str)
df["CASERATE"] = df["CASERATE"].astype(float)
df["WEEK_START"] = pd.to_datetime(df["DATE"])
#df["MONTH"] = df["DATE"].str[0:2].astype(int)
#df["DAY"] = df["DATE"].str[3:5].astype(int)
#df["YEAR"] = df["DATE"].str[6:].astype(int)

# Remove DATE
#final = df[["ZIP", "MONTH", "DAY", "YEAR", "CASERATE"]]
final = df[["ZIP", "WEEK_START", "CASERATE"]]

print(final["CASERATE"].describe())

# Save to SQL database
disk_engine = create_engine('sqlite:///final_project.db')
final.to_sql('covid_cases', disk_engine, if_exists='replace', index=False)

#creating diagrams
#https://machinelearningmastery.com/quick-and-dirty-data-analysis-with-pandas
#https://medium.com/swlh/identify-outliers-with-pandas-statsmodels-and-seaborn-2766103bf67c

# box plot of the variable height
ax = sns.boxplot(df["CASERATE"])

# xtick, label, and title
plt.xticks(fontsize=14)
plt.xlabel('Caserate', fontsize=14)
plt.title('Distribution of Caserate', fontsize=20)

plt.show()

print(final.head())