# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
import pandas as pd

#TEST FILE, FINAL FILE IS get_covid_data.py

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


# %%
test = pd.read_csv('covid_data/caserate.csv')
print(test.columns)
test = test.T
test.columns = test.iloc[0]
print(test.shape)
test = test[7:]
test.index = map(lambda x: x.replace("CASERATE_", ""), test.index)
#print(test.head(10))



# %%
df = []
for irow, row in test.iterrows():
    for icol in range(0, len(row)):
        df.append([irow, row.index[icol], row[icol]])

df = pd.DataFrame(df)
df.columns = ["ZIP", "DATE", "PCT"]
df["PCT"] = df["PCT"].astype(float)
df["MONTH"] = df["DATE"].str[0:2].astype(int)
df["DAY"] = df["DATE"].str[3:5].astype(int)
df["YEAR"] = df["DATE"].str[6:].astype(int)

final = df[["ZIP", "MONTH", "DAY", "YEAR", "PCT"]]
final


# %%



