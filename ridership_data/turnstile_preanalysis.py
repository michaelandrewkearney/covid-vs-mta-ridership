# IMPORTS 
import seaborn as sns 
import pandas as pd 
import matplotlib.pyplot as plt

df = pd.read_sql_table('ridership_per_station', 'sqlite:///final_project.db')
print("DATA")
print(df.head())

print("PLOTS")
sns.boxplot(y = df["RIDERSHIP"])
plt.show()

station_group = df.groupby('WEEK_START').agg({'WEEK_START' : 'first', 'RIDERSHIP' : 'sum'})
print(station_group.head(10))

sns.barplot(x = 'WEEK_START', 
            y = 'RIDERSHIP', 
            data = station_group)

plt.tight_layout()
plt.xticks(rotation = 90, fontsize = 1)
plt.show()

print("DESCRIPTION")
print(df.describe())
print(df[df.RIDERSHIP < 585000000].describe())



