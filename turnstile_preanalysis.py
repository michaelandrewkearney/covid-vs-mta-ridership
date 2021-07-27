# IMPORTS 
import seaborn as sns 
import pandas as pd 
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from matplotlib import rcParams
import plotly.express as px
from urllib.request import urlopen
import json

df = pd.read_sql_table('the_actual_coolest_table', 'sqlite:///final_project.db')

def percent_change(row): 
    final = row['RIDERSHIP2']
    initial = row['RIDERSHIP1']
    return 100 * (final - initial) / ((final + initial) / 2)


with urlopen('https://data-beta-nyc-files.s3.amazonaws.com/resources/6df127b1-6d04-4bb7-b983-07402a2c3f90/f4129d9aa6dd4281bc98d0f701629b76nyczipcodetabulationareas.geojson?Signature=Cgpzk%2FLRH2OybKfTH9ZVpy9tbE8%3D&Expires=1627413384&AWSAccessKeyId=AKIAWM5UKMRH2KITC3QA') as response:
    nyc = json.load(response)

coolest_df = df.groupby('STATION').agg({'STATION' : 'first', 'RIDERSHIP1' : 'sum', 'RIDERSHIP2' : 'sum', 'ZIP' : 'first'})
coolest_df['Percent Change (Ridership)'] = coolest_df.apply(percent_change, axis = 1)

fig = px.choropleth(coolest_df, 
                    geojson = nyc, 
                    locations = "ZIP",
                    color = "Percent Change (Ridership)", 
                    hover_name = "STATION", 
                    featureidkey="properties.postalCode", 
                    projection = "mercator")

fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()



# df = pd.read_sql_table('ridership_per_station', 'sqlite:///final_project.db')
# print("DATA")
# print(df.head())

# print("PLOTS")
# sns.boxplot(y = df["RIDERSHIP"])
# plt.show()

# week_group = df.groupby('WEEK_START').agg({'WEEK_START' : 'first', 'RIDERSHIP' : 'sum'})
# print(week_group.head(10))

# sns.barplot(x = 'WEEK_START', 
#             y = 'RIDERSHIP', 
#             data = week_group)

# plt.tight_layout()
# plt.xticks(rotation = 90, fontsize = 1)
# plt.show()

# print("DESCRIPTION")
# print(df.describe())
# print(df[df.RIDERSHIP < 585000000].describe())

# cool_df = pd.read_sql_table('cool_table', 'sqlite:///final_project.db')               
# cool_df_old = pd.read_sql_table('cool_table_old_data', 'sqlite:///final_project.db')


# cool_station_group = cool_df.groupby('STATION') \
#                             .agg({'STATION' : 'first', 'RIDERSHIP' : 'sum', 'RIDERSHIP:1' : 'sum'})

# cool_station_group['Percent Change'] = cool_station_group.apply(percent_change, axis = 1)
# print(cool_station_group.sort_values('Percent Change', ascending = False).head())

# cool_station_group_old = cool_df_old.groupby('STATION') \
#                             .agg({'STATION' : 'first', 'RIDERSHIP' : 'sum', 'RIDERSHIP:1' : 'sum'})

# cool_station_group_old['Percent Change Old'] = cool_station_group_old.apply(percent_change, axis = 1)
# print(cool_station_group_old.sort_values('Percent Change Old', ascending = False).head())

# cool_df = cool_station_group.join(cool_station_group_old[['STATION', 'Percent Change Old']], lsuffix='_new', rsuffix='_old')
# print(cool_df.head())

# ax = sns.barplot(x = 'STATION_new', 
#             y = 'Percent Change Old', 
#             data = cool_df.sort_values('Percent Change'))
# plt.xticks(rotation = 90, fontsize = 1)
# ax.set_title('Percent Change in Ridership from 2018 - 2019 in NYC Stations')
# plt.show()


# disk_engine = create_engine('sqlite:///final_project.db')
# cool_station_group.to_sql('cool_table_old_data', disk_engine, if_exists='replace', index=False)
