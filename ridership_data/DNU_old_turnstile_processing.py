import pandas as pd
import datetime
from datetime import date
from pandas.core.groupby.generic import generate_property
from sqlalchemy import create_engine

ridership = pd.read_csv('ridership_out.csv')
weeks = pd.read_csv('../covid_data/caserate.csv')
booths = pd.read_csv('../processed_data/booth_to_complex.csv')

weeks["WEEK"] = pd.to_datetime(weeks["week_ending"], infer_datetime_format=True)
weeks["YEAR"] = pd.to_datetime(weeks["week_ending"], infer_datetime_format=True)

weeks['WEEK'] = weeks[["WEEK"]].applymap(lambda x: x.date().isocalendar()[1])
weeks["YEAR"] = weeks[["YEAR"]].applymap(lambda x: x.date().isocalendar()[0])

#ridership['new_index'] = str(ridership['C/A']) + str(ridership['WEEK']) + str(ridership['YEAR'])
#ridership.set_index('new_index')

for booth in booths['Booth'].tolist():
    weeks[booth] = ridership.loc[str(booth)+str(weeks['WEEK'])+str(weeks['YEAR']), 'ENTRIES']

#- ridership.at[str(booth)+str(weeks['WEEK'])+str(weeks['YEAR'] - 1), 'ENTRIES']

print(weeks)





#disk_engine = create_engine('sqlite:///final_project.db')
#ridership_df.to_sql('ridership_by_booth', disk_engine, if_exists='replace', index=False)
