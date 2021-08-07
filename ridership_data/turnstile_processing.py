import os
import pandas as pd
import datetime
from datetime import date
from pandas.core.base import DataError
from pandas.core.groupby.generic import generate_property
from sqlalchemy import create_engine

path = 'weekly_data/'
csvs = [pd.read_csv(path + f) for f in os.listdir(path) if path.isfile(path + f) and f.endswith('.csv')]

log = pd.DataFrame()
log.concat(csvs)

log = log.rename(columns={'C/A': 'CONTROLAREA'}) # Rename C/A column to CONTROLAREA
log[['CONTROLAREA', 'UNIT', 'SCP', 'STATION', 'DIVISION', 'DESC']] = log[['CONTROLAREA', 'UNIT', 'SCP', 'STATION', 'DIVISION', 'DESC']].astype('category')
log['LINENAME'] = log['LINENAME'].astype('str')
log[['ENTRIES', 'EXITS']] = log[['ENTRIES', 'EXITS']].astype('int')
log['DATETIME'] = pd.to_datetime(log['DATE'] + ' ' + log['TIME'], infer_datetime_format=True) # Smartly merge DATE and TIME
log = log.drop(['DATE', 'TIME'], axis=1) # Drop DATE and TIME columns, which are strings and have been merged into DATETIME



# Check for duplicates
duplicates = log[log.duplicated(['DATETIME', 'CONTROLAREA', ], keep=False)].sort_values(by=['CONTROLAREA', 'SCP', 'DATETIME'])

look for duplicate control area, scp, and datetimes
report on that

reindex by datetime, with control area and scp as columns
find out how many gaps and how big are the gaps
fill in NaN with previous entry of previous datetime
    if there is no previous datetime, it should be the next datetime,
    but we should check for new services/stations since 2014 by examining the size of the gaps
sum by control area and remove multiindex columns
read out to csv


###

duplicates = log[log.duplicated(['DATETIME', 'CONTROLAREA'], keep=False)].sort_values(by=['CONTROLAREA', 'DATETIME'])

print(f'The shape of log is {log.shape()}.')
print(f'The shape of the duplicates in log is {duplicates.shape()}. Here are some values:')
print(duplicates)
#pivot = log.pivot(index='DATETIME', columns='CONTROLAREA', values=['ENTRIES', 'EXITS') # Pivot into table with DATETIME index and CONTROLAREA columns
#pivot.to_csv('../processed_data/ridership_table.csv')


# OLD BELOW

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
