from logging import info
from iovars import *
import pandas as pd
import numpy as np
from matplotlib import pyplot
import time
from tabulate import tabulate
import sys
from sqlalchemy import create_engine

try:
    log = pd.read_feather(event_log_name)
except IOError as e:
    logging.exception(f'File {event_log_name} could not be read or found. Scrape data and try again.')
    sys.exit(1)


areas = log[['CONTROLAREA', 'SCP']].drop_duplicates(ignore_index=True).sort_values(by=['CONTROLAREA', 'SCP'], ignore_index=True)
areas_len = len(areas.index)

locals = [ ('LMIN', lambda x: (x.shift(1) > x) & (x.shift(-1) > x)),
           ('LMAX', lambda x: (x.shift(1) < x) & (x.shift(-1) < x))]

header = ['AREA', 'SCP', 'Cnt','DtMin', 'DtMax', 'OffstCnt', 'DupeCnt', 'CielDCnt', 'IMin', 'ILminC', 'IMax', 'ILmaxC', 'IMon', 'OMin', 'OLminC', 'OMax', 'OLmaxC', 'Omon']
metadata = pd.DataFrame(None, columns=header)

stop = 0
for area in areas.itertuples(name=None):
    if stop > 5:
        break

    start = time.process_time()

    df = log.loc[(log['CONTROLAREA'] == area[1]) & (log['SCP'] == area[2]), ['DATETIME', 'ENTRIES', 'EXITS']].sort_values(by='DATETIME', ignore_index=True)
    for s, f in locals:
        for e in ['ENTRIES', 'EXITS']:
            df[f'{e}{s}'] = f(df[e])
    mask = df['DATETIME'].apply(lambda x: (x.minute, x.second, x.microsecond)) != (0,0,0)
    metadata.append([area[1],
            area[2],
            df.index.size,
            df['DATETIME'].min(),
            df['DATETIME'].max(),
            mask.sum(),
            df.duplicated(subset='DATETIME').sum(),
            df.loc[mask, 'DATETIME'].apply(lambda x: x.ceil('H')).append(df.loc[~mask, 'DATETIME']).duplicated().sum(),
            df['ENTRIES'].min(),
            df['ENTRIESLMIN'].sum(),
            df['ENTRIES'].max(),
            df['ENTRIESLMAX'].sum(),
            df['ENTRIES'].is_monotonic,
            df['EXITS'].min(),
            df['EXITSLMIN'].sum(),
            df['EXITS'].max(),
            df['EXITSLMAX'].sum(),
            df['EXITS'].is_monotonic])
    df.set_index('DATETIME')[['ENTRIES', 'EXITS']].plot(kind='line', title=f'Ridership at {area[1]}/{area[2]}').get_figure().savefig(f'test/{area[1]}_{area[2]}.png')
    pyplot.close()
    df.to_feather(f'test/{area[1]}_{area[2]}.ftr')
    

    mid = time.process_time()

    df = log.loc[(log['CONTROLAREA'] == area[1]) & (log['SCP'] == area[2]), ['DATETIME', 'ENTRIES', 'EXITS']].sort_values(by='DATETIME', ignore_index=True)
    for s, f in locals:
        for e in ['ENTRIES', 'EXITS']:
            df[f'{e}{s}'] = f(df[e])
    mask = df['DATETIME'].apply(lambda x: (x.minute, x.second, x.microsecond)) != (0,0,0)
    df['DATETIME'] = df['DATETIME'].apply(lambda x: x.ceil('H'))
    df.set_index('DATETIME')[['ENTRIES', 'EXITS']].plot(kind='line', title=f'Ridership at {area[1]}/{area[2]}').get_figure().savefig(f'test/{area[1]}_{area[2]}.png')
    pyplot.close()
    df.to_feather(f'test/{area[1]}_{area[2]}.ftr')

    end = time.process_time()

    print(f'{area[0]+1}/{areas_len} or {(area[0]+1)/areas_len:%} complete. {mid-start} vs. {end-mid}.')
    stop += 1
    

metadata.to_csv('test/log.csv', index=False)



# # TODO: Remove duplicates by DATETIME, CONTROLAREA, SCP
# index_subset=['DATETIME', 'CONTROLAREA', 'SCP']
# log['DESC'] = pd.Categorical(log['DESC'], categories=['REGULAR', 'RECOVR AUD'], ordered=True)
# log = log.sort_values(by=['DESC'])

# m = log['DESC'] == 'REGULAR'
# log = log[m].append(log[~m])

# m_all = log.duplicated(subset=index_subset, keep=False)
# m_rem = log.duplicated(subset=index_subset, keep='first')
# dup_log = pd.merge(log.loc[m_all], pd.Series(m_all ^ m_rem, name='KEPT'), how='left', left_index=True, right_index=True, sort=False)
# dup_log = dup_log.reset_index()
# dup_log = dup_log.sort_values(by=index_subset + ['KEPT'], ascending=[True for i in index_subset] + [False], ignore_index=True) # Sort so duplicates are adjacent
# dup_log.to_feather(duplicate_log_name) # Write duplicate_log to output file
# log = log.loc[~m_rem] # Remove non-first duplicate rows from event_log

# # Reindex and pivot: DATETIME vs CONTROLAREA & SCP
# max = max(log['DATETIME'])
# min = min(log['DATETIME'])

# mi = pd.MultiIndex.from_frame(log[['CONTROLAREA', 'SCP']].drop_duplicates(ignore_index=True).sort_values(by=['CONTROLAREA', 'SCP'], ignore_index=True))
# dti = pd.date_range(start=min, end=max, freq='H', name='dt') # Constructing DatetimeIndex using scope of event_log
# table = pd.DataFrame(data=None, index=dti, columns=mi, dtype='I')

# #for r in log.itertuples(index=False, name=None):


# d = pd.date_range(start=min(log['DATETIME']), stop=max(log['DATETIME']), freq='H'
# log = log['DATETIME', 'TCODE', 'TURNS']

# table = pd.pivot_table(event_log, values='TURNS', index='DATETIME', columns=['CONTROLAREA', 'SCP'], aggfunc=np.sum, observed=True)


# table = event_log.pivot(index='DATETIME', columns='TCODE', values='TURNS')


# print(table)
# # TODO: Fill gaps
# # TODO: Sum by CONTROLAREA
# # TODO: Read out to ftr


#     OLF STUFF:


# reindex by datetime, with control area and scp as columns
# find out how many gaps and how big are the gaps
# fill in NaN with previous entry of previous datetime
# sum by control area and remove multiindex columns
# read out to csv


# # OLD BELOW

# ridership = pd.read_csv('ridership_out.csv')
# weeks = pd.read_csv('../covid_data/caserate.csv')
# booths = pd.read_csv('../processed_data/booth_to_complex.csv')

# weeks["WEEK"] = pd.to_datetime(weeks["week_ending"], infer_datetime_format=True)
# weeks["YEAR"] = pd.to_datetime(weeks["week_ending"], infer_datetime_format=True)

# weeks['WEEK'] = weeks[["WEEK"]].applymap(lambda x: x.date().isocalendar()[1])
# weeks["YEAR"] = weeks[["YEAR"]].applymap(lambda x: x.date().isocalendar()[0])

# #ridership['new_index'] = str(ridership['C/A']) + str(ridership['WEEK']) + str(ridership['YEAR'])
# #ridership.set_index('new_index')

# for booth in booths['Booth'].tolist():
#     weeks[booth] = ridership.loc[str(booth)+str(weeks['WEEK'])+str(weeks['YEAR']), 'ENTRIES']

# #- ridership.at[str(booth)+str(weeks['WEEK'])+str(weeks['YEAR'] - 1), 'ENTRIES']



# print(weeks)





#disk_engine = create_engine('sqlite:///final_project.db')
#ridership_df.to_sql('ridership_by_booth', disk_engine, if_exists='replace', index=False)
