import logging
from iovars import *
import pandas as pd
import numpy as np
from scipy import stats
import sys

print('Reading feather. This can take a minute...')

try:
    log = pd.read_feather(event_log_name)
except IOError as e:
    logging.exception(f'File {event_log_name} could not be read or found. Scrape data and try again.')
    sys.exit(1)

log.columns=['area', 'unit', 'scp', 'station', 'lines', 'div', 'desc', 'i', 'o', 'dt']

print('Feather read. Acquiring areas. This can take a minute...')

grouped = log[['area', 'scp', 'desc', 'dt', 'i', 'o']].groupby(['area', 'scp'], sort=False)
areas = log[['area', 'scp']].drop_duplicates(ignore_index=True).sort_values(by=['area', 'scp'], ignore_index=True)
areas_len = areas.index.size

print('Areas acquired. Grouping by area. This can take a minute...')

# ? replaces 'i' or 'o', meaning entrances and exits, respectively
# '?diff' is the increase in turnstile count since the last audit event
# '?diff_ph' is the increase per hour in turnstile count since the last audit event
# '?z' is z-score for ?diff_ph, with a scope of that area and scp
# 'cumiz' is z-score for idiff_ph, with a system-wide scope

diff_dfs = []
for area in areas.itertuples(name=None):
	df = grouped.get_group((area[1], area[2])).sort_values(by=['dt', 'desc'], ascending=[True, False], ignore_index=True)
	og_size = df.index.size

	df = df.drop_duplicates(subset=['area', 'scp', 'dt'], ignore_index=True)

	# Convert to difference instead of cumulative count
	df['idiff'] = df['i'].diff()
	df['odiff'] = df['o'].diff()
	df['tdiff'] = df['dt'].diff()
	# df = df.dropna(subset=['idiff', 'odiff', 'tdiff'])

	# Calculate count diffs per hour
	df['idiff_ph'] = df['idiff'] * 3600 / df['tdiff'].dt.total_seconds()
	df['odiff_ph'] = df['odiff'] * 3600 / df['tdiff'].dt.total_seconds()

	df = df.fillna({'idiff':0, 'odiff':0, 'idiff_ph':0, 'odiff_ph':0, 'tdiff':pd.Timedelta(0)})

	# Calculate local z-scores
	df['iz'] = np.abs(stats.zscore(np.abs(df['idiff_ph']), nan_policy='omit'))
	df['oz'] = np.abs(stats.zscore(np.abs(df['odiff_ph']), nan_policy='omit'))

	df = df.reset_index().rename({'index': 'area_index'}, axis=1, inplace=True)
	diff_dfs.append(df[['area', 'scp', 'index', 'dt', 'i', 'o', 'idiff', 'odiff', 'tdiff', 'idiff_ph', 'odiff_ph', 'iz', 'oz']])
	df.to_feather(f'diffed_areas/{area[1]}_{area[2]}.ftr')
	print(f'{area[1]}/{area[2]}: {area[0]+1} of {areas_len} done, or {(area[0]+1)/areas_len:%}')

print('Areas grouped. Concatenating DataFrames. This can take a minute...')

# Concatenate diffed areas to calculate cumulative z-scores
diffs = pd.concat(diff_dfs, ignore_index=True)
diffs['cumiz'] = np.abs(stats.zscore(np.abs(diffs['idiff_ph'])))
diffs['cumoz'] = np.abs(stats.zscore(np.abs(diffs['odiff_ph'])))
diffs.to_feather('diffs.ftr')

print('Dataframes concatenated. Done!')
