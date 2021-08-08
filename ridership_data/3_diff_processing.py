import logging
import pandas as pd
import os
import matplotlib.pyplot as plot
import datetime
import numpy as np
from scipy import stats


diffs = {tuple(p.replace('.ftr', '').split('_')):pd.read_feather(f'diffed_areas/{p}') for p in os.listdir('diffed_areas') if p.endswith('.ftr')}
booth_map = pd.read_csv('../station_data/booth_to_complex.csv')
booth_map = booth_map.set_index('Booth')
booth_map = booth_map.dropna(subset=['Complex'])
booth_map['Complex'] = booth_map['Complex'].astype('int')


icomplexes = {}
ocomplexes = {}

count = 0
size = len(diffs.keys())

# ? replaces 'i' or 'o', meaning entrances and exits, respectively
# '?diff' is the increase in turnstile count since the last audit event
# '?diff_ph' is the increase per hour in turnstile count since the last audit event
# '?z' is z-score for ?diff_ph, with a scope of that area and scp
# 'cumiz' is z-score for idiff_ph, with a system-wide scope

diff_dfs = []

for (area, scp), test in diffs.items():
    if area in booth_map.index:
        break

area = 'A'
scp = '0'
test = pd.DataFrame()

og_size = test.index.size

# Remove duplicate datetime entries, prioritizing regular audit events (the df was sorted by 'desc' from 'REGULAR' to 'RECOVR AUD)
test = test.sort_values(by=['dt', 'desc'], ascending=[True, False], ignore_index=True).drop_duplicates(subset=['dt'], ignore_index=True).drop('desc', axis=1).set_index('dt')

# Convert to difference per hour instead of cumulative count
test['idiff'] = np.abs(test['i'].diff())
test['odiff'] = np.abs(test['o'].diff())

window=3

test['ithresh'] = test['idiff'].rolling(window, center=True).median() + 3 * test['idiff'].rolling(window, center=True).std().median()
test['othresh'] = test['odiff'].rolling(window, center=True).median() + 3 * test['odiff'].rolling(window, center=True).std().median()


test['ithresh'] = test['ithresh'] + (3 * test['idiff'].rolling(window).std().median())
ostdmed = test['idiff'].rolling(window).std().median()
test['othresh'] = test['odiff'].rolling(window).median() + (3 * ostdmed)

test['idiff'] = test['idiff'].where((test['idiff']<test['ithresh']), 0)




test = test.resample(rule='4H', closed=None, origin='start_day', label='right').nearest(limit=1).interpolate(method='time')


window = 10




# Fill with 4H freq datetime index
test = test.set_index('dt').combine_first(pd.DataFrame(index=pd.date_range(start=test['dt'].min().floor('4H'), end=test['dt'].max().ceil('4H'), freq='4h', name='dt')))
test[['idiff_ph', 'odiff_ph']] = test[['idiff_ph', 'odiff_ph']].fillna(method='bfill')
test['tdiff'] = test['dt'].diff()
test['tdiffprime'] = pd.DataFrame(test.index).set_index('dtidx', drop=False)['dtidx'].diff()
test['iprime'] = test['tdiffprime'].dt.total_seconds() * test['idiff_ph'] / 3600
test['oprime'] = test['tdiffprime'].dt.total_seconds() * test['odiff_ph'] / 3600
test = test[['iprime', 'oprime', 'idiff_ph', 'odiff_ph']].resample('4H', origin='start_day').sum()
test.loc[test['iprime'].lt(0) | test['iprime'].gt(50000) | test['idiff_ph'].gt(7200), ['iprime','oprime']] = 0
test.loc[test['oprime'].lt(0) | test['oprime'].gt(50000) | test['odiff_ph'].gt(7200), ['iprime','oprime']] = 0

# df = df.dropna(subset=['idiff', 'odiff', 'tdiff'])

test = test.fillna({'idiff':0, 'odiff':0, 'idiff_ph':0, 'odiff_ph':0, 'tdiff':pd.Timedelta(0)})

# Calculate local z-scores
test['iz'] = np.abs(stats.zscore(np.abs(test['idiff_ph']), nan_policy='omit'))
test['oz'] = np.abs(stats.zscore(np.abs(test['odiff_ph']), nan_policy='omit'))

test = test.reset_index().rename({'index': 'area_index'}, axis=1, inplace=True)
diff_dfs.append(test[['area', 'scp', 'index', 'dt', 'i', 'o', 'idiff', 'odiff', 'tdiff', 'idiff_ph', 'odiff_ph', 'iz', 'oz']])
test.to_feather(f'diffed_areas/{area}_{scp}.ftr')
print(f'{area}_{scp}: {idx+1} of {areas_len} done, or {(idx+1)/areas_len:%}')


    area = name.split('_')[0]
    if test.index.size > 1:
        if area in booth_map.index:
            complex = booth_map.loc[area, 'Complex']
            # Fill based on backed-up diff_ph
            test = test.set_index('dt').combine_first(pd.DataFrame(pd.date_range(start=test['dt'].min(), end=test['dt'].max(), freq='4h')).set_index(0))
            test.index.rename('dtidx', inplace=True)
            test[['idiff_ph', 'odiff_ph']] = test[['idiff_ph', 'odiff_ph']].fillna(method='bfill')
            test['tdiffprime'] = pd.DataFrame(test.index).set_index('dtidx', drop=False)['dtidx'].diff()
            test['iprime'] = test['tdiffprime'].dt.total_seconds() * test['idiff_ph'] / 3600
            test['oprime'] = test['tdiffprime'].dt.total_seconds() * test['odiff_ph'] / 3600
            test = test[['iprime', 'oprime', 'idiff_ph', 'odiff_ph']].resample('4H', origin='start_day').sum()
            test.loc[test['iprime'].lt(0) | test['iprime'].gt(50000) | test['idiff_ph'].gt(7200), ['iprime','oprime']] = 0
            test.loc[test['oprime'].lt(0) | test['oprime'].gt(50000) | test['odiff_ph'].gt(7200), ['iprime','oprime']] = 0
            #diff.fillna(0)


            # diff.loc[diff['idiff'].lt(0), 'idiff'] = 0
            # diff.loc[diff['odiff'].lt(0), 'odiff'] = 0
            # diff.loc[diff['idiff'].gt(diff_thresh) | diff['idiff_ph'].gt(ph_thresh), 'idiff'] = 0
            # diff.loc[diff['odiff'].gt(diff_thresh) | diff['odiff_ph'].gt(ph_thresh), 'odiff'] = 0
            # diff = diff.set_index('dt')
            # diff = diff[['idiff', 'odiff']].resample('4H', origin='start_day').sum()

            for e, d in [('i', icomplexes), ('o', ocomplexes)]:
                if complex in d:
                    d[complex].append(test[f'{e}prime'])
                else:
                    d[complex] = [test[f'{e}prime']]
    count += 1
    print(f'{name} done: {count}/{size} ({count/size:.2%})')


print('Areas grouped. Concatenating DataFrames. This can take a minute...')

# Concatenate diffed areas to calculate cumulative z-scores
diffs = pd.concat(diff_dfs, ignore_index=True)
diffs['cumiz'] = np.abs(stats.zscore(np.abs(diffs['idiff_ph'])))
diffs['cumoz'] = np.abs(stats.zscore(np.abs(diffs['odiff_ph'])))
diffs.to_feather('diffs.ftr')

print('Dataframes concatenated. Done!')

ins = []
outs = []

for e, d, a in [('i', icomplexes, ins), ('e', ocomplexes, outs)]:
    for complex, diffs in d.items():
        test = pd.concat(diffs, axis=1).fillna(0).sum(axis=1).rename(complex)
        test.columns = [complex]
        a.append(test)

rin = pd.DataFrame()
rout = pd.DataFrame()

for a, r, l in [(ins, rin, 'in'), (outs, rout, 'out')]:
    r = pd.concat(a, axis=1).fillna(0)
    t = r.index.min()
    if t.time() == datetime.time(4,0):
        r.loc[t.floor('d')] = 0
        r = r.sort_index()
    r.reset_index().to_feather(f'ridership_{l}.ftr')