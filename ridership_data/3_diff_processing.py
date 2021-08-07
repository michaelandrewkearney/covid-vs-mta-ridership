import logging
import pandas as pd
import os
import matplotlib.pyplot as plot
import datetime


diffs = {p.replace('.ftr', ''):pd.read_feather(f'diffed_areas/{p}') for p in os.listdir('diffed_areas') if p.endswith('.ftr')}
booth_map = pd.read_csv('../station_data/booth_to_complex.csv')
booth_map = booth_map.set_index('Booth')
booth_map = booth_map.dropna(subset=['Complex'])
booth_map['Complex'] = booth_map['Complex'].astype('int').astype('str')


icomplexes = {}
ocomplexes = {}

count = 0
size = len(diffs.keys())

for name, diff in diffs.items():
    area = name.split('_')[0]
    if diff.index.size > 1:
        if area in booth_map.index:
            complex = booth_map.loc[area, 'Complex']
            # Fill based on backed-up diff_ph
            diff = diff.set_index('dt').combine_first(pd.DataFrame(pd.date_range(start=diff['dt'].min(), end=diff['dt'].max(), freq='4h')).set_index(0))
            diff.index.rename('dtidx', inplace=True)
            diff[['idiff_ph', 'odiff_ph']] = diff[['idiff_ph', 'odiff_ph']].fillna(method='bfill')
            diff['tdiffprime'] = pd.DataFrame(diff.index).set_index('dtidx', drop=False)['dtidx'].diff()
            diff['iprime'] = diff['tdiffprime'].dt.total_seconds() * diff['idiff_ph'] / 3600
            diff['oprime'] = diff['tdiffprime'].dt.total_seconds() * diff['odiff_ph'] / 3600
            diff = diff[['iprime', 'oprime', 'idiff_ph', 'odiff_ph']].resample('4H', origin='start_day').sum()
            diff.loc[diff['iprime'].lt(0) | diff['iprime'].gt(50000) | diff['idiff_ph'].gt(7200), ['iprime','oprime']] = 0
            diff.loc[diff['oprime'].lt(0) | diff['oprime'].gt(50000) | diff['odiff_ph'].gt(7200), ['iprime','oprime']] = 0
            #diff.fillna(0)


            # diff.loc[diff['idiff'].lt(0), 'idiff'] = 0
            # diff.loc[diff['odiff'].lt(0), 'odiff'] = 0
            # diff.loc[diff['idiff'].gt(diff_thresh) | diff['idiff_ph'].gt(ph_thresh), 'idiff'] = 0
            # diff.loc[diff['odiff'].gt(diff_thresh) | diff['odiff_ph'].gt(ph_thresh), 'odiff'] = 0
            # diff = diff.set_index('dt')
            # diff = diff[['idiff', 'odiff']].resample('4H', origin='start_day').sum()

            for e, d in [('i', icomplexes), ('o', ocomplexes)]:
                if complex in d:
                    d[complex].append(diff[f'{e}prime'])
                else:
                    d[complex] = [diff[f'{e}prime']]
    count += 1
    print(f'{name} done: {count}/{size} ({count/size:.2%})')

ins = []
outs = []

for e, d, a in [('i', icomplexes, ins), ('e', ocomplexes, outs)]:
    for complex, diffs in d.items():
        df = pd.concat(diffs, axis=1).fillna(0).sum(axis=1).rename(complex)
        df.columns = [complex]
        a.append(df)

rin = pd.DataFrame()
rout = pd.DataFrame()

for a, r, l in [(ins, rin, 'in'), (outs, rout, 'out')]:
    r = pd.concat(a, axis=1).fillna(0)
    t = r.index.min()
    if t.time() == datetime.time(4,0):
        r.loc[t.floor('d')] = 0
        r = r.sort_index()
    r.reset_index().to_feather(f'ridership_{l}.ftr')