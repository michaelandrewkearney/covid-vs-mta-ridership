import logging
import pandas as pd
import os
import matplotlib.pyplot as plot
import datetime
import numpy as np
from scipy import stats
import sys
from time import process_time

col_names = {'initials': ['desc', 'dt', 'tdiff'], 'types': ['i', 'o'], 'bases': ['', 'diff', 'z', 'diffpt', 'zpt', 'diffsign', 'countingup'], 'suffixes': ['', '_raw']}
col_names = {'initials': ['desc', 'dt', 'tdiff'], 'types': ['i', 'o'], 'bases': ['', 'diff', 'diffpt', 'diffsign', 'countingup'], 'suffixes': ['', '_raw']}

def print_to_file(mystring, f, ogstdout=sys.stdout):
    sys.stdout = f
    print(mystring)
    sys.stdout = ogstdout

def is_long_enough(df, minlen=2):
    return len(df) >= minlen

def construct_column_list(initials=col_names['initials'], types=col_names['types'], bases=col_names['bases'], suffixes=col_names['suffixes']):
    if initials and types and bases and suffixes:
        return initials + [t+b+s for t in types for b in bases for s in suffixes]
    if initials and types and bases:
        return initials + [t+b for t in types for b in bases]
    if initials and types:
        return initials + [t for t in types]
    if initials:
        return initials
    return []

def get_thresh(df, type='i', quantile=0.95, multiplier=3):
    return df.loc[(df[f'{type}diffpt'] != 0), f'{type}diffpt'].copy().apply(np.abs).quantile(quantile)*multiplier

def get_buffered_boundaries(idxs=[], buffer=10, minindex=None, maxindex=None):
    if minindex is None:
        minindex = min(idxs)
    if maxindex is None:
        maxindex = max(idxs)
    boundaries = []
    idxs = sorted(list(idxs))
    while idxs:
        start = end = idxs.pop()
        while idxs and start - idxs[-1] <= buffer*2:
            start = idxs.pop()
        boundaries.append((max(start-buffer, minindex), start, end, min(end+buffer, maxindex)))
    return boundaries

def print_idxs_buffered(df, f, cols=None, idxlist=[], buffer=5, area='Unknown', scp='Unknown'):
    out = sys.stdout
    sys.stdout = f
    line = '*' * 100
    if cols is None:
        cols = df.columns
    if idxlist:
        print_to_file(line + '\n' + f'There are {len(idxlist)} selected indices in {area} / {scp} (len={len(df)}):\n' + line + '\n', f)
        for start_buffer, *_, end_buffer in get_buffered_boundaries(idxlist, buffer, minindex=df.index[0], maxindex=df.index[-1]):
            print_to_file(df.loc[start_buffer:end_buffer, cols], f)
            print_to_file('\n', f)
    else:
        print_to_file(line + '\n' + f'There are no selected indices in {area} / {scp} (len={len(df)}).\n' + line + '\n', f)
    print_to_file('\n\n', f)
    sys.stdout = out

def prepare_analysis_columns(df):
    # To make:
    # diff = (float) change in counter value from previous row
    # diffpt = (float) change in counter value from previous per unit time, where the unit is 4 hours
    # diffsign = {-1, 0, 1} sign of diff
    # countingup = {True, False} the row is part of a consecutive segment of monotonically increasing diffs.
    #   - Technically, this is the 'or' of df.col==1 for two columns: diffsign with 0 values bfilled, and diffsign with 0 values ffilled.
    #   - All negative diffs break a 'True' series and start a 'False' series until a positive diff or the first leading 0 of a positive diff.
    #   - Counter resets, spikes, and errors form a very short 'False' series (n < ~5), while negative counters form a larger series.
    # 'raw' suffixed columns store the initial value to be compared with processed values in processing analysis

    # These column names must match the col_names as defined at the beginning of the file
    df = df.dropna(subset=['i', 'o', 'dt'])
    df['tdiff'] = df['dt'] - df['dt'].shift(1, fill_value=df.at[0, 'dt']-pd.Timedelta(hours=4))
    for e in ['i', 'o']:
        df[f'{e}_raw'] = df[e]
        if is_long_enough(df, minlen=2):
            df[f'{e}diff'] = df[f'{e}diff_raw'] = df[e] - df[e].shift(1, fill_value=df.at[0, e])
            #df[f'{e}z'] = df[f'{e}z_raw'] = stats.zscore(df[f'{e}diff'])
            df[f'{e}diffpt'] = df[f'{e}diffpt_raw'] = df[f'{e}diff'] * 3600 * 4 / df['tdiff'].apply(lambda x: max(x.total_seconds(), 1))
            #df[f'{e}zpt'] = df[f'{e}zpt_raw'] = stats.zscore(df[f'{e}diffpt'])
            df[f'{e}diffsign'] = df[f'{e}diffsign_raw'] = np.sign(df[f'{e}diff']) # -1, 0, or 1
            df[f'{e}countingup'] = df[f'{e}countingup_raw'] = (df[f'{e}diffsign'].mask(df[f'{e}diffsign'] == 0).fillna(method='bfill') == 1) | (df[f'{e}diffsign'].mask(df[f'{e}diffsign'] == 0).fillna(method='ffill') == 1)
        else:
            df[f'{e}diff'] = df[f'{e}diff_raw'] = 0
            #df[f'{e}z'] = df[f'{e}z_raw'] = 0
            df[f'{e}diffpt'] = df[f'{e}diffpt_raw'] = 0
            #df[f'{e}zpt'] = df[f'{e}zpt_raw']= 0
            df[f'{e}diffsign'] = df[f'{e}diffsign_raw'] = 0
            df[f'{e}countingup'] = df[f'{e}countingup_raw'] = True
    cols = construct_column_list()
    for col in df.columns:
        assert col in cols
    return df

def reset_analysis_columns(df, ignore_cols=[], reset_e_from_diff=False, reset_diff_from_e=False, reset_tdiff=False):
    process_cols = {'diff', 'z', 'diffpt', 'zpt', 'diffsign', 'countingup'} - set(ignore_cols)
    process_cols = {'diff', 'diffpt', 'diffsign', 'countingup'} - set(ignore_cols)
    processed_cols = []
    if not 'tdiff' in df.columns or reset_tdiff:
        df['tdiff'] = df['dt'] - df['dt'].shift(1, fill_value=df.at[0, 'dt']-pd.Timedelta(hours=4))
    for e in ['i', 'o']:
        if reset_e_from_diff or 'diff' in process_cols:
            if reset_e_from_diff:
                df.loc[:, e] = df[f'{e}diff'].cumsum()
                processed_cols.append(e)
            elif reset_diff_from_e:
                df[f'{e}diff'] = df[e] - df[e].shift(1, fill_value=df.at[0, e])
                processed_cols.append(f'{e}diff')
            if f'{e}diff' in df.columns:
                # if 'z' in process_cols:
                #     df[f'{e}z'] = stats.zscore(df[f'{e}diff'])
                #     processed_cols.append(f'{e}z')
                if 'diffpt' in process_cols:
                    df[f'{e}diffpt'] = df[f'{e}diff'] * 3600 * 4 / df['tdiff'].apply(lambda x: max(x.total_seconds(), 1))
                    processed_cols.append(f'{e}diffpt')
                    # if 'zpt' in process_cols:
                    #     df[f'{e}zpt'] = stats.zscore(df[f'{e}diffpt'])
                    #     processed_cols.append(f'{e}zpt')
                if 'diffsign' in process_cols:
                    df[f'{e}diffsign'] = np.sign(df[f'{e}diff']) # -1, 0, or 1
                    processed_cols.append(f'{e}diffsign')
                    if 'countingup' in process_cols:
                        df[f'{e}countingup'] = (df[f'{e}diffsign'].mask(df[f'{e}diffsign'] == 0).fillna(method='bfill') == 1) | (df[f'{e}diffsign'].mask(df[f'{e}diffsign'] == 0).fillna(method='ffill') == 1)
                        processed_cols.append(f'{e}countingup')
    return df, processed_cols


def get_isol_falses(df, isol_series_max_len=10, col='icountingup'):
    offsets = []
    for series_len in range(1, isol_series_max_len+1):
        for offset in range(1, series_len+1):
            offsets.append((-offset, series_len+1-offset))
    mask = pd.Series(data=False, index=df.index)
    for upper, lower in offsets:
        mask = mask | (df[col].shift(upper, fill_value=True) & df[col].shift(lower, fill_value=True))
    mask = mask & ~df[col]
    return df, list(df.loc[mask].index)

def process_isol_negs(df):
    isol_negs_set = set()
    other_negs_set = set()
    jumps_set = set()
    dupes_set = set()
    for e in ['i', 'o']:
        df['tdiff'] = df['dt'] - df['dt'].shift(1, fill_value=df.at[0, 'dt']-pd.Timedelta(hours=4))
        df[f'{e}diff'] = df[e] - df[e].shift(1, fill_value=df.at[0, e])
        df[f'{e}diffpt'] = df[f'{e}diff'] * 3600 * 4 / df['tdiff'].apply(lambda x: max(x.total_seconds(), 1))

        # PROCESS ISOL NEGS
        # Set length of counting down sets
        df[f'{e}lastposidx'] = df[f'{e}nextposidx'] = df.reset_index()['index'].where(df[f'{e}countingup'], np.nan)
        if pd.isnull(df[f'{e}lastposidx'].iat[0]):
            df.iloc[0, df.columns.get_loc(f'{e}lastposidx')] = -1

        if pd.isnull(df[f'{e}nextposidx'].iat[-1]):
            df.iloc[-1, df.columns.get_loc(f'{e}nextposidx')] = df.index.size

        df[f'{e}lastposidx'] = df[f'{e}lastposidx'].ffill()
        df[f'{e}nextposidx'] = df[f'{e}nextposidx'].bfill()
        df[f'{e}negsetlen'] = df[f'{e}nextposidx'] - df[f'{e}lastposidx'] - 1

        #df, isol_negs_idxs = get_isol_falses(df, isol_series_max_len=3, col=f'{e}countingup')
        
        isol_negs_idxs = list(df[df[f'{e}negsetlen']>0 & df[f'{e}negsetlen']<10].index)

        thresh = get_thresh(df, e)
        minindex = df.index[0]
        maxindex = df.index[-1]

        while isol_negs_idxs:
            ni = isol_negs_idxs.pop()
            c = [df.loc[ni+x, e] if ni+x >= minindex and ni+x <= maxindex else None for x in [0, 1, 2, -2, -1]]
            if ni-2 >= minindex and c[0] >= c[-2]:
                # ni is the index after a positive spike...
                if np.abs(df.at[ni, f'{e}diffpt']) > thresh:
                    df.at[ni-1, e] = c[0] # Fill previous spike value with current normal value
                # ... or a small negative dip
                else:
                    df.at[ni, e] = c[-1]
            elif ni-1 >= minindex and ni+1 <= maxindex and c[1] >= c[-1]:
                # ni is the index of a negative spike, or a small negative dip
                #if np.abs(c[-1] - c[0]) * 3600 * 4 / max((df.at[ni, 'dt'] - df.at[ni-1, 'dt']).total_seconds(), 1) > thresh:
                df.at[ni, e] = c[-1] # Fill current spike value with previous normal value
                # ... or a small negative dip
                #else:
                #    df.at[ni, e] = c[-1]
            elif ni-1 >= minindex and c[0] * 3600 * 4 / max(df.at[ni, 'tdiff'].total_seconds(), 1) <= thresh:
                # Counter reset to near 0
                df.loc[ni:maxindex, e] += c[-1]
            elif ni-1 >= minindex:
                # Counter reset, but not to 0
                df.loc[ni:maxindex, e] += c[-1] - c[0]
            isol_negs_set.add(ni)

        df[f'{e}diff'] = df[e] - df[e].shift(1, fill_value=df.at[0, e])

        # PROCESS OTHER NEGS
        negs = set(df[df[f'{e}diff']<0].index)
        other_negs_set.update(negs)
        df[f'{e}diff'] = df[f'{e}diff'].apply(np.abs)
        df[f'{e}diffpt'] = df[f'{e}diff'] * 3600 * 4 / df['tdiff'].apply(lambda x: max(x.total_seconds(), 1))


        # PROCESS JUMPS
        thresh = get_thresh(df, type=e)
        jump_mask = ((df['tdiff'] <= pd.Timedelta('4H')) & (df[f'{e}diff'] > thresh)) | ((df['tdiff'] > pd.Timedelta('4H')) & (df[f'{e}diffpt'] > thresh))
        df.loc[jump_mask, f'{e}diff'] = 0
        jumps_set.update(set(df[jump_mask].index))

        df[f'{e}diffpt'] = df[f'{e}diff'] * 3600 * 4 / df['tdiff'].apply(lambda x: max(x.total_seconds(), 1))

    # PROCESS DUPLICATES
    dupes = set(df[df.duplicated(subset='dt')].index)
    if dupes:
        df = df.groupby('dt')[['desc', 'idiff', 'odiff']].agg({'desc': 'first', 'idiff': 'sum', 'odiff': 'sum'}).reset_index()
        df['tdiff'] = df['dt'] - df['dt'].shift(1, fill_value=df.at[0, 'dt']-pd.Timedelta(hours=4))
        for e in ['i', 'o']:
            df[f'{e}diffpt'] = df[f'{e}diff'] * 3600 * 4 / df['tdiff'].apply(lambda x: max(x.total_seconds(), 1))
    dupes_set.update(dupes)

    # RESAMPLE
    start = df.loc[(df['dt'].apply(lambda x: x.minute == 0) & df['dt'].apply(lambda x: x.second == 0)), 'dt'].min()
    minabs = df['dt'].min()
    if pd.isnull(start):
        start = minabs.floor('4H')
    while minabs < start:
        start -= pd.Timedelta('4H')
    end=df['dt'].max()
    df = df.set_index('dt')
    ogsize = df.index.size
    timeindex = pd.DataFrame(index=pd.date_range(start=start, end=end, freq='4H', name='dt'))
    df = df.combine_first(timeindex).rename_axis(index='dt')
    if not df.index.size == ogsize:
        df['tdiff'] = df['dt'] - df['dt'].shift(1, fill_value=df.at[0, 'dt']-pd.Timedelta(hours=4))
        for e in ['i', 'o']:
            df[f'{e}diffpt'] = df[f'{e}diffpt'].fillna(method='bfill')
            df[f'{e}diff'] = df[f'{e}diffpt'] * df['tdiff']
    df = df[['desc', 'idiff', 'odiff']].resample('4H', origin='start_day', offset='2H', closed='right', label='right').agg({'desc': 'first', 'idiff': 'sum', 'odiff': 'sum'})
    df.index = df.index + pd.tseries.frequencies.to_offset('-2H')
    df = df.reset_index()
    for e in ['i', 'o']:
        df[e] = df[f'{e}diff'].cumsum()
        df['i'] = df['i'].astype('int')
    return df



        



def process_negs(df):
    negs = []
    for e in ['i', 'o']:
        negs.extend(list(df[df[f'{e}diff'].lt(0)].index))
        df.loc[:, f'{e}diff'] = df[f'{e}diff'].apply(np.abs)
    df, processed_cols = reset_analysis_columns(df, reset_e_from_diff=True, ignore_cols=['diffsign', 'countingup'])
    for e in ['i', 'o']:
        assert df[e].is_monotonic
    return df, sorted(list(set(negs)))

def process_jumps(df):
    jumps = []
    for e in ['i', 'o']:
        thresh = get_thresh(df, type=e)
        jump_mask = ((df['tdiff'] <= pd.Timedelta('4H')) & (df[e] > thresh)) | ((df['tdiff'] > pd.Timedelta('4H')) & (df[f'{e}diffpt'] > thresh))
        df.loc[jump_mask, f'{e}diff'] = 0
        jumps.extend(list(df[jump_mask].index))
    df, processed_cols = reset_analysis_columns(df, ignore_cols=['z', 'diffpt', 'zpt', 'diffsign', 'countingup'])
    return df, sorted(list(set(jumps)))

def process_duplicates(df):
    dupes = list(df[df.duplicated(subset='dt')].index)
    if dupes:
        df = df.groupby('dt')[['desc', 'idiff', 'odiff']].agg({'desc': 'first', 'idiff': 'sum', 'odiff': 'sum'}).reset_index()
        df, _ = reset_analysis_columns(df, reset_tdiff=True, ignore_cols=['z', 'zpt', 'diffsign', 'countingup'])
    return df, sorted(dupes)

def process_resampling(df):
    


booth_map = pd.read_csv('../station_data/booth_to_complex.csv')
booth_map = booth_map.set_index('Booth')
booth_map = booth_map.dropna(subset=['Complex'])
booth_map['Complex'] = booth_map['Complex'].astype('int')


# icomplexes = {}
# ocomplexes = {}

# count = 0
# size = len(diffs.keys())

# diff_dfs = []

# diffs = {tuple(p.replace('.ftr', '').split('_')):pd.read_feather(f'diffed_areas/{p}') for p in os.listdir('diffed_areas') if p.endswith('.ftr')}
# diffs_size = len(diffs)

# with open('isol_negs.txt', 'w') as isol_negs_file, open('negs.txt', 'w') as negs_file, open('jumps.txt', 'w') as jumps_file, open('dupes.txt', 'w') as dupes_file, pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.expand_frame_repr', False):
#     count = 0
#     for (area, scp), df in diffs.items():
#         times = []
#         if count > 30:
#             break
#         assert df['dt'].is_monotonic, f'{area} / {scp} column \'dt\' is not monotonic.'
#         times.append(process_time())
#         df = prepare_analysis_columns(df)
#         if is_long_enough(df, minlen=2):
#             times.append(process_time())
#             df, isolneg_idxs = process_isol_negs(df)
#             print_idxs_buffered(df=df, f=isol_negs_file, cols=construct_column_list(), idxlist=isolneg_idxs, area=area, scp=scp)
#             times.append(process_time())
#             df, neg_idxs = process_negs(df)
#             print_idxs_buffered(df=df, f=negs_file, cols=construct_column_list(), idxlist=neg_idxs, area=area, scp=scp)
#             times.append(process_time())
#             df, jump_idxs = process_jumps(df)
#             print_idxs_buffered(df=df, f=jumps_file, cols=construct_column_list(), idxlist=jump_idxs, area=area, scp=scp)
#             times.append(process_time())
#             df, dupe_idxs = process_duplicates(df)
#             print_idxs_buffered(df=df, f=dupes_file, cols=construct_column_list(), idxlist=dupe_idxs, area=area, scp=scp)
#             times.append(process_time())
#             df = process_resampling(df)
#         times.append(process_time())
#         count += 1
#         print(f'{area} / {scp} processed: {count}/{diffs_size} or {count/diffs_size:.2%}. {[times[x]-times[x-1] for x in range(1, len(times))]}')
#         df.to_feather(f'processed_areas/{area}_{scp}.ftr')
    

        


# with open('neg_sets_out.txt', 'w') as f, pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.expand_frame_repr', False):
#     og_out = sys.stdout

#     stop = 0
#     for (area, scp), df in diffs.items():
#         if stop > 30:
#             break
#         if len(df) > 1:
#             og_size = df.index.size
#             df = df.dropna(subset=['i', 'o', 'dt'])
#             df['tdiff'] = df['dt'] - df['dt'].shift(1, fill_value=df.at[0, 'dt']-pd.Timedelta(hours=4))
#             minindex = df.index[0]
#             maxindex = df.index[-1]

#             for e in ['i', 'o']:
#                 print_to_file(f'*******************************\nArea: {area}\tSCP: {scp}\tType: {e}\n*******************************\n', f)

#                 df[f'{e}diff'] = df[e] - df[e].shift(1, fill_value=df.at[0, e])
#                 df[f'{e}diffpt'] = df[f'{e}diff'] * 3600 * 4 / df['tdiff'].apply(lambda x: x.total_seconds())
#                 df[f'{e}diffsign'] = df[f'{e}diff'].ge(0) # True if 0 or positive, else False
#                 df[f'{e}diffsigndiff'] = df[f'{e}diffsign'] != df[f'{e}diffsign'].shift(1, fill_value=df.at[0, f'{e}diffsign'])
#                 difflist = list(df[df[f'{e}diffsigndiff']].index)
                
                

#                 thresh = df[f'{e}diffpt'].apply(np.abs).quantile(0.95)*3

#                 for startbuffer, start, end, endbuffer in boundaries:
#                     compare = df.loc[startbuffer:endbuffer].copy()

#                     is_at_start = start == minindex
#                     is_at_end = end == maxindex
#                     slice = df.loc[start-int(~is_at_start):end+int(~is_at_end), [e, f'{e}diff', f'{e}diffpt']].copy()
#                     first = slice[e].iat[0]
#                     last = slice[e].iat[-1]
#                     maxi = slice[e].max()
#                     mini = slice[e].min()

#                     # Check for single sign change
#                     if start == end:
#                         if np.abs(slice.at[start, f'{e}diffpt']) > thresh:
#                             if slice.at[start, e] <= thresh: # if counter reset all the way to 0, preserve the first idiff
#                                 slice.at[start, f'{e}diff'] = slice.at[start, e]
#                             else:
#                                 slice.at[start, f'{e}diff'] = 0
#                         else:
#                             pass # don't do anything because it's part of a string of negatives that the buffer didn't catch
#                     elif last < first:
#                         top = slice[slice[e].ge(first)].copy()
#                         bottom = slice[slice[e].le(last)].copy()
#                         middle = slice[slice[e].lt(first) & slice[e].gt(last)].copy()
#                         slice = top.append(middle).append(bottom)
#                         slice.loc[:,f'{e}diff'] = slice[e] - slice[e].shift(1, fill_value=slice[e].iat[0]-slice[f'{e}diff'].iat[0])
#                     else:
#                         middle = slice[slice[e].ge(first) & slice[e].le(last)]
#                         top = slice[slice[e].gt(last)]
#                         bottom = slice[slice[e].lt(first)]
#                         for segment in [top, middle, bottom]:
#                             if not segment.empty:
#                                 segment.loc[:,f'{e}diff'] = segment[e] - segment[e].shift(1, fill_value=segment[e].iat[0]-segment[f'{e}diff'].iat[0])
#                         slice = middle.append(top).append(bottom)
                    
#                     compare[f'{e}diff_prime'] = slice[f'{e}diff']
#                     print_to_file(compare[['desc', 'dt', 'tdiff', e, f'{e}diff', f'{e}diff_prime', f'{e}diffsign', f'{e}diffsigndiff', f'{e}diffpt']], f)
#                     print_to_file('\n', f)
#                     df.update(slice[f'{e}diff'], overwrite=True)
#                 print_to_file('\n\n', f)
#             stop += 1
#         df.to_feather(f'negged_areas/{area}_{scp}.ftr')
#         print(f'{area}_{scp} done.')

#df['idiff'] = df['idiff'].apply(np.abs)



# # Remove duplicate datetime entries, prioritizing regular audit events (the df was sorted by 'desc' from 'REGULAR' to 'RECOVR AUD)
# df = df.sort_values(by=['dt', 'desc'], ascending=[True, False], ignore_index=True).drop_duplicates(subset=['dt'], ignore_index=True).drop('desc', axis=1).set_index('dt')

# # Convert to difference per hour instead of cumulative count
# df['idiff'] = df['i'].diff()
# df['odiff'] = df['o'].diff()

# window=3

# df['ithresh'] = df['idiff'].rolling(window, center=True).median() + 3 * df['idiff'].rolling(window, center=True).std().median()
# df['othresh'] = df['odiff'].rolling(window, center=True).median() + 3 * df['odiff'].rolling(window, center=True).std().median()


# df['ithresh'] = df['ithresh'] + (3 * df['idiff'].rolling(window).std().median())
# ostdmed = df['idiff'].rolling(window).std().median()
# df['othresh'] = df['odiff'].rolling(window).median() + (3 * ostdmed)

# df['idiff'] = df['idiff'].where((df['idiff']<df['ithresh']), 0)




# df = df.resample(rule='4H', closed=None, origin='start_day', label='right').nearest(limit=1).interpolate(method='time')


# window = 10




# # Fill with 4H freq datetime index
# df = df.set_index('dt').combine_first(pd.DataFrame(index=pd.date_range(start=df['dt'].min().floor('4H'), end=df['dt'].max().ceil('4H'), freq='4h', name='dt')))
# df[['idiff_ph', 'odiff_ph']] = df[['idiff_ph', 'odiff_ph']].fillna(method='bfill')
# df['tdiff'] = df['dt'].diff()
# df['tdiffprime'] = pd.DataFrame(df.index).set_index('dtidx', drop=False)['dtidx'].diff()
# df['iprime'] = df['tdiffprime'].dt.total_seconds() * df['idiff_ph'] / 3600
# df['oprime'] = df['tdiffprime'].dt.total_seconds() * df['odiff_ph'] / 3600
# df = df[['iprime', 'oprime', 'idiff_ph', 'odiff_ph']].resample('4H', origin='start_day').sum()
# df.loc[df['iprime'].lt(0) | df['iprime'].gt(50000) | df['idiff_ph'].gt(7200), ['iprime','oprime']] = 0
# df.loc[df['oprime'].lt(0) | df['oprime'].gt(50000) | df['odiff_ph'].gt(7200), ['iprime','oprime']] = 0

# # df = df.dropna(subset=['idiff', 'odiff', 'tdiff'])

# df = df.fillna({'idiff':0, 'odiff':0, 'idiff_ph':0, 'odiff_ph':0, 'tdiff':pd.Timedelta(0)})

# # Calculate local z-scores
# df['iz'] = np.abs(stats.zscore(np.abs(df['idiff_ph']), nan_policy='omit'))
# df['oz'] = np.abs(stats.zscore(np.abs(df['odiff_ph']), nan_policy='omit'))

# df = df.reset_index().rename({'index': 'area_index'}, axis=1, inplace=True)
# diff_dfs.append(df[['area', 'scp', 'index', 'dt', 'i', 'o', 'idiff', 'odiff', 'tdiff', 'idiff_ph', 'odiff_ph', 'iz', 'oz']])
# df.to_feather(f'diffed_areas/{area}_{scp}.ftr')
# print(f'{area}_{scp}: {idx+1} of {areas_len} done, or {(idx+1)/areas_len:%}')


#     area = name.split('_')[0]
#     if df.index.size > 1:
#         if area in booth_map.index:
#             complex = booth_map.loc[area, 'Complex']
#             # Fill based on backed-up diff_ph
#             df = df.set_index('dt').combine_first(pd.DataFrame(pd.date_range(start=df['dt'].min(), end=df['dt'].max(), freq='4h')).set_index(0))
#             df.index.rename('dtidx', inplace=True)
#             df[['idiff_ph', 'odiff_ph']] = df[['idiff_ph', 'odiff_ph']].fillna(method='bfill')
#             df['tdiffprime'] = pd.DataFrame(df.index).set_index('dtidx', drop=False)['dtidx'].diff()
#             df['iprime'] = df['tdiffprime'].dt.total_seconds() * df['idiff_ph'] / 3600
#             df['oprime'] = df['tdiffprime'].dt.total_seconds() * df['odiff_ph'] / 3600
#             df = df[['iprime', 'oprime', 'idiff_ph', 'odiff_ph']].resample('4H', origin='start_day').sum()
#             df.loc[df['iprime'].lt(0) | df['iprime'].gt(50000) | df['idiff_ph'].gt(7200), ['iprime','oprime']] = 0
#             df.loc[df['oprime'].lt(0) | df['oprime'].gt(50000) | df['odiff_ph'].gt(7200), ['iprime','oprime']] = 0
#             #diff.fillna(0)


#             # diff.loc[diff['idiff'].lt(0), 'idiff'] = 0
#             # diff.loc[diff['odiff'].lt(0), 'odiff'] = 0
#             # diff.loc[diff['idiff'].gt(diff_thresh) | diff['idiff_ph'].gt(ph_thresh), 'idiff'] = 0
#             # diff.loc[diff['odiff'].gt(diff_thresh) | diff['odiff_ph'].gt(ph_thresh), 'odiff'] = 0
#             # diff = diff.set_index('dt')
#             # diff = diff[['idiff', 'odiff']].resample('4H', origin='start_day').sum()

#             for e, d in [('i', icomplexes), ('o', ocomplexes)]:
#                 if complex in d:
#                     d[complex].append(df[f'{e}prime'])
#                 else:
#                     d[complex] = [df[f'{e}prime']]
#     count += 1
#     print(f'{name} done: {count}/{size} ({count/size:.2%})')


# print('Areas grouped. Concatenating DataFrames. This can take a minute...')

# # Concatenate diffed areas to calculate cumulative z-scores
# diffs = pd.concat(diff_dfs, ignore_index=True)
# diffs['cumiz'] = np.abs(stats.zscore(np.abs(diffs['idiff_ph'])))
# diffs['cumoz'] = np.abs(stats.zscore(np.abs(diffs['odiff_ph'])))
# diffs.to_feather('diffs.ftr')

# print('Dataframes concatenated. Done!')

# ins = []
# outs = []

# for e, d, a in [('i', icomplexes, ins), ('o', ocomplexes, outs)]:
#     for complex, diffs in d.items():
#         df = pd.concat(diffs, axis=1).fillna(0).sum(axis=1).rename(complex)
#         df.columns = [complex]
#         a.append(df)

# rin = pd.DataFrame()
# rout = pd.DataFrame()

# for a, r, l in [(ins, rin, 'in'), (outs, rout, 'out')]:
#     r = pd.concat(a, axis=1).fillna(0)
#     t = r.index.min()
#     if t.time() == datetime.time(4,0):
#         r.loc[t.floor('d')] = 0
#         r = r.sort_index()
#     r.reset_index().to_feather(f'ridership_{l}.ftr')