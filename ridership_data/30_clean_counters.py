from numpy.lib import index_tricks
import pandas as pd
import os
import numpy as np
from pandas.core.algorithms import diff, value_counts
import turnstile

def process():
    areas = turnstile.get_scp_diffs_by_area(cols=['dt', 'desc', 'i', 'o'])
    for area in areas:
        for scp in areas[area]:
            df = areas[area][scp]
            df = df.sort_values(by=['dt', 'desc'])
            df = turnstile.make_cols(df, all=True)
            

    # Sort by dt, desc
    # Cast monotonic, isolated, non-duplicate RECOVRs to REGULARs (make sure to flip negset RECOVRs)
    # Sort duplicates and RECOVR sets
        # Remove outside sets to new scp
    # remove spikes to own scp
    # assert no duplicates remain
    # reindex to dt, discard desc
    # for area in areas:
    #   from smallest scp to largest
    #       from largest to smallest:
    #           check if small index fits in big index
    #           check if small and large do not overlap
    #           check if small in large is monotonic
    #           if so, add small to large
    # discard too small scps
    # flip negsets
    # fix negative resets
    # index scps together
    # interpolate nans
    # reindex to 4hr midnight-origin
    # add



def get_single_dates():
    dates = {}
    scp_diffs = turnstile.get_scp_diffs_by_area()
    for area in scp_diffs:
        for scp in scp_diffs[area]:
            if len(scp_diffs[area][scp]) < 10:
                s = set(scp_diffs[area][scp]['dt'])
                if not area in dates:
                    dates[area] = []
                dates[area].update(s)
    return dates
    
def process_duplicates(areas=turnstile.get_scp_diffs_by_area(cols=['dt', 'desc', 'i', 'o'])):
    for area in areas:
        for scp in areas[area]:
            df = areas[area][scp]
            idxs = df[df.duplicated(subset='dt', keep=False)].index
            boundaries = turnstile.get_buffered_boundaries(idxs=list(idxs), minindex=df.index.min(), maxindex=df.index.max())
            for sb, s, e, eb in boundaries:
                segment = df.loc[sb:eb].copy()
                segment['c'] = segment['i'] + segment ['o']
                before = segment.at[s, 'c'] if sb==s else segment.at[s-1, 'c']
                after = segment.at[e, 'c'] if eb==e else segment.at[e+1, 'c']
                sliver = segment.loc[s:e].copy()
                idx = sliver.index.copy()
                if before <= after:
                    sliver = sliver.sort_values(by='c')
                else:
                    top = sliver[sliver['c'] >= before].sort_values(by='c').copy()
                    bottom = sliver[sliver['c'] < before].sort_values(by='c').copy()
                    sliver = top.append(bottom)
                sliver.index = idx
                segment.update(sliver)
                to_print = segment.join(df.loc[sb:eb].copy(), r_suffix='_raw')
                print(to_print)
                segment = segment.drop('c', axis=1)
                df.update(segment)
            areas[area][scp] = df

def process_scp_sets():




    diffs = turnstile.get_scp_diffs_by_area()
    for area in diffs:
        for scp in diffs[area]:
            df = diffs[area][scp]
            df = df.set_index(['dt', 'desc'])
            diffs[area][scp] = df
    min_fill_diff_size = 10
    for area in diffs:
        for scp1 in diffs[area]:
            if len(diffs[area][scp1]) <= min_fill_diff_size:
                idxs = list(diffs[area][scp1].index.droplevel(level=1))
                for scp2 in diffs[area]:
                    if len(diffs[area][scp2]) > min_fill_diff_size:
                        for idx in idxs:
                            if not idx in diffs[area][scp2].index.droplevel(level=1) and :






    for each len1 diff:
        if there is one and only one nan value at that datatime in the other diffs not of len1 and the value is between the existing values:
            fill in that value
            delete the len1 diff



    for datetime in 




    scp_diffs = turnstile.get_scp_diffs_by_area()
    for area in scp_diffs:
        for scp in scp_diffs[area]:
            if len(scp_diffs[area][scp]) == 1:
                if all other scps are not na at that dt index:





    combined_diffs_by_area = turnstile.get_combined_diffs_by_area()
    for area in combined_diffs_by_area:
        timestamp_of_single_diffs = set()
        for col in combined_diffs_by_area[area].columns:
            if combined_diffs_by_area[area][col].notna().sum() == 0:
            if combined_diffs_by_area[area][col].notna().sum() == 1:
                get index of the value in that column
                if there are no 

                combined_diffs_by_area[area][col].index
    

    
    if there are no NaNs where there is a single scp diff, delete that scp diff
    else: fill in the nans with the scp diff value if it is between the value before and after
    if there is no regular audit where there is a recovery audit, make it a regular audit


    dfs = []
    for df in dfs:
        df.groupby('dt').sum()
    idx = dfs[0].index
    for df in dfs:
        idx = idx.union(df.index)
    for df in dfs:

    get master index
    reindex each scp in group with existing min and max, fill 0
    add scp groups together
    pass

def process_diffs(diffs=turnstile.get_diffs()):

    with open(turnstile.negset_output_file, 'w') as neg_set_file, open(turnstile.isoneg_output_file, 'w') as iso_neg_file, pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.expand_frame_repr', False):

        for k, df in diffs.items():

            ### MAKE COLS
            df = turnstile.make_cols(df, all=True)

            ### NEGATE NEGSETS
            neg_set_idxs = set()
            for e in turnstile.io:
                neg_set_mask = df[f'{e}negsetlen'].gt(turnstile.min_negset_len)
                neg_set_idxs.update(list(df[neg_set_mask].index))
                df[f'{e}diff'] = df[f'{e}diff'].mask(neg_set_mask, -df[f'{e}diff'])
            df = turnstile.make_cols(df, cumsum=True, all=True)

            for startbuffer, *_, endbuffer in turnstile.get_buffered_boundaries(idxs=neg_set_idxs, buffer=4, minindex=df.index.min(), maxindex=df.index.max()):
                turnstile.print_to_file(f'{k}:', neg_set_file)
                turnstile.print_to_file(df.loc[startbuffer:endbuffer, ['dt', 'tdiff', 'iraw', 'i', 'idiff', 'idiffsign', 'icountingup', 'oraw', 'o', 'odiff', 'odiffsign', 'ocountingup']], neg_set_file)

            ### FIX ISOLATED NEGS
            iso_neg_idxs = set()
            minindex = df.index[0]
            maxindex = df.index[-1]

            for e in turnstile.io:
                iso_neg_mask = df[f'{e}diffsign'].lt(0)
                idxs = sorted(list(df[iso_neg_mask].index))
                iso_neg_idxs.update(idxs)

                while idxs:
                    idx = idxs.pop()
                    c = [df.loc[idx+x, e] if idx+x >= minindex and idx+x <= maxindex else None for x in [0, 1, 2, -2, -1]]
                    if idx-2 >= minindex and c[0] >= c[-2]:
                        # ni is the index after a positive spike...
                        if c[-1] > c[1]:
                            df.at[idx-1, e] = c[0] # Fill previous spike value with current normal value
                        # ... or a small negative dip
                        else:
                            df.at[idx, e] = c[-1] # Fill current dip value with previous normal value
                    elif idx-1 >= minindex and idx+1 <= maxindex and c[1] >= c[-1]:
                        # ni is the index of a negative spike, or a small negative dip
                        df.at[idx, e] = c[-1] # Fill current spike value with previous normal value
                    elif idx-1 >= minindex and c[0] * 3600 * 4 / max(df.at[idx, 'tdiff'].total_seconds(), 1) <= turnstile.get_rolled_thresh(df[f'{e}diff'], idx, mask=~iso_neg_mask):
                        # Counter reset to near 0
                        df.loc[idx:maxindex, e] += c[-1]
                    elif idx-1 >= minindex:
                        # Counter reset, but not to 0
                        df.loc[idx:maxindex, e] += c[-1] - c[0]

            df = turnstile.make_cols(df, all=True)

            for startbuffer, *_, endbuffer in turnstile.get_buffered_boundaries(idxs=iso_neg_idxs, buffer=4, minindex=df.index.min(), maxindex=df.index.max()):
                turnstile.print_to_file(f'{k}:', iso_neg_file)
                turnstile.print_to_file(df.loc[startbuffer:endbuffer, ['dt', 'tdiff', 'iraw', 'i', 'idiff', 'idiffsign', 'icountingup', 'oraw', 'o', 'odiff', 'odiffsign', 'ocountingup']], iso_neg_file)
        k_join = '_'.join(k)
        df.to_feather(f'3_process_diffs/{k_join}.ftr')

    

def process_isol_negs(df):
    
    return

    if True:
        #thresh = get_thresh(df, type=e)
        thresh = turnstile.get_thresh(df)
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

        df[f'{e}diff'] = df[e] - df[e].shift(1, fill_value=df.at[0, e])

def process_df(area, scp, df):
    #fig, (ax1, ax2) = plot.subplots(nrows=2)
    #ax1.plot(df['dt'], df[['i', 'o']])

    df = df.copy()
    isol_negs_set = set()
    other_negs_set = set()
    jumps_set = set()
    dupes_set = set()
    df['tdiff'] = df['dt'] - df['dt'].shift(1, fill_value=df.at[0, 'dt']-pd.Timedelta(hours=4))
    for e in ['i', 'o']:
        df[f'{e}_raw'] = df[e]
        df[f'{e}diff'] = df[f'{e}diff_raw'] = df[e] - df[e].shift(1, fill_value=df.at[0, e])
        df[f'{e}diffpt'] = df[f'{e}diff'] * 3600 * 4 / df['tdiff'].copy().apply(lambda x: max(x.total_seconds(), 1))
        df[f'{e}diffsign'] = df[f'{e}diffsign_raw'] = np.sign(df[f'{e}diff']) # -1, 0, or 1
        df[f'{e}countingup'] = df[f'{e}countingup_raw'] = (df[f'{e}diffsign'].mask(df[f'{e}diffsign'] == 0).fillna(method='bfill') == 1) | (df[f'{e}diffsign'].mask(df[f'{e}diffsign'] == 0).fillna(method='ffill') == 1)

        # PROCESS ISOL NEGS
        # Set length of counting down sets
        process_isol_negs()



        # PROCESS OTHER NEGS
        negs = set(df[df[f'{e}diff']<0].index)
        other_negs_set.update(negs)
        df[f'{e}diff'] = df[f'{e}diff'].apply(np.abs)
        df[f'{e}diffpt'] = df[f'{e}diff'] * 3600 * 4 / df['tdiff'].copy().apply(lambda x: max(x.total_seconds(), 1))


        # PROCESS JUMPS
        thresh = get_thresh(df, type=e)
        jump_mask = ((df['tdiff'] <= pd.Timedelta('4H')) & (df[f'{e}diff'] > thresh)) | ((df['tdiff'] > pd.Timedelta('4H')) & (df[f'{e}diffpt'] > thresh))
        #jump_mask = (df[f'{e}diff'] > thresh)
        df.loc[jump_mask, f'{e}diff'] = 0
        jumps_set.update(set(df[jump_mask].index))
    
        #df[f'{e}diffpt'] = df[f'{e}diff'] * 3600 * 4 / df['tdiff'].apply(lambda x: max(x.total_seconds(), 1))
        df[e] = df[f'{e}diff'].cumsum()
        df['i'] = df['i'].astype('int')
        
    # df = df[['dt', 'idiff', 'odiff', 'idiff_raw', 'odiff_raw']].resample('4H', on='dt', origin='start_day', offset='2H', closed='right', label='right').sum()
    # df.index = df.index + pd.tseries.frequencies.to_offset('-2H')
    # for e in ['i', 'o']:
    #     df[e] = df[f'{e}diff'].cumsum()

    # PROCESS DUPLICATES
    df = df.groupby(by='dt').agg({'i': 'last', 'o': 'last', 'i_raw': 'last', 'o_raw': 'last', 'idiff': 'sum', 'odiff': 'sum'})

    if len(df) > 1:
        df = df[['i', 'o', 'i_raw', 'o_raw']].resample('4H', origin='start_day', offset='2H', closed='right', label='right').nearest(limit=1).interpolate(method='time')
        df.index = df.index + pd.tseries.frequencies.to_offset('-2H')
        for e in ['i', 'o']:
            df[e] = df[e].astype(int)
            df[f'{e}diff'] = df[e] - df[e].shift(1, fill_value=df.at[df.index.min(), e])

    assert df['i'].is_monotonic and df['o'].is_monotonic

    df.reset_index().to_feather(f'new_processed_areas/{area}_{scp}.ftr')

    #ax2.plot(df[['i', 'o']])

    #fig.suptitle(f'Processing Ridership Data at {area} / {scp}')
    #fig.savefig(f'new_processed_charts/{area}_{scp}.png')

    #plot.close(fig)

    return df

def clean_dfs(diffs, booth_map):

    count = 0
    processed = 0
    not_processed = 0
    diffs_len = len(diffs)
    complexes = {}

    for (area, scp), df in diffs.items():
        if area in booth_map.index:
            complex = booth_map.at[area, 'Complex']
            df = process_df(area, scp, df)[['idiff', 'odiff']].sum(axis=1)
            if complex in complexes:
                complexes[complex].append(df)
            else:
                complexes[complex] = [df]
            print(f'Processed {area} / {scp}: {count}/{diffs_len} done or {count/diffs_len:2%}')
            processed += 1
        else:
            print(f'No complex match: did not process {area} / {scp}: {count}/{diffs_len} done or {count/diffs_len:.2%}')
            not_processed += 1
        count += 1

    print(f'Processed {processed} and did not process {not_processed} out of {count} total.')
    
    to_concat = []

    for complex, dfs in complexes:
        to_concat.append(pd.concat(dfs, axis=1).fillna(0).sum(axis=1).rename(complex))

    ridership = pd.concat(to_concat, axis=1).fillna(0)
    ridership.reset_index().to_feather('ridership.ftr')
    return ridership

def import_meta():
    booth_map = pd.read_csv('../station_data/booth_to_complex.csv')
    booth_map = booth_map.set_index('Booth')
    booth_map = booth_map.dropna(subset=['Complex'])
    booth_map['Complex'] = booth_map['Complex'].astype('int')

    diffs = {tuple(p.replace('.ftr', '').split('_')):pd.read_feather(f'diffed_areas/{p}') for p in os.listdir('diffed_areas') if p.endswith('.ftr')}

    return diffs, booth_map

def ignore():
    return
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
    df = df.combine_first(timeindex).rename_axis(index='dt').sort_index()
    if not df.index.size == ogsize:
        df = df.reset_index()
        df['tdiff'] = df['dt'] - df['dt'].shift(1, fill_value=df.at[0, 'dt']-pd.Timedelta(hours=4))
        df = df.set_index('dt')
        for e in ['i', 'o']:
            df[f'{e}diffpt'] = df[f'{e}diffpt'].fillna(method='bfill')
            df[f'{e}diff'] = df[f'{e}diffpt'] * df['tdiff'].apply(lambda x: x.total_seconds())
    df = df[['desc', 'idiff', 'odiff']].resample('4H', origin='start_day', offset='2H', closed='right', label='right').agg({'desc': 'first', 'idiff': 'sum', 'odiff': 'sum'})
    df.index = df.index + pd.tseries.frequencies.to_offset('-2H')
    df = df.reset_index()
    for e in ['i', 'o']:
        df[e] = df[f'{e}diff'].cumsum()
        df['i'] = df['i'].astype('int')
    return df