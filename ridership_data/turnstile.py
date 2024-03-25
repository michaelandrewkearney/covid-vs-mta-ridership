import pandas as pd
import numpy as np
import logging
import os
import sys

# TODO
# 10_scrape_audits
# 20_group_areas
# 30_clean_counters
# 40_resample_timeindex
# 50_aggregate_complexes
# 

##### SET UP FILE I/O #####
# Directories for each of the 5 stages
scrape_dir = '10_scrape_turnstile/'
group_dir = '20_group_turnstiles/'
clean_dir = '30_clean_counters/'
resample_dir = '40_resample_timeindex/'
aggregate_dir = '50_aggregate_complexes/'

# Files shared between stages
event_log_name = 'turnstile_audit_event_log.ftr'
dropped_log_name = 'turnstile_dropped_audit_event_log.ftr'
week_log_name = 'turnstile_audit_event_week_download_log.txt'
duplicate_log_name = 'duplicate_audit_event_log.ftr'
audit_table_name = 'audit_table.ftr'
dupes_output_file = 'dupes.txt'
neg_set_file = 'neg_sets.txt'
iso_neg_file = 'iso_negs.txt'

# Create global variables and methods
logging.basicConfig(level=logging.INFO, filename='turnstile.log', filemode='a', format='%(asctime)s %(levelname)-8s %(message)s')
earliest_correctly_formatted_date = 141018 # Data before 10/18/14
io = ['i', 'o']
min_negset_len = 10

def print_to_file(mystring, f, ogstdout=sys.stdout):
    sys.stdout = f
    print(mystring)
    sys.stdout = ogstdout

def get_scp_diffs(dir=group_dir+'scp_diffs/', cols=None):
    assert dir.endswith('/')
    return {tuple(p.replace('.ftr', '').split('_')):pd.read_feather(dir+p, columns=cols) for p in os.listdir(dir) if p.endswith('.ftr')}

def get_area_diffs(dir=group_dir+'area_diffs/', cols=None):
    assert dir.endswith('/')
    return {p.replace('.ftr', ''):pd.read_feather(dir+p, columns=cols) for p in os.listdir(dir) if p.endswith('.ftr')}

def get_scp_diffs_by_area(dir=group_dir+'scp_diffs/', cols=None):
    assert dir.endswith('/')
    diffs = {}
    for p in os.listdir(dir):
        if p.endswith('.ftr'):
            area, scp = tuple(p.replace('.ftr', '').split('_'))
            if not area in diffs:
                diffs[area] = {}
            diffs[area].update({scp:pd.read_feather(dir+p, columns=cols)})
    return diffs

def get_combined_diffs_by_area(dir=group_dir+'area_diffs/', cols=None):
    cols=['scp', 'desc', 'dt', 'i', 'o']
    diffs = {}
    for area, df in {p.replace('.ftr', ''):pd.read_feather(dir+p, columns=cols) for p in os.listdir(dir) if p.endswith('.ftr')}.items():
        df[['scpa', 'scpb', 'scpc']] = df['scp'].apply(lambda x: x.split('-')).to_list()
        df = df.drop('scp', axis=1)
        df = df.set_index(['dt', 'desc', 'scpa', 'scpb', 'scpc'])
        df = df.unstack().unstack().unstack()
        diffs.update({area:df})
    return diffs

    
# def get_diffs_by_scp(dir=group_dir+'diffs/', cols=None):
#     areas = get_diffs_by_area(dir, cols)
#     for area in areas:
#         adf = pd.DataFrame()
#         for scp in areas[area]:
#             df = areas[area][scp]
#             df.columns = [(*tuple(scp.split('-')), col) for col in df.columns]
#             if adf.empty:
#                 adf = df.copy()
#             else:
#                 adf = adf.merge(df, on='dt', how='outer')
#         adf.columns = pd.MultiIndex.from_tuples(adf.columns)
#     return None


def get_boothmap():
    boothmap = pd.read_feather('boothmap.ftr')
    boothmap = boothmap.set_index('Booth')
    boothmap = boothmap.dropna(subset=['Complex'])
    boothmap['Complex'] = boothmap['Complex'].astype('int')
    return boothmap

def make_cols(df, all=False, raw=False, tdiff=False, cumsum=False, diff=False, diffpt=False, diffsign=False, countingup=False, negsetlen=False):
    assert not (cumsum and diff), 'Cannot process both cumsum and diff in a single call of process_cols(). Choose one.'
    if df.empty:
        return df
    if all:
        raw = tdiff = diff = diffpt = diffsign = countingup = negsetlen = True
    cols = df.columns
    if raw:
        for e in io:
            assert e in cols, f'df neeeds {e} col to process raw cols'
        for e in io:
            df[e+'raw'] = df[e]
        cols = df.columns
    if tdiff:
        assert 'dt' in cols, 'df needs dt col to process tdiff col'
        df['tdiff'] = df['dt'].diff()
        df['tdiff'].iat[0] = pd.Timedelta(hours=4)
        cols = df.columns
    if cumsum:
        assert 'idiff' in cols and 'odiff' in cols, 'df needs diff cols to integrate i/o cols'
        for e in io:
            df[e] = df[e+'diff'].cumsum()
        cols = df.columns
    elif diff:
        assert 'i' in cols and 'o' in cols, 'df needs i/o cols to derive diff cols'
        for e in io:
            df[e+'diff'] = df[e].diff().fillna(0).astype('int')
        cols = df.columns
    if diffpt:
        for col in ['idiff', 'odiff', 'tdiff']:
            assert col in cols, f'df needs {col} col to process diffpt'
        for e in io:
            df[f'{e}diffpt'] = ((df[f'{e}diff'] * (60*60*4) / df['tdiff'].apply(lambda x: x.total_seconds() if x.value != 0 else pd.Timedelta(hours=4).total_seconds())) if len(df) > 1 else 0)
        cols = df.columns
    if diffsign:
        for e in io:
            assert e+'diff' in cols, f'df needs {e}diff col to process diffsign'
        for e in io:
            df[f'{e}diffsign'] = np.sign(df[f'{e}diff']) # -1, 0, or 1
        cols = df.columns
    if countingup:
        for e in io:
            assert e+'diffsign' in cols, f'df needs {e}diffsign col to process countingup'
        for e in io:
            m = df[f'{e}diffsign'].mask(df[f'{e}diffsign'] == 0)
            if pd.isnull(m.iat[0]):
                m.iat[0] = 1
            if pd.isnull(m.iat[-1]):
                m.iat[-1] = 1
            df[f'{e}countingup'] = (m.copy().fillna(method='bfill') == 1) | (m.copy().fillna(method='ffill') == 1)
    if negsetlen:
        for e in io:
            assert e+'countingup' in cols, f'df needs {e}countingup col to process negsetlen'
        for e in io: # Identify length of negatively monotonic portions
            last = next = df.reset_index()['index'].where(df[f'{e}countingup'], np.nan)
            if pd.isnull(last.iat[0]):
                last.iat[0] = -1
            if pd.isnull(next.iat[-1]):
                next.iat[-1] = df.index.size
            last = last.ffill()
            next = next.bfill()
            df[f'{e}negsetlen'] = (next - last - 1).apply(lambda x: max(0, x))
    return df

def get_rolling_thresh(s, idx, windowsize=250, mask=True):
    minindex, maxindex = s.index.min(), s.index.max()
    series = s[mask].loc[max(idx-windowsize/2,  minindex):min(idx+windowsize/2, maxindex)].copy()
    std = series.std()
    mean = series.mean()
    return std*3+mean

def get_thresh(df, type='i', quantile=0.95, multiplier=10):
    if len(df) < 2:
        return 0
    return 50000
    return min((df[f'{type}diffpt'] + 1).copy().apply(np.abs).quantile(quantile)*multiplier, 50000)

def get_buffered_boundaries(idxs=[], buffer=10, proximity=None, minindex=None, maxindex=None):
    if proximity == None:
        proximity = buffer*2
    if not idxs:
        return []
    if minindex is None:
        minindex = min(idxs)
    if maxindex is None:
        maxindex = max(idxs)
    boundaries = []
    idxs = sorted(list(idxs))
    while idxs:
        start = end = idxs.pop()
        while idxs and start - idxs[-1] <= proximity:
            start = idxs.pop()
        boundaries.append((max(start-buffer, minindex), start, end, min(end+buffer, maxindex)))
    return boundaries

def get_largest_diffs(diffs=None):
    if diffs is None:
        diffs = get_scp_diffs()
    ls = []
    for (area, scp), df in diffs.items():
        for e in io:
            if not f'{e}diff' in df.columns:
                df = make_cols(df, diff=True)
            ls.append(((area, scp, e), df[f'{e}diff'].apply(np.abs).nlargest(n=100, keep='all').reset_index(drop=True)))
    mylist = list(zip(*ls))
    largest = pd.concat(mylist[1], axis=1, keys=mylist[0])
    largest = largest.mask(largest < 10, np.nan).dropna(how='all', axis=0).transpose()
    return largest

# def fix_negs(df):
#     for e in io:
#         neg_run_mask = df[f'{e}negsetlen'].gt(10)
#         df = df[f'{e}diff'].mask(df[f'{e}negsetlen'].gt(10))


#         isol_negs_idxs = sorted(list(df[(df[f'{e}negsetlen']>0) & (df[f'{e}negsetlen']<10)].index))

#         thresh = get_thresh(df, type=e)
#         minindex = df.index[0]
#         maxindex = df.index[-1]

#         while isol_negs_idxs:
#             ni = isol_negs_idxs.pop()
#             c = [df.loc[ni+x, e] if ni+x >= minindex and ni+x <= maxindex else None for x in [0, 1, 2, -2, -1]]
#             assert c[0] < 0
#             if ni-2 >= minindex and c[0] >= c[-2]:
#                 # ni is the index after a positive spike...
#                 if c[-1] > c[1]:
#                 #if np.abs(df.at[ni, f'{e}diffpt']) > thresh:
#                     df.at[ni-1, e] = c[0] # Fill previous spike value with current normal value
#                 # ... or a small negative dip
#                 else:
#                     df.at[ni, e] = c[-1]
#             elif ni-1 >= minindex and ni+1 <= maxindex and c[1] >= c[-1]:
#                 # ni is the index of a negative spike, or a small negative dip
#                 #if np.abs(c[-1] - c[0]) * 3600 * 4 / max((df.at[ni, 'dt'] - df.at[ni-1, 'dt']).total_seconds(), 1) > thresh:
#                 df.at[ni, e] = c[-1] # Fill current spike value with previous normal value
#                 # ... or a small negative dip
#                 #else:
#                 #    df.at[ni, e] = c[-1]
#             elif ni-1 >= minindex and c[0] * 3600 * 4 / max(df.at[ni, 'tdiff'].total_seconds(), 1) <= thresh:
#                 # Counter reset to near 0
#                 df.loc[ni:maxindex, e] += c[-1]
#             elif ni-1 >= minindex:
#                 # Counter reset, but not to 0
#                 df.loc[ni:maxindex, e] += c[-1] - c[0]

#     #df = make_cols(df, diff=True, diffpt=True, diffsign=True, countingup=True)

#     #return df, isol_negs_idx_dict

def process_duplicates(areas=None):
    if areas is None:
        get_scp_diffs_by_area(cols=['dt', 'desc', 'i', 'o'])
    for area in areas:
        for scp in areas[area]:
            df = areas[area][scp]
            idxs = df[df.duplicated(subset='dt', keep=False)].index
            boundaries = get_buffered_boundaries(idxs=list(idxs), minindex=df.index.min(), maxindex=df.index.max())
            for sb, s, e, eb in boundaries:
                segment = df.loc[sb:eb].copy()
                sliver = segment.loc[s if sb==s else s-1:e if eb==e else e+1].copy()
                idx = sliver.index.copy()
                if sliver['i'].is_monotonic and sliver['o'].is_monotonic:
                    pass
                elif sliver['i'].is_monotonic:
                    pass
                elif sliver['o'].is_monotonic:
                    pass
                elif sliver['i'].iat[0]:
                    sliver = sliver.sort_values(by='c')
                else:
                    before=0
                    top = sliver[sliver['c'] >= before].sort_values(by='c').copy()
                    bottom = sliver[sliver['c'] < before].sort_values(by='c').copy()
                    sliver = top.append(bottom)
                sliver.index = idx
                segment.update(sliver)
                to_print = segment.join(df.loc[sb:eb].copy(), rsuffix='_raw')
                print(to_print)
                segment = segment.drop('c', axis=1)
                df.update(segment)
            areas[area][scp] = df
    return areas

def process_recovers(areas=None):
    if areas is None:
        areas = get_scp_diffs_by_area(cols=['dt', 'desc', 'i', 'o'])
    summary = pd.DataFrame()
    for area in areas:
        for scp in areas[area]:
            df = areas[area][scp]
            racount = (df['desc'] == 'RECOVR AUD').sum()
            idxs = df[(df['desc'] == 'RECOVR AUD') & ~(df.duplicated(subset='dt', keep=False))].index
            boundaries = get_buffered_boundaries(idxs=sorted(list(idxs)), buffer=5, proximity=1, minindex=df.index.min(), maxindex=df.index.max())
            c = 0
            for sb, s, e, eb in boundaries:
                sliver = df.loc[max(sb, s-1):min(eb, e+1)].copy()
                if (sliver['i'].is_monotonic or sliver['i'].is_monotonic_decreasing) and (sliver['o'].is_monotonic or sliver['o'].is_monotonic_decreasing):
                    sliver.loc[s:e, 'desc'] = 'REGULAR'
                    df.update(sliver)
                    c += e-s+1
            summary = summary.append({'area': area, 'scp': scp, 'count': c, 'idxcount': len(idxs), 'racount': racount}, ignore_index=True)
    print(summary.set_index(['area', 'scp']).sum(axis=0))
    return areas, summary
            #print(boundaries)
            # for sb, s, e, eb in boundaries:
            #     segment = df.loc[sb:eb].copy()
            #     segment['c'] = segment['i'] + segment ['o']
            #     before = segment.at[s, 'c'] if sb==s else segment.at[s-1, 'c']
            #     after = segment.at[e, 'c'] if eb==e else segment.at[e+1, 'c']
            #     sliver = segment.loc[s:e].copy()
            #     idx = sliver.index.copy()
            #     if before <= after:
            #         sliver = sliver.sort_values(by='c')
            #     else:
            #         top = sliver[sliver['c'] >= before].sort_values(by='c').copy()
            #         bottom = sliver[sliver['c'] < before].sort_values(by='c').copy()
            #         sliver = top.append(bottom)
            #     sliver.index = idx
            #     segment.update(sliver)
            #     to_print = segment.join(df.loc[sb:eb].copy(), rsuffix='_raw')
            #     print(to_print)
            #     segment = segment.drop('c', axis=1)
            #     df.update(segment)
            # areas[area][scp] = df
    #return areas
