
import pandas as pd
import os
diffs = {tuple(p.replace('.ftr', '').split('_')):pd.read_feather(f'diffed_areas/{p}') for p in os.listdir('diffed_areas') if p.endswith('.ftr')}
diffs_size = len(diffs)

from diff_processing import *

with open('isol_negs.txt', 'w') as isol_negs_file, open('negs.txt', 'w') as negs_file, open('jumps.txt', 'w') as jumps_file, open('dupes.txt', 'w') as dupes_file, pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.expand_frame_repr', False):
    count = 0
    for (area, scp), df in diffs.items():
        times = []
        if count > 30:
            break
        assert df['dt'].is_monotonic, f'{area} / {scp} column \'dt\' is not monotonic.'
        times.append(process_time())
        df = prepare_analysis_columns(df)
        times.append(process_time())
        df, isolneg_idxs = process_isol_negs(df)
        print_idxs_buffered(df=df, f=isol_negs_file, idxlist=isolneg_idxs, area=area, scp=scp)
        times.append(process_time())
        df, neg_idxs = process_negs(df)
        print_idxs_buffered(df=df, f=negs_file, idxlist=neg_idxs, area=area, scp=scp)
        times.append(process_time())
        df, jump_idxs = process_jumps(df)
        print_idxs_buffered(df=df, f=jumps_file, idxlist=jump_idxs, area=area, scp=scp)
        times.append(process_time())
        df, dupe_idxs = process_duplicates(df)
        print_idxs_buffered(df=df, f=dupes_file, idxlist=dupe_idxs, area=area, scp=scp)
        times.append(process_time())
        df = process_resampling(df)
        times.append(process_time())
        count += 1
        print(f'{area} / {scp} processed: {count}/{diffs_size} or {count/diffs_size:.2%}. {[times[x]-times[x-1] for x in range(1, len(times))]}')
        df.to_feather(f'processed_areas/{area}_{scp}.ftr')

for (area, scp), df in diffs.items():
    times = []
    if count > 30:
        break
    assert df['dt'].is_monotonic, f'{area} / {scp} column \'dt\' is not monotonic.'
    times.append(process_time())
    df = process_df(df)
    times.append(process_time())
    count += 1
    print(f'{area} / {scp} processed: {count}/{diffs_size} or {count/diffs_size:.2%}. {[times[x]-times[x-1] for x in range(1, len(times))]}')
    df.to_feather(f'processed_areas/{area}_{scp}.ftr')