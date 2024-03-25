from turnstile import get_diffs, get_boothmap, make_cols
import pandas as pd
import numpy as np
from math import ceil
import itertools

def do_index_analysis():
    analysis_cols = ['complex', 'area', 'scp', 'df_len',
            'dt_min', 'dt_max', 'dt_avg_step',
            'tdiff_min', 'tdiff_med', 'tdiff_mean', 'tdiff_max', 'dt_offset',
            'idiff_min', 'idiff_med', 'idiff_std', 'idiff_mean', 'idiff_max', 'idiff_150th',
            'idiff_neg_ct', 'idiff_zero_ct', 'idiff_pos_ct',
            'idiffpt_min', 'idiffpt_med', 'idiffpt_std', 'idiffpt_mean', 'idiffpt_max',
            'inegset_ct', 'inegset_avg_len',
            'odiff_min', 'odiff_med', 'odiff_std', 'odiff_mean', 'odiff_max', 'odiff_150th',
            'odiff_neg_ct', 'odiff_zero_ct', 'odiff_pos_ct',
            'odiffpt_min', 'odiffpt_med', 'odiffpt_std', 'odiffpt_mean', 'odiffpt_max',
            'onegset_ct', 'onegset_avg_len']

    diffs = get_diffs()
    boothmap = get_boothmap()

    analyses = []
    count = 0
    diffs_len = len(diffs)

    for (area, scp), df in diffs.items():
        if area in boothmap.index:
            complex = boothmap.loc[area, 'Complex']
            df = make_cols(df, all=True)
            size = df.index.size
            dtmin = df['dt'].min()
            dtmax = df['dt'].max()
            inegset_ct = ceil(df['icountingup'].diff().fillna(False).sum() / 2)
            onegset_ct = ceil(df['ocountingup'].diff().fillna(False).sum() / 2)

            data=[complex, area, scp, size,
                dtmin, dtmax, (dtmax-dtmin)/size,
                df['tdiff'].min(), df['tdiff'].median(), df['tdiff'].mean(), df['tdiff'].max(), df['dt'].apply(lambda x: x.hour % 4).mode()[0],
                df['idiff'].min(), df['idiff'].median(), df['idiff'].std(), df['idiff'].mean(), df['idiff'].max(), np.abs(df['idiff']).nlargest(n=150).iloc[-1],
                (df['idiffsign'] == -1).sum(), (df['idiffsign'] == 0).sum(), (df['idiffsign'] == 1).sum(),
                df['idiffpt'].min(), df['idiffpt'].median(), df['idiffpt'].std(), df['idiffpt'].mean(), df['idiffpt'].max(), 
                inegset_ct, ((~df['icountingup']).sum() / inegset_ct) if inegset_ct != 0 else 0,
                df['odiff'].min(), df['odiff'].median(), df['odiff'].std(), df['odiff'].mean(), df['odiff'].max(), np.abs(df['odiff']).nlargest(n=150).iloc[-1],
                (df['odiffsign'] == -1).sum(), (df['odiffsign'] == 0).sum(), (df['odiffsign'] == 1).sum(),
                df['odiffpt'].min(), df['odiffpt'].median(), df['odiff'].std(), df['odiffpt'].mean(), df['odiffpt'].max(),
                onegset_ct, ((~df['icountingup']).sum() / onegset_ct) if onegset_ct != 0 else 0]
            
            df.to_feather(f'areas/{complex}_{area}_{scp}.ftr')
            analyses.append(pd.Series(data, index=analysis_cols, name=f'{area}_{scp}'))
            count +=1
            print(f'{area} {scp} done: {count}/{diffs_len} = {count/diffs_len:.2%}')

    analysis = pd.concat(analyses, axis='columns', ignore_index=True).transpose()
    analysis.to_feather('analysis.ftr')
    return analysis

diffs = get_diffs()
areas = {area:pd.concat({k[1]:diffs[k].groupby(by='dt').sum()[['i', 'o']] for k in ks}, axis=1).ffill().bfill().astype('int') for area, ks in itertools.groupby(sorted(list(diffs.keys()), key=(lambda x:x[0])), key=lambda x:x[0])}
for area, df in areas.items():
    df[[('total', 'i'), ('total', 'o')]] = df.groupby(level=1, axis=1).sum()
