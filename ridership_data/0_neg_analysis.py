import logging
from iovars import *
import pandas as pd
import numpy as np
import os
import sys

try:
    diffs = {tuple(p.replace('.ftr', '').split('_')):pd.read_feather(f'diffed_areas/{p}') for p in os.listdir('diffed_areas') if p.endswith('.ftr')}
except IOError as e:
    logging.exception(f'Diffed areas could not be read or found. Try again.')
    sys.exit(1)

print('Read feathers.')

buffer = 4
limit = 40

with open(negs_output_file, 'w') as f, pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.expand_frame_repr', False):
    og_out = sys.stdout
    sys.stdout = f

    for (area, scp), df in diffs.items():
        isolnegs = []
        for e in ['i', 'o']:
            df[f'{e}diff'] = df[e] - df[e].shift(1, fill_value=df.at[0, e])
            df[f'{e}diffsign'] = np.sign(df[f'{e}diff']) # -1, 0, or 1
            df[f'{e}countingup'] = (df[f'{e}diffsign'].mask(df[f'{e}diffsign'] == 0).fillna(method='bfill') == 1) | (df[f'{e}diffsign'].mask(df[f'{e}diffsign'] == 0).fillna(method='ffill') == 1)
            df[f'{e}negisol'] = ~df[f'{e}countingup'] & df[f'{e}countingup'].shift(1) & df[f'{e}countingup'].shift(-1)
            isolnegs.extend(list(df.loc[df[f'{e}negisol']].index))

        negset = sorted(list(set(isolnegs)))
        
        boundaries = []

        while negset:
            start = end = negset.pop()
            while negset and start - negset[-1] <= buffer*2:
                start = negset.pop()
            boundaries.append((start-buffer, end+buffer))

        print(f'Area: {area}\tSCP: {scp}\tNeg set count: {len(boundaries)}')

        for start, end in boundaries:
            if end-start-buffer*2 > limit:
                size = int(np.floor((limit+buffer*2)/2))
                print(f'Negative Set above {limit} row limit: {end-start-buffer*2} rows. First and last {size} rows are shown.')
                print(df[start:start+size])
                print('. . .')
                print(df[end-size:end])
            else:
                print(df[start:end])
            print('\n')
        print('\n')

        
    
    sys.stdout = og_out

logging.info(f'Negative analysis complete. See {negs_output_file}.')