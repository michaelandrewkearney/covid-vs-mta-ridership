import logging
from iovars import *
import pandas as pd
import numpy as np
from scipy import stats
import sys

print('Reading feather. This can take a minute...')

feather = 'diffs.ftr'
outliers_output_file = 'outliers.txt'
buffer = 4

try:
    diffs = pd.read_feather(feather)
except IOError as e:
    logging.exception(f'File {feather} could not be read or found. Scrape data and try again.')
    sys.exit(1)

print('Feather read. Processing outliers. This can take a minute...')

def print_outliers(col='cumiz', thresh=0.002):
    outliers = diffs[diffs[col]>thresh].sort_values(by=col)
    print(f'{outliers.index.size} outliers detected. Printing to file...')
    og_out = sys.stdout
    with open(outliers_output_file, 'w') as f, pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.expand_frame_repr', False):
        sys.stdout = f
        for i in outliers.index:
            print(diffs.loc[i-buffer+1:i+buffer])
            print('\n\n')
    sys.stdout = og_out
    print('Printing done.')

print(diffs.columns)
col = input('Type a column name:')
thresh = input('Enter a threshold (\'default\' for 0.002):')
print_outliers(col=col, thresh=thresh)