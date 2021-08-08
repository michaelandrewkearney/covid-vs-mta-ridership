import logging
from iovars import *
import pandas as pd
import numpy as np
from scipy import stats
import sys

print('Reading feather. This can take a minute...')

cols = ['CONTROLAREA', 'SCP', 'DESC', 'ENTRIES', 'EXITS', 'DATETIME']

try:
    log = pd.read_feather(event_log_name, columns=cols)
except IOError as e:
    logging.exception(f'File {event_log_name} could not be read or found. Scrape data and try again.')
    sys.exit(1)

log.columns=['area', 'scp', 'desc', 'i', 'o', 'dt']

print('Feather read. Acquiring areas. This can take a minute...')

grouped = log.groupby(['area', 'scp'], sort=False)
areas = log[['area', 'scp']].drop_duplicates(ignore_index=True).sort_values(by=['area', 'scp'], ignore_index=True)
areas_len = areas.index.size

print('Areas acquired. Grouping by area. This can take a minute...')

for idx, area, scp in areas.itertuples(name=None):
	grouped.get_group((area, scp)).reset_index()[['desc', 'i', 'o', 'dt']].to_feather(f'diffed_areas/{area}_{scp}.ftr')
	print(f'{area}_{scp}: {idx+1} of {areas_len} done, or {(idx+1)/areas_len:%}')