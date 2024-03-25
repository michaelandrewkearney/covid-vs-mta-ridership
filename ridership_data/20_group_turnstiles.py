import logging

from numpy.lib.function_base import kaiser
from turnstile import event_log_name, scrape_dir, group_dir
import pandas as pd
import sys

print('Reading feather. This can take a minute...')

try:
    log = pd.read_feather(scrape_dir+event_log_name)
except IOError as e:
    logging.exception(f'File {scrape_dir}{event_log_name} could not be read or found. Scrape data and try again.')
    sys.exit(1)

print('Feather read. Acquiring areas. This can take a minute...')

cols = ['area', 'unit', 'scp', 'station', 'lines', 'div']
areas = pd.DataFrame(data=[key for key, _ in log.groupby(by=cols)], columns=cols)
areas.to_feather(group_dir+'scps_with_dupes.ftr')

by_scp = False

if by_scp:
    cols = ['area', 'scp']
    areas = pd.DataFrame(data=[key for key, _ in areas.groupby(by=cols)], columns=cols)
    areas.to_feather(group_dir+'scps_without_dupes.ftr')
    areas_len = len(areas)
    grouped = log.groupby(cols, sort=False)

    print('Areas acquired. Grouping by area. This can take a minute...')

    for idx, area, scp in areas.itertuples(name=None):
        grouped.get_group((area, scp)).reset_index()[['desc', 'i', 'o', 'dt']].to_feather(group_dir+f'scp_diffs/{area}_{scp}.ftr')
        print(f'{area}_{scp}: {idx+1} of {areas_len} done, or {(idx+1)/areas_len:%}')
else:
    cols = ['area']
    areas = pd.DataFrame(data=[key for key, _ in areas.groupby(by=cols)], columns=cols)
    areas.to_feather(group_dir+'areas_without_dupes.ftr')
    areas_len = len(areas)
    grouped = log.groupby(cols, sort=False)

    print('Areas acquired. Grouping by area. This can take a minute...')

    for idx, area in areas.itertuples(name=None):
        grouped.get_group(area).reset_index()[['scp', 'desc', 'i', 'o', 'dt']].to_feather(group_dir+f'area_diffs/{area}.ftr')
        print(f'{area}: {idx+1} of {areas_len} done, or {(idx+1)/areas_len:%}')