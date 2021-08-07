import logging
from iovars import *
import sys
import pandas as pd

dupes_output_file = 'dupes.txt'

try:
    log = pd.read_feather(event_log_name)
except IOError as e:
    logging.exception(f'File {event_log_name} could not be read or found. Scrape data and try again.')
    sys.exit(1)

print('Read feather.')

log.columns=['area', 'unit', 'scp', 'station', 'lines', 'div', 'desc', 'i', 'o', 'dt']

log = log.sort_values(by=['area', 'scp', 'dt', 'desc'], ascending=[True, True, True, False], ignore_index=True)

dupe_buffer = 4

i = pd.DataFrame(log[log.duplicated(subset=['area', 'scp', 'dt'], keep=False)].index, columns=['ind'])
i['inddiff'] = i['ind'] - i['ind'].shift(1, fill_value=0)
j = i[i['inddiff']>(dupe_buffer)].reset_index()
j['from_index'] = j['index']
j['to_index'] = j['index'].shift(-1, fill_value=i.index[-1])-1
j['rinddiff'] = 0
for ind, row in j.iterrows():
	row['rinddiff'] = i.loc[row['from_index']+1:row['to_index'], 'inddiff'].sum()


og_out = sys.stdout
with open(dupes_output_file, 'w') as f, pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.expand_frame_repr', False):
	sys.stdout = f
	for x, r in j.iterrows():
		print(log.loc[(r['ind']-dupe_buffer+1):(r['ind']+r['rinddiff']+dupe_buffer)])
		print('\n\n')
	sys.stdout = og_out

logging.info(f'Duplicate analysis complete. See {dupes_output_file}.')