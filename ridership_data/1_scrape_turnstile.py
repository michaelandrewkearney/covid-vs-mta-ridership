from iovars import *
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import sys
import os
import logging
from tabulate import tabulate

# Weeks are be representated by an int of the format YYMMDD, where the date is the
# Saturday after a Saturday 00:00:00 to Friday 23:59:59 cycle. For example, the week
# 210731 means from 7/24/21 00:00:00 until 7/30/21 23:59:59. In other words, if week
# 210731 and all previous weeks are present in the turnstile audit event week
# download log, the turnstile audit event log includes audit events up to 7/31/21 at
# midnight (but not any audit events collected on 7/31/21).
#
# TURNSTILE DATA FIELD DESCRIPTIONS
# C/A      = Control Area (A002)
# UNIT     = Remote Unit for a station (R051)
# SCP      = Subunit Channel Position represents an specific address for a device (02-00-00)
# STATION  = Represents the station name the device is located at
# LINENAME = Represents all train lines that can be boarded at this station
#           Normally lines are represented by one character.  LINENAME 456NQR repersents train server for 4, 5, 6, N, Q, and R trains.
# DIVISION = Represents the Line originally the station belonged to BMT, IRT, or IND   
# DATE     = Represents the date (MM-DD-YY)
# TIME     = Represents the time (hh:mm:ss) for a scheduled audit event
# DESC     = Represent the "REGULAR" scheduled audit event (Normally occurs every 4 hours)
#           1. Audits may occur more that 4 hours due to planning, or troubleshooting activities. 
#           2. Additionally, there may be a "RECOVR AUD" entry: This refers to a missed audit that was recovered. 
# ENTRIES  = The cumulative entry register value for a device
# EXITS    = The cumulative exit register value for a device

# Variables based on the format of MTA data presentation that must be up to date for this script to work
turnstile_url = 'http://web.mta.info/developers/turnstile.html'
is_turnstile_link = lambda href: href and re.compile("data/nyct/turnstile/").search(href) # the href must start with the given string

# Request MTA turnstile data link list page and check for errors
try:
    turnstile_page = requests.get(turnstile_url, timeout=10)
    turnstile_page.raise_for_status()
except (requests.exceptions.RequestException, requests.exceptions.HTTPError) as e:
    logging.exception(f'Error retrieving {turnstile_url}.')
    sys.exit(1)

# Process MTA turnstile data link list page
turnstile_soup = BeautifulSoup(turnstile_page.content, "html.parser")
weekly_links = turnstile_soup.findAll('a', href=is_turnstile_link)

# Get list of dates on turnstile data link list page (int YYMMDD, str href) and filter to only correctly formatted dates (on and after Oct 18, 2014)
dates = list(filter(lambda date: date[0] >= earliest_correctly_formatted_date, [(int(re.search(r"\d+", link['href']).group(0)), link['href']) for link in turnstile_soup.findAll('a', href=is_turnstile_link)]))
dates.sort(key=lambda x: x[0])
downloaded_dates_count = len(dates)

# Prepping empty containers and checking if the master log and downloaded week list dependencies exist
week_log = []
# event_log = pd.DataFrame() # Already created as global variable
dropped_log = pd.DataFrame()

if os.path.isfile(event_log_name) and os.path.isfile(dropped_log_name) and os.path.isfile(week_log_name):
    if input('\nDefault behavior is to update local files with MTA data published since the last time this script was run.\n' +
            'Would you like to overwrite all existing turnstile data and scrape from scratch instead?\nY/n: ') != 'Y':
        try:
            with open(week_log_name, 'r') as f:
                week_log = [line[:-1] for line in f]
        except IOError as e:
            logging.exception(f'File {week_log_name} could not be read or found. Creating new {week_log_name} and {event_log_name} files.')
            if input('Could not open local files. Overwriting turnstile data regardless of earlier user input. Abort? Y/n: ') == 'Y':
                sys.exit(0)
        else:
            new_dates = list(filter(lambda date: date[0] not in week_log, dates)) # Remove already logged dates from newly downloaded date list
            if not len(dates): # We want to exit if there are no dates to add before we bother to read the feather to memory.
                logging.warning(f'All turnstile links matched a week in {week_log_name}, so no changes were made to local files. {downloaded_dates_count} turnstile links were identified and {len(week_log)} weeks are listed in {week_log_name}.')
                print('There are no weeks to add to the ridership audit event log, likely because it is up to date. See log for more information.')
                sys.exit(0)
            try:
                event_log = pd.read_feather(event_log_name)
                dropped_log = pd.read_feather(dropped_log_name)
            except IOError as e:
                logging.exception(f'File {event_log_name} or {dropped_log_name} could not be read or found. Creating new {week_log_name}, {event_log_name}, and {dropped_log_name} files.')
                if input('Could not open local files. Overwriting turnstile data regardless of earlier user input. Abort? Y/n: ') == 'Y':
                    sys.exit(0)
            else:
                dates = new_dates

# Pull and clean data from CSVs on MTA site
update = [] #list of tuples (int week, DataFrame df, int og_size, int new_size)
for date, href in dates:
    # Get CSV
    df = pd.read_csv('http://web.mta.info/developers/' + href) # Read CSV from MTA site
    og_size = len(df.index)

    # Data Cleanup
    df.columns = [col.strip() for col in df.columns] # Remove spaces from column names
    df = df.rename(columns={'C/A': 'CONTROLAREA'}) # Rename C/A column to CONTROLAREA

    # Combining DATE and TIME into DATETIME
    df['DATETIME'] = pd.to_datetime(df['DATE'] + ' ' + df['TIME'], infer_datetime_format=True) # Smartly merge DATE and TIME
    df = df.drop(['DATE', 'TIME'], axis=1) # Drop DATE and TIME columns, which are strings and have been merged into DATETIME

    # Removing NaN rows in essential columns to log
    dropped_df = pd.DataFrame(columns=df.columns)
    essential_cols = ['CONTROLAREA', 'SCP', 'ENTRIES', 'EXITS', 'DATETIME']
    for col in essential_cols:
        na_free = df.dropna(subset=[col])
        dropped_df = dropped_df.append(df[~df.index.isin(na_free.index)])
        df = na_free
    df = df.fillna("UNKNOWN") # NaNs in all other columns won't block analysis, but do need to be resolved so that good data in other columns isn't lost

    # Typecasting for memory efficiency
    cat_cols = ['CONTROLAREA', 'UNIT', 'SCP', 'STATION', 'DIVISION', 'DESC']
    str_cols = ['LINENAME']
    int_cols = ['ENTRIES', 'EXITS']

    for col in cat_cols:
        df[col] = df[col].astype('category')
    for col in str_cols:
        df[col] = df[col].astype('str')
    for col in int_cols:
        df[col] = df[col].astype('int')

    dropped_df['SOURCEWEEK'] = date
    dropped_df['SOURCEINDEX'] = dropped_df.index

    # Add to queue to be concated with master list
    print(f'Finished scraping week {date}.')
    update.append((date, df, dropped_df, og_size, len(df.index), len(df.index) - og_size, len(dropped_df.index)))

# Sort update list and zip for metadata processing
update.sort(key = lambda x: x[0])
update_zip = list(zip(*update))

# Write week log to txt
week_log.extend(update_zip[0])
if len(week_log) != len(set(week_log)):
    if input(f'Duplicate weeks detected. Abort before overwriting {week_log_name}, {event_log_name}, and {dropped_log_name}? Y/n: ') == 'Y':
        logging.error(f'{len(week_log)-len(set(week_log))} duplicate weeks were identified. Script was aborted before local files were overwritten.')
        sys.exit(0)
    logging.error(f'{len(week_log)-len(set(week_log))} duplicate weeks were identified. Script was NOT aborted, and local files were likely overwritten. Check data for duplicate entries.')
    
with open(week_log_name, 'w') as f:
    for week in week_log:
        f.write(f'{week}\n')

# Write event log to feather
event_log_og_size = len(event_log.index)
event_log = event_log.sort_values(by=['CONTROLAREA', 'SCP', 'DATETIME', 'DESC'], ignore_index=True)
event_log = pd.concat(list(update_zip[1]) + [event_log], ignore_index=True)
event_log.to_feather(event_log_name)

# Write dropped log to feather
dropped_log_og_size = len(dropped_log.index)
dropped_log = pd.concat(list(update_zip[2]) + [dropped_log], ignore_index=True)
dropped_log.to_feather(dropped_log_name)

# Print report to log. Tuple index IDs = {0: date, 1: df, 2: dropped_df, 3: og_size, 4: df_length, 5: size_difference, 6: dropped_df_length}
log_report = 'Reached end of script.\n'
log_report += f'{len(update_zip[0])} weeks of data were added to {event_log_name} () and {dropped_log_name}, from {min(update_zip[0])} to {max(update_zip[0])}.\n'
log_report += f'{event_log_name} had {event_log_og_size} rows and gained {len(event_log.index)-event_log_og_size} rows. {dropped_log_name} had {dropped_log_og_size} rows and gained {len(dropped_log.index)-dropped_log_og_size} rows.\n'
headers = ['New Week', 'Event Count (EC)', 'Downloaded EC', 'EC Difference', 'Logged Dropped EC', 'Dropped EC Match']
report_table = []
match_count = 0
for date, df, dropped_df, og_size, new_size, size_difference, dropped_df_size in update:
    report_table.append([date, int(new_size), int(og_size), int(size_difference), int(dropped_df_size), -size_difference == dropped_df_size])
    if -size_difference == dropped_df_size:
        match_count += 1

log_report += tabulate(report_table, headers=headers, tablefmt="github") + '\n'

report_table = []
for name, func in [('Min', lambda x: min(x)), ('Max', lambda x: max(x)), ('Mean', lambda x: sum(x)/len(x)), ('Sum', lambda x: sum(x))]:
    to_append = [name]
    for i in [4, 3, 5, 6]:
        to_append.append(func(update_zip[i]))
    to_append.append('N/A')
    report_table.append(to_append)
report_table[-1][-1] = match_count

log_report += tabulate(report_table, headers=headers, tablefmt="github")
print(log_report)
logging.info(log_report)