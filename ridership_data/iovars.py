import pandas as pd
import logging

# Set up log and file I/O
logging.basicConfig(level=logging.INFO, filename='turnstile.log', filemode='a', format='%(asctime)s %(levelname)-8s %(message)s')
earliest_correctly_formatted_date = 141018 # Data before 10/18/14
event_log_extension = '.ftr'
dropped_log_extension = '.ftr'
week_log_extension = '.txt'
duplicate_log_extension = '.ftr'
audit_table_extension = '.ftr'

event_log_name = 'turnstile_audit_event_log' + event_log_extension
dropped_log_name = 'turnstile_dropped_audit_event_log' + dropped_log_extension
week_log_name = 'turnstile_audit_event_week_download_log' + week_log_extension
duplicate_log_name = 'duplicate_audit_event_log' + duplicate_log_extension
audit_table_name = 'audit_table' + audit_table_extension