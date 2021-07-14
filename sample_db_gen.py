import argparse
import pandas as pd
from sqlalchemy import create_engine

parser = argparse.ArgumentParser()
parser.add_argument("database_path", help="Path to the database you want to sample")
args = parser.parse_args()

table_sql = """SELECT 
    name
FROM 
    sqlite_master 
WHERE 
    type ='table' AND 
    name NOT LIKE 'sqlite_%';
"""

sql_path = 'sqlite:///' + args.database_path

tables = pd.read_sql(table_sql, sql_path)['name']

for table in tables: 
    df = pd.read_sql_table(table, sql_path)
    disk_engine = create_engine(sql_path[:-3] + '_sample.db')
    df.head(100).to_sql(table, disk_engine, if_exists='replace', index=False)

