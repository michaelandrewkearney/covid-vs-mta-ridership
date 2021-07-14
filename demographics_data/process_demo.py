import pandas as pd
from sqlalchemy import create_engine

demo_df = pd.read_csv('NYC_income.csv')

disk_engine = create_engine('sqlite:///final_project.db')
demo_df.to_sql('demographics_by_county', disk_engine, if_exists='replace', index=False)