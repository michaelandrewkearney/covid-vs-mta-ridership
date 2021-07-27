import pandas as pd
from sqlalchemy import create_engine
import seaborn as sns
import matplotlib.pyplot as plt
import geopy

#DATA:
#https://qri.cloud/nyc-transit-data/turnstiles_station_list

# tranpose for zips, set col and row headers, and cut off non zipcode entries
data = pd.read_csv('nyc_zip_locations/body.csv')
data = data[["stop_name", "gtfs_latitude", "gtfs_longitude"]]

data["gtfs_latitude"] = data["gtfs_latitude"].astype(float)
data["gtfs_longitude"] = data["gtfs_longitude"].astype(float)

data = data[0:520]
data = data.dropna()

# CREDIT TO
# https://gis.stackexchange.com/questions/352961/convert-lat-lon-to-zip-postal-code-using-python


def get_zipcode(df, geolocator, lat_field, lon_field):
    location = geolocator.reverse((df[lat_field], df[lon_field]))
    address = location.raw['address']
    
    if ("postcode" in address):
        zip = address["postcode"]
        print(zip)
        return zip

    print("SKIP")
    return None

geolocator = geopy.Nominatim(user_agent='csci1951a-project')

data["ZIP"] = data.apply(get_zipcode, axis=1, geolocator=geolocator, lat_field='gtfs_latitude', lon_field='gtfs_longitude')

data = data.dropna()

data.columns = ["STATION", "LAT", "LON", "ZIP"]

data["STATION"] = data["STATION"].str.upper()

print(data.head())

# Save to SQL database
disk_engine = create_engine('sqlite:///final_project.db')
data.to_sql('station_lats', disk_engine, if_exists='replace', index=False)

print(data.head())



# SELECT DISTINCT * from station_lats WHERE STATION IS NOT NULL
# CREATE TABLE zip_match AS SELECT DISTINCT * from station_lats GROUP BY STATION HAVING STATION IS NOT NULL

#SELECT RIDERSHIP, r.STATION, WEEK_START, ZIP, LAT, LON from ridership_per_station as r JOIN zip_match as z ON r.STATION = z.STATION


"""
CREATE TABLE the_actual_coolest_table AS
SELECT rz1.RIDERSHIP as RIDERSHIP1, rz1.WEEK_START as  WEEK_START1, rz2.RIDERSHIP as RIDERSHIP2, rz1.WEEK_START as  WEEK_START2,
rz1.STATION as STATION, rz1.ZIP as ZIP, rz1.LAT as LAT, rz1.LON as LON
from ridership_by_zip as rz1 JOIN ridership_by_zip as rz2 ON rz1.STATION = rz2.STATION 
AND (julianday(datetime(rz1.WEEK_START, '-1 year')) - julianday(rz2.WEEK_START)) BETWEEN -1 AND 1
"""

"""
CREATE TABLE proccessed_ridership AS
SELECT ZIP, WEEK_START1 as WEEK_START, 
(RIDERSHIP1 - RIDERSHIP2)/CAST(((RIDERSHIP1 + RIDERSHIP2)/2) as REAL) as PER_DIF from the_actual_coolest_table

CREATE TABLE proccessed_table AS
SELECT c.ZIP, c.WEEK_START, PER_DIF, CASERATE from covid_cases as c JOIN proccessed_ridership as r 
ON c.ZIP = r.ZIP AND datetime(c.WEEK_START) = datetime(r.WEEK_START)
"""

"""
CREATE TABLE demo_all_table AS
SELECT c.ZIP, COVID_CASE_PERCENT, PER_DIF, c.MED_INCOME 
from demo_covid_table as c JOIN demo_ridership_table as r ON c.ZIP = r.ZIP
GROUP BY c.ZIP
"""