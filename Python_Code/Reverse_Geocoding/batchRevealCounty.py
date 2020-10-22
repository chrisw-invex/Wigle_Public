#import all necessary libraries
import snowflake.connector
from snowflake.connector.converter_null import SnowflakeNoConverterToPython
import pandas as pd
import os
import time
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine
import multiprocessing as mp
import numpy as np
from arcgis.gis import GIS
from arcgis.geocoding import reverse_geocode
import dask.dataframe as dd
from dask.multiprocessing import get



#Code to reverse geocode a batch of lat/long values using arcGIS REST API.
def reverseGeocode(coordinates):
    try:
        gis = GIS('http://www.arcgis.com', '*****', '*****')
        #Search up corndinates using the arcGIS API
        results = reverse_geocode([coordinates[1],coordinates[0]])
        # result is a list containing ordered dictionary.
        return results['address']
    except():
        #If the county cannot be found with the API
        return {'Subregion':'','Region':''}

#Apply the search for county
def revGeoPandasCounty(x):
    place = reverseGeocode((x[3],x[4]))
    return (place['Subregion'])
#Apply search for state
def revGeoPandasState(x):
    place = reverseGeocode((x[3],x[4]))
    return (place['Region'])
#Apply the functions to the data
def revGeoPandas(df):
    df['COUNTY'] = df.apply(revGeoPandasCounty,axis=1)
    df['STATE'] = df.apply(revGeoPandasState,axis=1)
    return df

#A function that creates snowflake enignes in order to upload the data to the database
def createEngine(SNOW_USER,SNOW_PASS,SNOW_ACCOUNT,SNOW_DB,SNOW_SCHEMA):
    engine = create_engine(URL(
                    user=SNOW_USER,
                    password=SNOW_PASS,
                    account=SNOW_ACCOUNT,
                    database=SNOW_DB,
                    schema=SNOW_SCHEMA,
                    role='sysadmin',
                 ))
    return engine

#Set up a function so that testing can be added
def main():
    #Build credential variables
    SNOW_ACCOUNT = '*****'
    SNOW_USER = '*****'
    SNOW_PASS = '*****'
    SNOW_FILE_FORMAT = 'DEFAULT_CSV'
    SNOW_DB = '*****'
    SNOW_SCHEMA = '*****'

    # connect to snowflake using set credentials
    ctx = snowflake.connector.connect(
            user=SNOW_USER,
            password=SNOW_PASS,
            account=SNOW_ACCOUNT,
            database=SNOW_DB,
            schema=SNOW_SCHEMA,
            converter_class=SnowflakeNoConverterToPython
            )
    #Get all the data from the table so that we can loop through it
    cur = ctx.cursor().execute("SELECT timestamp, maid, os, lat, lon, ip_address, connection_type, Place_name, Category, country_iso3, carrier, make , model FROM REVEAL_DATA_RAW")
    try:
        batchSize = 16000
        #Get the columns of the database table
        cols = [i[0] for i in cur.description]
        #Start a timer
        tic = time.perf_counter()
        #Grab a batch of the data
        ret = cur.fetchmany(batchSize)
        #While there is still data to be grabbed
        while len(ret) >= 0:
            #Create a temporary dataframe to store the values
            df = pd.DataFrame(ret,columns=cols)
            #Apply the function to the dataframe 
            df = revGeoPandas(df)
            #Create the engine
            engine = createEngine(SNOW_USER,SNOW_PASS,SNOW_ACCOUNT,SNOW_DB,SNOW_SCHEMA)
            #Send the data to the database
            df.to_sql('reveal_data',engine,if_exists='append',index=False)
            toc = time.perf_counter()
            print(f"Uploaded "+str(len(ret))+" entries in "+str(toc-tic)+" seconds")
            tic = time.perf_counter()
            ret = cur.fetchmany(batchSize)


    finally:
        cur.close()

main()
