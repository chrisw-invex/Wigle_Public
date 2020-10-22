#Import all the needed libraries
import snowflake.connector
import reverse_geocoder as rg
from snowflake.connector.converter_null import SnowflakeNoConverterToPython
import pandas as pd
import os
import time
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine


#Code to reverse geocode a batch of lat/long values. Max batch size is 16384.
def reverseGeocode(coordinates):
    result = rg.search(coordinates)
    # result is a list containing ordered dictionary.
    return result[0]

#Set up a function so that testing can be added
def main():
    #Build credential variables
    SNOW_ACCOUNT = '*****'
    SNOW_USER = '*****'
    SNOW_PASS = '**********'
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
    #Create the cursor to point to the database instance
    cur = ctx.cursor()
    try:
        #Keeps track of how many rows have been procecced since the last batch went out
        count = 0
        #I havent figured out how to get the table size from the connector so I hard coded it for now
        tableSize = 749542376
        #Keeps track of the max number of rows being sent to table in each batch
        batchSize = 16384
        #Get the needed rows from REVEAL_DATA_RAW
        cur.execute("SELECT timestamp, maid, os, lat, lon, ip_address, connection_type, Place_name, Category, country_iso3, carrier, make , model FROM REVEAL_DATA_RAW")
        #Create the empty dataframe to store the data while lat long is being processed
        cols = [i[0] for i in cur.description]
        df = pd.DataFrame(columns=cols)
        #Store the lat/longs of each entry
        address = []
        #Number of batches sent out
        run = 1
        tic = time.perf_counter()
        #Loop through each record
        for (timestamp, maid, os, lat, lon, ip_address, connection_type, Place_name, Category, country_iso3, carrier, make , model) in cur:
            address.append((lat,lon))
            #Get the values of the given entry
            values = str(timestamp)+"|"+str(maid)+"|"+str(os)+"|"+str(lat)+"|"+str(lon)+\
                    "|"+str(ip_address)+"|"+str(connection_type)+"|"+str(Place_name)+"|"+\
                    str(Category)+"|"+str(country_iso3)+"|"+str(carrier)+"|"+str(make)+\
                    "|"+str(model)
            #Format the values to isnert into the table
            valuesList = values.split('|')
            valuesList = [i.replace(',','-') for i in valuesList]
            valuesList = ['na' if x == '' else x for x in valuesList]
            df.loc[len(df)] = valuesList
            count+=1
            #When The batch is ready to be sent
            if count==batchSize:
                #Get the counties and states
                places = rg.search(address)
                county = [i['admin2'] for i in places]
                states = [i['admin1'] for i in places]
                df['COUNTY'] = county
                df['STATE'] = states
                #Create the engine to insert the data to REVEAL_DATA
                engine = create_engine(URL(
                    user=SNOW_USER,
                    password=SNOW_PASS,
                    account=SNOW_ACCOUNT,
                    database=SNOW_DB,
                    schema=SNOW_SCHEMA,
                    role='sysadmin',
                 ))
                #Insert the dataframe into the table
                df.to_sql('reveal_data',engine,if_exists='append',index=False)
                #Reset the values to start the next batch
                df = pd.DataFrame(columns=cols)
                address=[]
                places = []
                toc = time.perf_counter()
                print(f"Uploaded "+str(count*run)+" entries in "+str(toc-tic)+" seconds")
                run+=1
                count = 0
                tic = time.perf_counter()
            #On the last iteration
            if (count == (tableSize%batchSize) and run == ((tableSize-(tableSize%batchSize))/batchSize)):
                places = rg.search(address)
                county = [i['admin2'] for i in places]
                states = [i['admin1'] for i in places]
                df['COUNTY'] = county
                df['STATE'] = states
                engine = create_engine(URL(
                    user=SNOW_USER,
                    password=SNOW_PASS,
                    account=SNOW_ACCOUNT,
                    database=SNOW_DB,
                    schema=SNOW_SCHEMA,
                    role='sysadmin',
                 ))
                #Send the data back to snowflake to update the database
                df.to_sql('reveal_data',engine,if_exists='append',index=False)
                df = pd.DataFrame(columns=cols)
                address=[]
                places = []
                toc = time.perf_counter()
                print(f"Uploaded "+str(count*run)+" entries in "+str(toc-tic)+" seconds")
                run+=1
                count = 0
                break



    finally:
        cur.close()


#Call the function
main()
