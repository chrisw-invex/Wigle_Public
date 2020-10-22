#Import needed python3 lbraries
import boto3
import json
import pandas as pd
from arcgis.gis import GIS
from arcgis.geocoding import reverse_geocode
import snowflake.connector
from snowflake.connector.converter_null import SnowflakeNoConverterToPython


#Set up credentials for the account
SNOW_ACCOUNT = '*****'
SNOW_USER = '*****'
SNOW_PASS = '*****'
SNOW_FILE_FORMAT = 'DEFAULT_CSV'
SNOW_DB = '*****'
SNOW_SCHEMA = '*****'

#Code to get the county and state of the lat/long
def reverseGeocode(coordinates):
    try:
        gis = GIS('http://www.arcgis.com', '*****', '*****')
        #result = rg.search(coordinates)
        results = reverse_geocode([coordinates[1],coordinates[0]])
        # result is a list containing ordered dictionary.
        return results['address']
    except():
        return {'Subregion':'','Region':''}

#Make the snowflake engine variable
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

global engine
engine = createEngine(SNOW_USER,SNOW_PASS,SNOW_ACCOUNT,SNOW_DB,SNOW_SCHEMA)

#Apply the geocode function to the dataframe row
def revGeoPandas(x):
    place = reverseGeocode((x[0],x[1]))
    area = "'"+place['Subregion']+','+place['Region']+"'"
    return area
    #Upload the data to the table
    #ctx.cursor().execute('UPDATE REVEAL_DATA_RAW SET AREA = '+area+' WHERE LAT = '+str(x[0])+' AND LON = '+str(x[1])+';')    

#Start up the sqs client
client = boto3.client('sqs')

#The sqs queue url to pull messages from
queueURL = 'https://sqs.us-east-1.amazonaws.com/652239706161/Reveal_Data_Transfer'
try:
    #Keeps going until there is an error which means that the queue is empty
    while True:
        
        #Get the next message
        message = client.receive_message(QueueUrl = queueURL)
        #Process the emssage
        values = json.loads(message['Messages'][0]['Body'])['Message']
        values = values.replace('[','').replace(']','').replace('), ',')|').replace('(','').replace(')','').split('|')
        values = [i.split(', ') for i in values]
        #Put the message data in a data frame
        df = pd.DataFrame(values, columns =['Lat','Lon'])
        #Apply the function to the data
        df['Area'] = df.apply(revGeoPandas, axis = 1)
        df.to_sql('reveal_location',engine,if_exists='append',index=False)
        
except():
    print('Done')
