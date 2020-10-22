import boto3
import json
import snowflake.connector
from snowflake.connector.converter_null import SnowflakeNoConverterToPython


def reverseGeocode(coordinates):
    try:
        gis = GIS('http://www.arcgis.com', 'cwigle98', '9060hokie14873')
        #result = rg.search(coordinates)
        results = reverse_geocode([coordinates[1],coordinates[0]])
        # result is a list containing ordered dictionary.
        return results['address']
    except():
        return {'Subregion':'','Region':''}

SNOW_ACCOUNT = 'oja95667.us-east-1'
SNOW_USER = 'CHRISW@INVEXTECHS.COM'
SNOW_PASS = 'Invex906014873'
SNOW_FILE_FORMAT = 'DEFAULT_CSV'
SNOW_DB = 'DEVELOPMENT_DB'
SNOW_SCHEMA = 'PUBLIC'

client = boto3.client('sns')
topic = 'arn:aws:sns:us-east-1:652239706161:Reveal_Data_Transfer'

ctx = snowflake.connector.connect(
            user=SNOW_USER,
            password=SNOW_PASS,
            account=SNOW_ACCOUNT,
            database=SNOW_DB,
            schema=SNOW_SCHEMA,
            converter_class=SnowflakeNoConverterToPython
            )

cur = ctx.cursor().execute("SELECT lat, lon FROM REVEAL_DATA_RAW;")
ret = [1]
while len(ret)>0:
    ret = cur.fetchmany(8000)

    response = client.publish(TopicArn=topic,Message=str(ret))

print(response['MessageId'])
