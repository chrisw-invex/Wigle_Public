-- script that creates the table to store the reveal data
use role sysadmin;
use warehouse INCOMING_WH;
use database Development_DB;
use schema public;

create or replace file format gzip_format
  type = CSV
  FIELD_DELIMITER = ','
  FIELD_OPTIONALLY_ENCLOSED_BY='"'
  SKIP_HEADER=1
  COMPRESSION=GZIP;

--Create the table to store the reveal data
create or replace table REVEAL_DATA_RAW 
(utc_timestamp TIMESTAMP_NTZ,
maid string,
os string,
lat float,
lon float,
ip_address string,
connection_type string,
accuracy string,
gps_speed string,
Place_name string,
placeID string,
Category string,
country_iso3 string,
carrier string,
make string,
model string,
app_id string,
org_id string"DEVELOPMENT_DB"."PUBLIC"."REVEAL_DATA_RAW");


-- Create staging area to load COVID Tracker csv file
--Takes 1.63 seconds to stage the data from one day
CREATE or REPLACE STAGE Reveal_Data_stage
--Edit this in order to upload more data
url='s3://*****'
credentials=(aws_key_id='*****' aws_secret_key='*****')
file_format = gzip_format;

--Takes 280 ms to remove the _SUCCESS file from one folder
rm @Reveal_Data_stage/_SUCCESS;


--Takes 6 minutes 41 seconds to copy all the data from one folder into the table
copy into REVEAL_DATA_RAW
    from '@Reveal_Data_stage'
    file_format = gzip_format;
