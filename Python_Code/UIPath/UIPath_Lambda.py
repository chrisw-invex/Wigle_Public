import boto3
import json
import pymysql
import sys
import csv
import datetime
from io import StringIO

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    date = [str(datetime.datetime.now())]
    s3.put_object(Body='\n'.join(date), Bucket='***',Key='***/***')
    host = "***"
    User = "***"
    password = '***'
    dbName = "***"
    try:
        con = pymysql.connect(host=host, user=User, passwd=password, db=dbName, connect_timeout=5)
    except Exception as e:
        return {'statusCode': 200,'body': json.dumps('RDS connection failed')}
    obj = csv.reader(StringIO(s3.get_object(Bucket= '***', Key= '***/***')['Body'].read().decode('utf-8')))
    with con.cursor() as cur:
        for ind,row in enumerate(obj):
            if(len(row)==5 and ind != 0):
                for i in range(0,len(row)):
                    row[i] = row[i].replace("'",'')
                sql = "INSERT INTO UIPATH VALUES ('"+row[0]+"','"+row[1]+"','"+row[2]+"','"+row[3]+"','"+row[4]+"');"
                try:
                    cur.execute(sql)
                except Exception as e:
                    print("Error trying to insert:"+row[0])
                
    con.commit()
    con.close()
    
    
    
    
    
    
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Successfully ran')
    }
