#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  2 09:37:43 2020

@author: chriswigle
"""

import requests
import bs4
import json
import boto3
from botocore.exceptions import ClientError
import os
from io import BytesIO
from boto3.dynamodb.conditions import Key, Attr
import datetime

def lambda_handler(event, context):
    urlDicts = getDicts()
    looseScrapper(urlDicts)
    return {'Result':True}
    
def getDicts():
    sqs = boto3.client('sqs')
    queue_url = '***'
    response = sqs.receive_message(QueueUrl=queue_url)
    values = response['Messages'][0]['Body'].strip('[]').split(', ')
    urlDicts = []
    for i in values:
        dynamodb = boto3.resource('dynamodb', endpoint_url="***")
        table = dynamodb.Table('***')
        response = table.scan(FilterExpression=Attr('freq').eq(int(i)))
        for a in response['Items']:
            urlDicts.append(a)
    return urlDicts
    
    
def looseScrapper(url):
    for x in url:
        webhtml = requests.get(x['url'])
        websitedata = bs4.BeautifulSoup(webhtml.text,"html.parser")
        tags = x['articleTag'].split(',')
        if(len(tags)>1):
            articles = websitedata.find_all(tags[0],json.loads(tags[1]))
        else:
            articles = websitedata.find_all(tags[0])
        if x['secondaryArticleTag'] != '':
            tags = x['secondaryArticleTag'].split(',')
            if(len(tags)>1):
                articles += websitedata.find_all(tags[0],json.loads(tags[1]))
            else:
                articles += websitedata.find_all(tags[0])
        for a in articles:
            ext = a.get(x['suburl'])
            fullUrl = x['url'].replace(x['urlOverlap'],'')+ext
            filename = Punctuation(ext.replace(x['urlOverlap'],''))
            jdump = json.dumps(requests.get(fullUrl).text)
            s3ObjectUpload(jdump,filename+'.json')
            subdata = bs4.BeautifulSoup(requests.get(fullUrl).text,"html.parser")
            imageurls = subdata.findAll(itemprop="image")
            imagenum = 1
            for img in imageurls:
                webimage = requests.get(img.get('content'))
                imgtitle = filename+'_'+str(imagenum)+'.png'
                fp = BytesIO(webimage.content)
                fp.seek(0)
                s3ImageUpload(fp,imgtitle)
                imagenum+=1
        
def Punctuation(string): 
    # punctuation marks 
    punctuations = '''!()[]{};:'"\,<>./?@#$%^&*_~'''
    # traverse the given string and if any punctuation 
    # marks occur replace it with null 
    for x in string.lower(): 
        if x in punctuations: 
            string = string.replace(x, "") 
    # Print string without punctuation 
    return string

def s3ObjectUpload(jsonObj,title):
    s3 = boto3.client('s3')
    current_time = datetime.datetime.now()
    s3.put_object(Body=str(jsonObj),Bucket='***',Key=str(current_time.month)+'-'+str(current_time.year)+'/'+title)

def s3ImageUpload(imgObj,title):
    s3 = boto3.client('s3')
    current_time = datetime.datetime.now()
    s3.put_object(Body=imgObj,Bucket='***',Key=str(current_time.month)+'-'+str(current_time.year)+'/'+'media/'+title)
