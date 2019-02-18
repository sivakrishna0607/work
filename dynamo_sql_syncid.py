import boto3,csv,re,os
import time,datetime
import pymysql
import sys
conn = pymysql.connect(host="localhost", port=3306, user="root", passwd="root", db="testdb1")
cursor = conn.cursor()
dynamodb = boto3.resource("dynamodb",region_name ='ap-south-1')
i=0
row_count=0
table_name='lf_app'
def func1():
    response= dynamodb.Table(table_name).scan()
    item=response['Items']
    print(item)
func1()