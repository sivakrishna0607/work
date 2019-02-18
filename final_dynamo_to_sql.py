import boto3
import schedule
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
import csv
import time, datetime
import pymysql
import re, os, system
# import mysql.connector
# from mysql.connector.constants import ClientFlag
import sys
import io
connection = pymysql.connect(database='loanfront', host="localhost", user="root", passwd="root")
cursor = connection.cursor()

# connection = mysql.connector.connect(user='cf_rds_my_dev_ad', password='RecOngiRtiOnthEChaRGYNOCkUlAtERF', database='loanfront', host='cf-rds-my-dev.cluster-coolstqbhxhn.ap-south-1.rds.amazonaws.com', port='3306', client_flags=[ClientFlag.LOCAL_FILES])
# cursor = connection.cursor(buffered=True)
# from mysql.connector.constants import ClientFlag
dynamodb = boto3.resource("dynamodb",region_name ='ap-south-1')
i=0
row_count=0
def func1(a):
    response= dynamodb.Table(table_name).scan()
    before=cursor.execute('SELECT * FROM '+table)
    print(before)
    item=response['Items'][before:]
    print(table_name,item)
    if len(item) == 0:
        print('No new elements in dynamodb')

    elif os.path.exists(os.path.join('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/'))== True:
        if os.access('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/', os.W_OK)== True:
            filename = table_name+str(time.strftime("%Y%m%d-%H%M%S"))+".csv"
            start_time=datetime.datetime.now()
        try:
            print('aa')
            with open('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/'+filename, 'w+',newline='',encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames = fields,extrasaction='ignore')
                writer.writeheader()
                writer.writerows(item)
            with open('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/'+filename,"r",encoding='utf-8') as f:
                reader = csv.reader(f)
                data = list(reader)
                row_count = len(data)-1
                end_time=datetime.datetime.now()
                status='Sucess'
                print('csv sucess')
        except:
            with open('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/'+filename,"r") as f:
                reader = csv.reader(f)
                data = list(reader)
                row_count = len(data)-1
            end_time=datetime.datetime.now()
            status='Fail'
            print('CSV',status)

        # sql_insert_query = """ INSERT INTO TOTEL2(TABLENAME,CSVNAME,STARTTIME,ENDTIME,INSERTROWCOUNT,STATUS)VALUES (%s,%s,%s,%s,%s,%s)"""
        # insert=[filename,table_name,start_time,end_time,row_count,status]
        # result  = cursor.execute(sql_insert_query, insert)
        # conn.commit()
        # print('ddddddddddddddd')
####==================================csv to sql data transfer ======================================#####################
        def newest(path):
            files = os.listdir(path)
            paths = [os.path.join(path, basename) for basename in files]
            return max(paths, key=os.path.getctime)
        am=repr(newest('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/'))
        csv_file=re.findall(('\w+'+'-'+'\w+'+'.csv'),am)
        print('THIS IS FULL',am)
        try:
            def csv_to_mysql():
                start_time=datetime.datetime.now()
                def str_concat():

                    sql = "LOAD DATA LOCAL INFILE "+am+" INTO TABLE "+table+" FIELDS TERMINATED BY ','" \
                            " OPTIONALLY ENCLOSED BY '\\\"' ESCAPED BY '\\\\\\\\' LINES TERMINATED BY '\\\\r\\\\n';"

                    return str(sql)
                str_concat()
                system("mysql -u $dbUser -h $dbHost --password=$dbPass --local_infile=1 -e \"$sql\" $dbName")
                cursor.execute(str_concat())
                cursor.commit()
                end_time=datetime.datetime.now()
                print(end_time)
                status='Sucess'
                print(status)
            csv_to_mysql()
        except Exception as e:
            print(e)
            end_time=datetime.datetime.now()
            status='Fail'
            print(status)

        def totel_result():
            sql_insert_query = """ INSERT INTO TOTEL1(CSVNAME,TABLENAME,STARTTIME,ENDTIME,INSERTROWCOUNT,STATUS)VALUES (%s,%s,%s,%s,%s,%s)"""
            after=cursor.execute(str('SELECT * FROM '+table))
            row_count=after-before
            insert=[csv_file,table_name,start_time,end_time,row_count,status]
            result  = cursor.execute(sql_insert_query, insert)
            cursor.commit()
        #totel_result()
    else:
        print('please create "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/" path ')


for x in range(1,10):
    print(x)
    if x==1:
        table_name='lf_app'
        table='lf_app'
        fields= ['cid', 'pkgName', 'installTime', 'lastUpdateTime','syncId','versionCode']
        func1(fields)
    elif x==2:
        table_name='lf_call_log'
        table='lf_call_log'
        fields=['cid','time','duration','number','syncId','type']
        func1(fields)
    elif x==3:
        table_name='lf_contact'
        table='LF_CONTACT'
        fields=['cid', 'number', 'favourite','group','modifiedAt','name','relationShip','syncId','type','title','otherNumbers']
        func1(fields)
    # elif x==4:
    #     table_name='lf_device_detail'
    #     table='LF_DEVICE_DETAILS'
    #     fields=['cid','imei','device','deviceId','model','ram','sdCapacity','syncId','time','imei2']
    #     func1(fields)
    elif x==5:
        table_name='lf_location'
        table='LF_LOCATION'
        fields=['cid','time', 'cellId','cellLac','cellPos','s2CellId','s2CellLac','syncId','latLng']
        func1(fields)
    elif x==6:
        table_name='lf_sms'
        table='LF_SMS'
        fields=['cid','time','body','from','syncId']
        func1(fields)
    elif x==7:
        table_name='lf_sync'
        table='LF_SYNC'
        fields=['cid','time', 'syncId','failedContacts','numCalllog','numContacts','numSms','osVersion','rooted','syncStatus']
        func1(fields)
    elif x==8:
        table_name='lf_telephony_info'
        table='LF_TELEPHONY_INFO'
        fields=['cid','time','carrier_name','data','roam','sim_mcc','sim_mnc','syncId','mcc','mnc','operator','s2Roam','sim2_mcc','sim2_mnc','carrier2_name']
        func1(fields)
    elif x==9:
        table_name='lf_user_calllogs_sync'
        table='LF_USER_CALLLOGS_SYNC'
        fields=['cid','entryTime','syncId']
        func1(fields)



