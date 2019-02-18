import boto3
import schedule
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
import csv
import time, datetime
#import pymysql
import re, os, sys
# import mysql.connector
# from mysql.connector.constants import ClientFlag
import sys
import io
import MySQLdb
connection = MySQLdb.connect(database='loanfront', host="localhost", user="root", passwd="root")
cursor = connection.cursor()

#connection = pymysql.connect(host="cf-rds-my-dev.cluster-coolstqbhxhn.ap-south-1.rds.amazonaws.com", port=3306, user="cf_rds_my_dev_ad", passwd="RecOngiRtiOnthEChaRGYNOCkUlAtERF", db="loanfront")

#cursor = connection.cursor()
#path = '/'         # "/var/tmp/mysql_csv_files/"

def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
    return dynamodb


def Data_transfer(Tname, fields):  # execute this function only
    print("Data Processing...")
    dynamodb = boto3.resource("dynamodb", region_name='ap-south-1')
    response = dynamodb.Table(Tname).scan()
    cursor.execute("SELECT count(cid) FROM " + Tname)
    i=cursor.fetchone()
    #print('the row count',i[0])
    item = response['Items'][i[0]:]

    if (len(item) == 0):
        print(Tname ,"Sorry No New Data Available In DynamoDB")

    else:
        filename = Tname + str(time.strftime("%Y-%m-%d_%H-%M-%S")) + ".csv"

        #Filename =(path+filename).decode('utf-8')
        #print(type(Filename),Filename)
        Stime = datetime.datetime.now()
        try:

            encoding = "utf-8"
            with io.open(filename, encoding="utf-8",mode='w+') as csvfile:
            # creating a csv dict writer object
                writer = csv.DictWriter(csvfile, fieldnames=fields)
                # writing headers (field names)
                writer.writeheader()
                # writing data rows
                writer.writerows(item)
                # print(writer.writerows(item))
                csvfile.close()
                #Etime = datetime.datetime.now()
                status = "sucess"

        except Exception as e:
            print("Exception:", e)
            status = "Fail"
            Etime = datetime.datetime.now()
            sys.exit()
        # def count_Row(filename, path):
        #     a = path + filename
        #     csvfile = open(a, 'r')
        #     reader = csv.reader(csvfile, delimiter=",")
        #     data = list(reader)
        #     row_count = len(data) - 1
        #     return row_count
        # Rcount = count_Row(filename, path)
        # print(Rcount)
        def newest(path):  # Open the current file
            files = os.listdir(path)
            paths = [os.path.join(path, basename) for basename in files]
            return max(paths, key=os.path.getctime)
        dir = repr(newest(os.getcwd()))
        print(dir,Tname,"Sucessfully Data Insert into Csv file")

        Stime = datetime.datetime.now()
        cursor.execute("SELECT count(*) FROM " + Tname)
        i = cursor.fetchone()
        #print('aaaaaaaaaaaaaaaaaaaaa',i[0])
        print(i)
        sql_insert_query = """LOAD DATA LOCAL INFILE """ + dir + """
            INTO TABLE """ + Tname + """
            COLUMNS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '"'
            ESCAPED BY '"'
            LINES TERMINATED BY '\n'
            IGNORE 1 ROWS;"""
        try:
            print(sql_insert_query)
            cursor.execute(sql_insert_query)
            connection.commit()
            Etime = datetime.datetime.now()
            status = "Sucess"
            print("Sucessfully Data Insert into MySQl DataBase")
        except Exception as e:
            print("Exception:\t",Tname, e)
            Etime = datetime.datetime.now()
            status = "Fail"
            print("Some Error comming")
        #i1 = cursor.execute("SELECT count(*) FROM " + Tname)
        #print('aaaaaaaaaaaaa',i,i1)
        #Rcount = (i1 - i)
        # table_info(Filename, Tname,Stime, Etime, Rcount, status)


def newest(path):  # Open the current file
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)


def Allfunction():
    # if os.path.exists(os.path.join(path)) == True:
    #     if os.access(path, os.W_OK) == True:

            Tname = "lf_app"
            fields = ['cid', 'pkgName', 'installTime', 'lastUpdateTime', 'syncId', 'versionCode', 'name']
            Data_transfer(Tname, fields)

            Tname = "lf_call_log"
            fields = ['cid', 'time', 'duration', 'number', 'syncId', 'type']
            Data_transfer(Tname, fields)

            Tname = "lf_contact"
            fields = ['cid', 'number', 'favourite', 'group', 'modifiedAt', 'name', 'relationShip', 'syncId', 'type',
                      'otherNumbers', 'title']
            Data_transfer(Tname, fields)

            Tname = "lf_device_detail"
            fields = ['cid', 'imei', 'device', 'deviceId', 'model', 'ram', 'sdCapacity', 'syncId', 'time', 'imei2']
            Data_transfer(Tname, fields)

            Tname = "lf_location"
            fields = ['cid', 'time', 'cellId', 'cellLac', 'cellPos', 's2CellId', 's2CellLac', 'syncId', 'latLng']
            Data_transfer(Tname, fields)

            Tname = "lf_sms"
            fields = ['cid', 'time', 'body', 'from', 'syncId']
            Data_transfer(Tname, fields)

            Tname = "lf_sync"
            fields = ['cid', 'time', 'failedContacts', 'numCalllog', 'numContacts', 'numSms', 'osVersion', 'rooted',
                      'syncId', 'syncStatus']
            Data_transfer(Tname, fields)

            Tname = "lf_telephony_info"
            fields = ['cid', 'time', 'carrier_name', 'data', 'roam', 'sim_mcc', 'sim_mnc', 'syncId', 'mcc', 'mnc',
                      'operator', 's2Roam', 'sim2_mcc', 'sim2_mnc', 'carrier2_name']
            Data_transfer(Tname, fields)

            Tname = "lf_user_calllogs_sync"
            fields = ['cid', 'entryTime', 'syncId']
            Data_transfer(Tname, fields)
    #     else:
    #         print('Your folder dont have to write permissins')
    # else:
    #     print("Please create Path "+path)

print("Thank You ")


if __name__ == '__main__':
    Allfunction()
