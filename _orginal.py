import boto3
import schedule
import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
import csv
import time, datetime
#import pymysql
import re, os, sys
import mysql.connector
from mysql.connector.constants import ClientFlag
import sys
import io
# connection = pymysql.connect(database='loanfront', host="localhost", user="root", passwd="root")
# cursor = connection.cursor()
connection = mysql.connector.connect(user='cf_rds_my_dev_ad', password='RecOngiRtiOnthEChaRGYNOCkUlAtERF', database='loanfront', host='cf-rds-my-dev.cluster-coolstqbhxhn.ap-south-1.rds.amazonaws.com', port='3306', client_flags=[ClientFlag.LOCAL_FILES])
cursor = connection.cursor(buffered=True)

#connection = pymysql.connect(host="cf-rds-my-dev.cluster-coolstqbhxhn.ap-south-1.rds.amazonaws.com", port=3306, user="cf_rds_my_dev_ad", passwd="RecOngiRtiOnthEChaRGYNOCkUlAtERF", db="loanfront")

#cursor = connection.cursor()
#path = "/var/tmp/mysql_csv_files/"

def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
    return dynamodb
def Data_transfer(Tname, fields,change):  # execute this function only
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


            with io.open(filename, mode='w+',encoding="utf-8") as csvfile:
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
            print('chnadan kumar')
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
        print(Tname,"Sucessfully Data Insert into Csv file")

        Stime = datetime.datetime.now()
        cursor.execute("SELECT count(*) FROM " + Tname)
        i = cursor.fetchone()
        #print('aaaaaaaaaaaaaaaaaaaaa',i[0])
        #print(i)
        try:
            sql_insert_query = """LOAD DATA LOCAL INFILE """ + dir + """
            INTO TABLE """ + Tname + """
            COLUMNS TERMINATED BY ','
            OPTIONALLY ENCLOSED BY '"'
            ESCAPED BY '"'
            LINES TERMINATED BY '\n'"""
            repr(fields)+change+';'

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


    Tname = "lf_app"
    fields = ('cid', 'pkgName', 'installTime', 'lastUpdateTime', 'syncId', 'versionCode', 'name')
    change=''
    Data_transfer(Tname, fields,change)

    Tname = "lf_call_log"
    fields = ('cid', 'time', 'duration', 'number', 'syncId', 'type')
    change=' call_time=time'
    Data_transfer(Tname, fields,change)

    Tname = "lf_contact"
    fields = ('cid', 'number', 'favourite', 'group', 'modifiedAt', 'name', 'relationShip', 'syncId', 'type',
              'otherNumbers', 'title')
    change = ' set contact_number=number,contact_group=group,contact_type=type'
    Data_transfer(Tname, fields,change)

    Tname = "lf_device_detail"
    fields = ('cid', 'imei', 'device', 'deviceId', 'model', 'ram', 'sdCapacity', 'syncId', 'time', 'imei2')
    change = ''
    Data_transfer(Tname, fields,change)

    Tname = "lf_location"
    fields = ('cid', 'time', 'cellId', 'cellLac', 'cellPos', 's2CellId', 's2CellLac', 'syncId', 'latLng')
    change = ''
    Data_transfer(Tname, fields,change)

    Tname = "lf_sms"
    fields = ('cid', 'time', 'body', 'from', 'syncId')
    change = ''
    Data_transfer(Tname, fields,change)

    Tname = "lf_sync"
    fields = ('cid', 'time', 'failedContacts', 'numCalllog', 'numContacts', 'numSms', 'osVersion', 'rooted',
              'syncId', 'syncStatus')
    change = ''
    Data_transfer(Tname, fields,change)

    Tname = "lf_telephony_info"
    fields = ('cid', 'time', 'carrier_name', 'data', 'roam', 'sim_mcc', 'sim_mnc', 'syncId', 'mcc', 'mnc',
              'operator', 's2Roam', 'sim2_mcc', 'sim2_mnc', 'carrier2_name')
    change = ''
    Data_transfer(Tname, fields,change)

    Tname = "lf_user_calllogs_sync"
    fields = ('cid', 'entryTime', 'syncId')
    change = ''
    Data_transfer(Tname, fields,change)


print("Thank You ")


if __name__ == '__main__':
    Allfunction()
