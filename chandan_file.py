import boto3
import decimal
from boto3.dynamodb.conditions import Key, Attr
import csv
import time,datetime
import pymysql
import re,os,sys
connection = pymysql.connect(database='loanfront', host="localhost", user="root", passwd="root")
cursor = connection.cursor()
def get_dynamodb_resource():
    dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
    return dynamodb


def Data_transfer(Tname,fields,change): # execute this function only
    print("Data Processing...")
    dynamodb = boto3.resource("dynamodb", region_name='ap-south-1')
    response = dynamodb.Table(Tname).scan()
    i = cursor.execute("SELECT * FROM "+Tname)
    item = response['Items'][i:]
    if(len(item)==0):
        print("Sorry No New Data Available In DynamoDB")
        #sys.exit()
    else:
        filename = Tname + str(time.strftime("%Y-%m-%d_%H-%M-%S")) + ".csv"
        Filename=filename
        Stime=datetime.datetime.now()
        try:
            csvfile = open('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/'+filename, 'w+',newline='',encoding="utf8")
            # creating a csv dict writer object
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            # writing headers (field names)
            writer.writeheader()
            # writing data rows
            writer.writerows(item)
            # print(writer.writerows(item))
            csvfile.close()
            Etime=datetime.datetime.now()
            status="sucess"

        except Exception as e:
            print("Exception:",e)
            status="Fail"
            Etime = datetime.datetime.now()
        Rcount=count_Row(filename)
        #print(Rcount)
        dir = repr(newest('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/'))
        print("Sucessfully Data Insert into Csv file")
        #table_info(Filename,Tname, Stime, Etime, Rcount, status)
        exexute(Tname,Filename,dir,fields,change)


def count_Row(filename):
    csvfile = open('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/' + filename, 'r',encoding="utf8")
    reader = csv.reader(csvfile, delimiter=",")
    data = list(reader)
    row_count = len(data)-1
    return row_count


# def table_info(Filename,Tname, Stime, Etime, Rcount, status):
#     sql = """ INSERT INTO data_capture(Filename,Tname,Stime,Etime,RCount,status) VALUES (%s,%s,%s,%s,%s,%s)"""
#     value = [Filename, Tname, Stime, Etime, Rcount, status]
#     cursor.execute(sql, value)
#     connection.commit()

def exexute(Tname,Filename,dir,fields,change):
    Stime=datetime.datetime.now()
    i = cursor.execute("SELECT * FROM "+Tname)
    try:
        sql_insert_query = """LOAD DATA INFILE """+dir+"""
        INTO TABLE """+Tname+"""
        COLUMNS TERMINATED BY ','
        OPTIONALLY ENCLOSED BY '"'
        ESCAPED BY '"'
        LINES TERMINATED BY '\n'"""
        repr(fields)+change+';'

        cursor.execute(sql_insert_query)
        connection.commit()
        Etime=datetime.datetime.now()
        status="Sucess"

        print("Sucessfully Data Insert into MySQl DataBase")
    except Exception as e:
        print("Exception:\t",e)
        Etime=datetime.datetime.now()
        status="Fail"
        print("Some Error comming")

    i1 = cursor.execute("SELECT * FROM "+Tname)
    Rcount=(i1-i)
    #table_info(Filename, Tname,Stime, Etime, Rcount, status)

def newest(path): # Open the current file
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)

def function():
    if os.path.exists(os.path.join('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/')) == True:
        if os.access('C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/', os.W_OK) == True:

                Tname="lf_app"
                fields1 = ('cid', 'pkgName', 'installTime', 'lastUpdateTime', 'syncId', 'versionCode', 'name')
                fields = ('cid', 'pkgName', 'installTime', 'lastUpdateTime', 'syncId', 'versionCode', 'name')
                change=''
                Data_transfer(Tname,fields,change)

                Tname="lf_call_log"
                fields1 = ('cid', 'call_time', 'duration', 'number', 'syncId', 'type')
                fields = ('cid', 'time', 'duration', 'number', 'syncId', 'type')
                change=' set call_time=time'
                Data_transfer(Tname,fields,change)

                Tname="lf_contact"
                fields1 = ('cid', 'contact_number', 'favourite', 'contact_group', 'modifiedAt', 'name', 'relationShip', 'syncId', 'contact_type',
                          'otherNumbers', 'title')
                fields = ('cid', 'number', 'favourite', 'group', 'modifiedAt', 'name', 'relationShip', 'syncId',
                          'type','otherNumbers', 'title')
                change = ' set contact_number=number,contact_group=group,contact_type=type'
                Data_transfer(Tname,fields,change)

                Tname="lf_device_detail"
                fields1 = ('cid', 'imei', 'device', 'deviceId', 'model', 'ram', 'sdCapacity', 'syncId', 'time', 'imei2')
                fields = ('cid', 'imei', 'device', 'deviceId', 'model', 'ram', 'sdCapacity', 'syncId', 'time', 'imei2')
                change = ''
                Data_transfer(Tname,fields,change)

                Tname="lf_location"
                fields1 = ('cid', 'time', 'cellId', 'cellLac', 'cellPos', 's2CellId', 's2CellLac', 'syncId', 'latLng')
                fields = ('cid', 'time', 'cellId', 'cellLac', 'cellPos', 's2CellId', 's2CellLac', 'syncId', 'latLng')
                change = ''
                Data_transfer(Tname,fields,change)

                Tname="lf_sms"
                fields1 = ('cid', 'time', 'body', 'msg_from', 'syncId')
                fields = ('cid', 'time', 'body', 'from', 'syncId')
                change = ''
                Data_transfer(Tname,fields,change)

                Tname="lf_sync"
                fields1 = ('cid', 'time', 'failedContacts', 'numCalllog', 'numContacts', 'numSms', 'osVersion', 'rooted',
                          'syncId', 'syncStatus')
                fields = ('cid', 'time', 'failedContacts', 'numCalllog', 'numContacts', 'numSms', 'osVersion', 'rooted',
                          'syncId', 'syncStatus')
                change = ''
                Data_transfer(Tname,fields,change)

                Tname="lf_telephony_info"
                fields1 = ('cid', 'time', 'carrier_name', 'data', 'roam', 'sim_mcc', 'sim_mnc', 'syncId', 'mcc', 'mnc',
                          'operator', 's2Roam', 'sim2_mcc', 'sim2_mnc', 'carrier2_name')
                fields = ('cid', 'time', 'carrier_name', 'data', 'roam', 'sim_mcc', 'sim_mnc', 'syncId', 'mcc', 'mnc',
                          'operator', 's2Roam', 'sim2_mcc', 'sim2_mnc', 'carrier2_name')
                change = ''
                Data_transfer(Tname,fields,change)

                Tname="lf_user_calllogs_sync"
                fields1 = ('cid', 'entryTime', 'syncId')
                fields = ('cid', 'entryTime', 'syncId')
                change = ''
                Data_transfer(Tname,fields,change)
        else:
            print('Your folder dont have to write permissins')
    else:
        print("Please create Path  'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/'")

    print("Thank You ")

if __name__ == '__main__':
    function()
