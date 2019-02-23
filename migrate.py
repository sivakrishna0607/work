import boto3
import csv
import datetime,time
import os
import MySQLdb
import sys
import yaml

# connection = MySQLdb.connect(database='loanfront', host="localhost", user="root", passwd="root")
# cursor = connection.cursor()
#
connection = MySQLdb.connect('cf-rds-dev.cluster-coolstqbhxhn.ap-south-1.rds.amazonaws.com', 'cf_rds_my_dev_ad', 'RecOngiRtiOnthEChaRGYNOCkUlAtERF', 'loanfront')
cursor = connection.cursor()

# connection = pymysql.connect(host="cf-rds-my-dev.cluster-coolstqbhxhn.ap-south-1.rds.amazonaws.com", port=3306, user="cf_rds_my_dev_ad", passwd="RecOngiRtiOnthEChaRGYNOCkUlAtERF", db="loanfront")
# cursor = connection.cursor()
# path = "/var/tmp/mysql_csv_files/"


def get_dynamodb_resource():

    dynamodb = boto3.resource("dynamodb", region_name="ap-south-1")
    return dynamodb


def scan_table_allpages(table_name):

    dynamodb = get_dynamodb_resource()
    table = dynamodb.Table(table_name)

    sql = 'SELECT TABLE_NAME, LASTKEY FROM dynamo_lastkey WHERE TABLE_NAME={} and LASTKEY<> "None"' \
                                        ' ORDER BY id DESC LIMIT 1'.format(repr(table_name))
    cursor.execute(sql)
    lastkey1=cursor.fetchone()
    print("last key 1",lastkey1)
    sql1 = 'SELECT COUNT FROM dynamo_lastkey WHERE TABLE_NAME={} and LASTKEY= "None"' \
          ' ORDER BY id DESC LIMIT 1'.format(repr(table_name))
    cursor.execute(sql1)
    count = cursor.fetchone()
    print("Count ",count)
    if count == None:
        count1=0
    else:
        count1 = count[0]

    lastkey = 1
    response = 'No'
    items = []
    if lastkey1 != None :
        lastkey = yaml.load(lastkey1[1])
    else:
        response = table.scan()
        sql = 'INSERT INTO dynamo_lastkey(TABLE_NAME,LASTKEY,COUNT)VALUES({},"{}",{})'.format(repr(table_name),
                                                                                         response.get('LastEvaluatedKey'),
                                                                                         len(response['Items']))
        cursor.execute(sql)
        # lastkey = response.get('LastEvaluatedKey')
        connection.commit()
    counter = 0
    while True:
        counter += 1
        if lastkey:
            print("Last Key ",lastkey)
            if response == 'No' :
                print("Getting response")
                response = table.scan(ExclusiveStartKey=lastkey)
            #print(response)
            print("Items length ",len(response['Items']))
            items += response['Items']
            lsk = response.get('LastEvaluatedKey')
            print("Counter ",counter," Lsk ",lsk)
            if (counter == 1 and lsk == None):
                break
            else :
                sql = 'INSERT INTO dynamo_lastkey(TABLE_NAME,LASTKEY,COUNT)VALUES({},"{}",{})'.format(repr(table_name),
                                                                                          lsk,
                                                                                          len(items))
                print(sql)
                print("Item Length ",len(items))
                cursor.execute(sql)
                connection.commit()
                lastkey = lsk
                response = 'No'
        else:
            break

    return items[count1:]


def Data_transfer(Tname, fields, change):  # execute this function only
    items = scan_table_allpages(Tname)
    print("Final items length ",len(items))
    #sys.exit()
    if not items:
        print(Tname, ": No New Data Available In DynamoDB")
    elif len(items[0]) == 0:
        print(Tname, ": No New Data Available In DynamoDB")
    else:
        filename = Tname +"_"+str(time.strftime("%Y-%m-%d_%H-%M-%S")) + ".csv"

        # Filename =(path+filename).decode('utf-8')
        # print(type(Filename),Filename)
        Stime = datetime.datetime.now()

        try:
            encoding = "utf-8"
            with open(filename, encoding="utf-8", mode='w+') as csvfile:
                # creating a csv dict writer object
                writer = csv.DictWriter(csvfile, fieldnames=fields,extrasaction='ignore')
                # writing headers (field names)
                writer.writeheader()
                # writing data rows
                writer.writerows(items)
                # print(writer.writerows(item))
                csvfile.close()
                # Etime = datetime.datetime.now()
                status = "sucess"


        except Exception as e:
            print("Exception:", e)
            status = "Fail"
            Etime = datetime.datetime.now()
            sys.exit()

        def newest(path):  # Open the current file
            files = os.listdir(path)
            paths = [os.path.join(path, basename) for basename in files]
            return max(paths, key=os.path.getctime)

        dir = repr(newest(os.getcwd()))
        print(Tname, "Sucessfully Data Insert into Csv file")

        Stime = datetime.datetime.now()
        cursor.execute("SELECT count(*) FROM " + Tname)
        i = cursor.fetchone()
        # print('aaaaaaaaaaaaaaaaaaaaa',i[0])
        # print(i)
        try:
            sql_insert_query = """LOAD DATA LOCAL INFILE """ + dir + """
                INTO TABLE """ + Tname + """
                COLUMNS TERMINATED BY ','
                OPTIONALLY ENCLOSED BY '"'
                ESCAPED BY '"'
                LINES TERMINATED BY '\n'"""
            repr(fields) + change + ';'

            cursor.execute(sql_insert_query)
            connection.commit()
            Etime = datetime.datetime.now()
            status = "Sucess"
            print("Sucessfully Data Insert into MySQl DataBase")
        except Exception as e:
            print("Exception:\t", Tname, e)
            Etime = datetime.datetime.now()
            status = "Fail"
            print("Some Error comming")
        # i1 = cursor.execute("SELECT count(*) FROM " + Tname)
        # print('aaaaaaaaaaaaa',i,i1)
        # Rcount = (i1 - i)
        # table_info(Filename, Tname,Stime, Etime, Rcount, status)

def newest(path):  # Open the current file
    files = os.listdir(path)
    paths = [os.path.join(path, basename) for basename in files]
    return max(paths, key=os.path.getctime)

def Allfunction():

    Tname = "lf_app"
    fields = ('cid', 'pkgName', 'installTime', 'lastUpdateTime', 'syncId', 'versionCode', 'name')
    change = ''
    Data_transfer(Tname, fields, change)

    Tname = "lf_call_log"
    fields = ('cid', 'time', 'duration', 'number', 'syncId', 'type')
    change = ' call_time=time'
    Data_transfer(Tname, fields, change)

    Tname = "lf_contact"
    fields = ('cid', 'number', 'favourite', 'group', 'modifiedAt', 'name', 'relationShip', 'syncId', 'type',
             'otherNumbers', 'title')
    change = ' set contact_number=number,contact_group=group,contact_type=type'
    Data_transfer(Tname, fields, change)

    Tname = "lf_device_detail"
    fields = ('cid', 'imei', 'device', 'deviceId', 'model', 'ram', 'sdCapacity', 'syncId', 'time', 'imei2')
    change = ''
    Data_transfer(Tname, fields, change)

    Tname = "lf_location"
    fields = ('cid', 'time', 'cellId', 'cellLac', 'cellPos', 's2CellId', 's2CellLac', 'syncId', 'latLng')
    change = ''
    Data_transfer(Tname, fields, change)

    Tname = "lf_sms"
    fields = ('cid', 'time', 'body', 'from', 'syncId')
    change = ''
    Data_transfer(Tname, fields, change)

    Tname = "lf_sync"
    fields = ('cid', 'time', 'failedContacts', 'numCalllog', 'numContacts', 'numSms', 'osVersion', 'rooted',
             'syncId', 'syncStatus')
    change = ''
    Data_transfer(Tname, fields, change)

    Tname = "lf_telephony_info"
    fields = ('cid', 'time', 'carrier_name', 'data', 'roam', 'sim_mcc', 'sim_mnc', 'syncId', 'mcc', 'mnc',
             'operator', 's2Roam', 'sim2_mcc', 'sim2_mnc', 'carrier2_name')
    change = ''
    Data_transfer(Tname, fields, change)

    Tname = "lf_user_calllogs_sync"
    fields = ('cid', 'entryTime', 'syncId')
    change = ''
    Data_transfer(Tname, fields, change)

print("Thank You ")

if __name__ == '__main__':
    Allfunction()
