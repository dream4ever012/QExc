# -*- coding: utf-8 -*-
"""
Created on Mon Jul 02 20:15:00 2018

@author: hkim85
"""
import pyzilla
import os
import jaydebeapi
import pandas as pd
from datetime import datetime
import requests



PATH = 'C:\\Users\\hkim85.823WS2016-03\\OneDrive\\Documents\\Caleb\\2016_PhDinCS\\201603_IndStudy\\201709IndStu\\InfusionPump\\InfPump_Data_20170923\\'
PATH = 'C:\\Users\\hkim85\\Documents\\h2\\bin'#'C:\\Program Files (x86)\\H2\\bin'
os.chdir(PATH)
os.getcwd()



idd = ""
pw = ""
url="tcp://140.192.38.17:8094"
dbName="test2"
conn = jaydebeapi.connect("org.h2.Driver", # driver class
                           "jdbc:h2:{}/~/Downloads/{}".format(url, dbName),
                           ["", ""], # credentials\
                           "C://Program Files (x86)//H2//bin//h2-1.4.196.jar")

def get_connection(url="tcp://140.192.38.17:8094", dbName="test2", idd = "", pw = "",\
                   jarPath = "C://Program Files (x86)//H2//bin//h2-1.4.196.jar"):
    try:
        conn = jaydebeapi.connect("org.h2.Driver", # driver class
                           #"jdbc:h2:~/test",
                           "jdbc:h2:{}/~/Downloads/{}".format(url, dbName),
                           #"jdbc:h2:tcp://localhost:8082/~/test",
                           #"jdbc:h2:tcp:localhost:9123/mem:testdb", # JDBC url
                           [idd, pw], # credentials
                           jarPath)#"./h2-1.4.196.jar")## # location of H2 jar
    except Exception as e:
        print e
        print "WARNING! Connection failed."
        return None
    return conn

def get_cursor(conn):
	try:
		cur = conn.cursor()
	except:
		print "WARNING! Cursor Failed."
		return None
	return cur



conn_test2 = get_connection("tcp://140.192.38.17:8094", "test2")
cur_test2 = get_cursor(conn_test2)

cur_test2.execute("SELECT changed FROM BUGS;")
res = cur_test2.fetchone()

import jpype
classpath = "C:\Program Files\Java\jdk-10.0.2\bin\server"
jpype.getDefaultJVMPath() 
jpype.startJVM("C:\Program Files\Java\jdk-10.0.2\bin\server\jvm.dll", "-ea", "-Djava.class.path=%s" % classpath)
import ctypes
ctypes.CDLL('C:\\Program Files\\Java\\jdk-10.0.2\\bin\\server\\jvm.dll')
PATH ="C:\Program Files\Java\jdk-10.0.2\bin"
os.chdir(PATH)
os.getcwd()
import ctypes
ctypes.CDLL('C:\\Program Files\\Java\\jdk-10.0.2\\bin\\server\\jvm.dll')
conn_test2 = get_connection("tcp://140.192.38.17:8094", "test2")
cur = get_cursor(conn_test2)

"""
###########################
0) Initial loading
URL = 'https://bugzilla.mozilla.org/rest/bug?include_fields=id,product,component,assigned_to,status,resolution,summary,last_change_time&product=Firefox&resolution=---'
1) get result ==> pandas
2) pandas ==> csv
3) csv ==> h2 upload
PATH = 'C:\\Users\\hkim85\\Documents\\GitHub\\QEng-master\\QEng\\Data\\bugs\\'
os.chdir(PATH)
os.getcwd()

csv_filename_bugs = 'bugs.csv'

'AS SELECT * FROM CSVREAD('bugs.csv')'
sql = 'DROP TABLE bugs IF EXISTS; CREATE TABLE bugs AS SELECT * FROM CSVREAD('C:\\Users\\hkim85.823WS2016-03\\Documents\\GitHub\\QEng\\Data\\bugs\\bugs.csv' )'
cur.execute(sql)
conn.commit()
###########################
"""
cur.execute("SELECT COUNT(*) FROM BUGS;")
row = cur.fetchone()

cur.execute("SELECT changed FROM BUGS;")

cur.execute("SELECT changed FROM BUGS WHERE 'BUG ID' = '265048';")
res = cur.fetchone()
type(res)
# getting list of datetime obj

def runQuery(cur, sql):
    cur.execute(sql)
    return cur

def getChangedCol(cur, col_name='changed', tbl_name= 'BUGS'):
    """ input: cursor, column name and table name
        output: rs: selected column """
    sql = "SELECT {} FROM {}".format(col_name, tbl_name)
    return runQuery(cur, sql)
    
def checkIfIdExist(cur, Id, pred = '', col_name='BUG ID', tbl_name= 'BUGS'):
    sql = "SELECT {} FROM {} WHERE {}BUG ID{} = {}".format(col_name, tbl_name, '"', '"', Id)
    return runQuery(cur, sql)

col_name='BUG ID'
'dfd {}{}{} '.format('"',col_name,'"')

def dateStrTodtObj(cur):
    """ input: cur
        output: list of datetime obj"""
    date_list = []
    while True:
        from datetime import datetime
        row = cur.fetchone()
        if row == None:
            break
        # append
        date_list.append(datetime.strptime(row[0], '%m/%d/%Y %H:%M'))
    return date_list

def getLatest(date_list):
    """ input: list of datetime object
        output: latest datetime object"""
    data_list_tup = [(idx, date_obj) for idx, date_obj in enumerate(date_list)]
    data_list_tup.sort(key=lambda tup: tup[1], reverse=True)
    return data_list_tup[0]

def getLatestDatetime(cur, col_name='changed', tbl_name= 'BUGS'):
    #cur.execute("SELECT {} FROM {}".format(col_name, tbl_name))
    getChangedCol(cur, col_name, tbl_name)
    date_list = dateStrTodtObj(cur)
    return getLatest(date_list)[1]

latestDatetime = getLatestDatetime(cur, col_name='changed', tbl_name= 'BUGS')

# sort and get the latest one



'https://bugzilla.mozilla.org/rest/bug?include_fields={}&classification=Client%20Software&product=Firefox&resolution=---'

'https://bugzilla.mozilla.org/rest/bug?include_fields=id,summary,status&chfieldfrom=2018-07-05T18:01:30Z&chfieldto=2018-07-08T18:01:30Z&product=Firefox&resolution=---'

# 


"""
##############
Enums: will be extended
##############
"""

from enum import Enum

class Classfic(Enum):
    ClientSoftware = 'Client%20Software'
    Components = 'Components'
    ServerSoftware = 'Server%20Software'
    Other = 'Other'
    Graveyard = 'Graveyard'

    def __str__(self):
        return self.value

    def __repr__(self):
        return self.value
    
    def __eq__(self, other):
        return self.value == other

class Product(Enum):
    Add_onSDK = 'Add-on%20SDK'
    Firefox = 'Firefox'

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value
    
    def __eq__(self, other):
        return self.value == other
    
class Resolution(Enum):
    ___ = '---'
    FIXED = 'FIXED'
    INVALID = 'INVALID'

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value
    
    def __eq__(self, other):
        return self.value == other
    
class ECT(Enum):
    FROM = 'chfieldfrom='
    TO = 'chfieldto='
    NOW = 'Now'
    Fields = 'include_fields='

    def __str__(self):
        return self.value
    
    def __repr__(self):
        return self.value

    def __eq__(self, other):
        return self.value == other

ECT.NOW
    
class Fields(Enum):
    AdvSearch = 'id,product,component,assigned_to,status,resolution,summary,last_change_time'
    ID = 'id'
    PROD = 'product'
    COMP = 'component'
    ASSGtO = 'assigned_to'
    RESOL = 'resolution'
    SUMM = 'summary'
    LCtIME = 'last_change_time'

    def __repr__(self):
        return self.value
    
    def __str__(self):
        return self.value

    def __eq__(self, other):
        return self.value == other


"""
###########################
1) Initial loading
###########################
"""

def getURLstring(fromDT, toDT=ECT.NOW, fields= Fields.AdvSearch, bASE_URL= 'https://bugzilla.mozilla.org/rest/bug?'):
    BASE_URL = bASE_URL
    include_fields = 'include_fields={}'.format(fields)
    timeFromto = '&chfieldfrom={}&chfieldto={}'.format(fromDT, toDT)
    ect = '&product=Firefox&resolution=---'
    URL = BASE_URL + include_fields + timeFromto + ect
    return URL

check_URL = 'https://bugzilla.mozilla.org/rest/bug?include_fields=id,product,component,assigned_to,status,resolution,summary,last_change_time&chfieldfrom=2018-07-07T21:32:00Z&chfieldto=Now&product=Firefox&resolution=---'

check_URL = 'https://bugzilla.mozilla.org/rest/bug?include_fields=id,product,component,assigned_to,status,resolution,summary,last_change_time&chfieldfrom=2006-01-01T00:00:00Z&chfieldto=Now&product=Firefox&resolution=---'


latestDatetime = getLatestDatetime(cur, col_name='changed', tbl_name= 'BUGS')

""" URL"""
URL = getURLstring(latestDatetime.strftime("%Y-%m-%dT%H:%M:%SZ"), fields= Fields.AdvSearch)

URL1 = getURLstring(latestDatetime.isoformat()+"Z", fields= Fields.AdvSearch)
URL == URL1
"""
###########################
2) Get update REST API
###########################
"""
"""REST API """
fromDT1_str = '2018-07-07T21:32:00Z'
toDT1_str = '2018-07-08T18:01:30Z'
toDT1_str = '2018-07-15T00:00:00Z'#'Now'
URL = getURLstring(fromDT1_str, toDT1_str)

# update result
r = requests.get(URL)
res = r.json()

res.keys()
ECT.Fields

   

def getListOfDic(fromDT, toDT):
    """ input from to """
    from datetime import datetime
    # convert to str if datetime
    if isinstance(fromDT, datetime): fromDT = fromDT.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(toDT, datetime): toDT = toDT.strftime("%Y-%m-%dT%H:%M:%SZ")
    URL = getURLstring(fromDT, toDT)
    r = requests.get(URL)
    res = r.json()
    return res['bugs']    

#CLEANING

type(res)
len(res)

"""
CONVERT into list of dictionaries

"""
    

Resolution.FIXED == 'FIXED'
datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

temp =\
[
     {
         "assigned_to" : "nobody@mozilla.org",
         "assigned_to_detail" : {
            "email" : "nobody@mozilla.org",
            "id" : 1,
            "name" : "nobody@mozilla.org",
            "real_name" : "Nobody; OK to take it and work on it"
         },
         "component" : "Tabbed Browser",
         "id" : 1461055,
         "last_change_time" : "2018-07-05T12:14:20Z",
         "product" : "Firefox",
         "resolution" : "",
         "status" : "UNCONFIRMED",
         "summary" : "Pinned tab icon fails to load after wake up from sleep"
      },
      {
         "assigned_to" : "nobody@mozilla.org",
         "assigned_to_detail" : {
            "email" : "nobody@mozilla.org",
            "id" : 1,
            "name" : "nobody@mozilla.org",
            "real_name" : "Nobody; OK to take it and work on it"
         },
         "component" : "File Handling",
         "id" : 102380,
         "last_change_time" : "2018-07-12T13:18:03Z",
         "product" : "Firefox",
         "resolution" : "",
         "status" : "NEW",
         "summary" : "\"Open in New Window\" -> download or helper app leaves extra blank window"
      },
      {
         "assigned_to" : "nobody@mozilla.org",
         "assigned_to_detail" : {
            "email" : "nobody@mozilla.org",
            "id" : 1,
            "name" : "nobody@mozilla.org",
            "real_name" : "Nobody; OK to take it and work on it"
         },
         "component" : "Keyboard Navigation",
         "id" : 237027,
         "last_change_time" : "2018-07-11T14:39:25Z",
         "product" : "Firefox",
         "resolution" : "",
         "status" : "REOPENED",
         "summary" : "Inconsistent \"open in new tab\" shortcuts (Ctrl+Enter for links, but Alt+Enter in address bar and search bar)"
      },
      {
         "assigned_to" : "nobody@mozilla.org",
         "assigned_to_detail" : {
            "email" : "nobody@mozilla.org",
            "id" : 1,
            "name" : "nobody@mozilla.org",
            "real_name" : "Nobody; OK to take it and work on it"
         },
         "component" : "Bookmarks & History",
         "id" : 265048,
         "last_change_time" : "2018-07-11T08:11:50Z",
         "product" : "Firefox",
         "resolution" : "",
         "status" : "NEW",
         "summary" : "Search/find won't find text in the Description column"
      }
]

temp = res[res.keys()[0]]


import json
type(temp[0])
json.loads(temp)



"""
###########################
2-1) clean the data
    1) match column names
    2) create datetime obj for sorting
###########################
"""
    
dt = pd.DataFrame([dic.values() for dic in temp], columns=temp[0].keys())
dt1 = dt.drop(['assigned_to_detail'], axis = 1)
dt1
dt1.columns = ['Product', 'Status', 'Assignee', 'Resolution', 'Summary', 'Component', 'Bug ID', 'Changed']
dt1 = dt1[['Bug ID', 'Product', 'Component', 'Assignee', 'Status', 'Resolution', 'Summary', 'Changed']]
dt1.loc[:,'ChangedDT'] = dt1['Changed'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))
dt2 = dt1.sort_values('ChangedDT', inplace=False, ascending=True)
latestDatetime_dt2 = dt2['ChangedDT'].iloc[-1]

type(dt2['ChangedDT'])

#datetime.strptime('2018-07-12T13:18:03Z', "%Y-%m-%dT%H:%M:%SZ")
""" """


"""
###########################
3) update H2 Server
    1) check if there is             
###########################
"""

#getChangedCol(cur, col_name='BugID', tbl_name= 'BUGS')

#checkIfIdExist(cur, Id, pred = '', col_name='BUG ID', tbl_name= 'BUGS')

for i in range(5):
    # REST API to check update
    
    
    # if there is any
    
    
        # if the update is a new
            # insert
        # else (update is not new)
            # delete
            # insert
res = df5.join(dt2, on = pd.Index(['Bug ID']), how='inner')   
res111 = df5.join(dt2, lsuffix='_7_14', rsuffix='_7_7', how='inner')

ck = dt2['Bug ID']
dt2.columns
frames = [dt2, df5]
pd.concat(frames, axis=1, join='inner', join_axes='Bug ID')          
"""
###########################
4) Experiment
    
###########################
"""

"""
###########################
    ) Experiment 
###########################
"""

PATH = 'C:\\Users\\hkim85\\Documents\\GitHub\\QEng-master\\QEng\\Data\\bugs'
os.chdir(PATH)
os.getcwd()
import csv
""" FIRST WRITE """
sql = "SELECT * FROM BUGS;"
cur = runQuery(cur, sql)
rows = cur.fetchall()
df = pd.DataFrame(rows)
df.head()
df.columns = ['BugID', 'Product', 'Component', 'Assignee', 'Status', 'Resolution', 'Summary', 'Changed']
df.shape # (10000, 8)
df.to_csv('bugs_7_7.csv', index=False, encoding="utf-8")

""" update file """
7/7/2018 21:32

fromDT1_str = '2018-07-07T21:32:00Z'
toDT1_str = '2018-07-08T18:01:30Z'
toDT1_str = '2018-07-15T00:00:00Z'#'Now'
res = getListOfDic(fromDT1_str, toDT1_str)
res[0].keys()
df3 = pd.DataFrame([dic.values() for dic in res], columns=res[0].keys())
df4 = df3.drop(['assigned_to_detail'], axis = 1)
df4.head()
df4.columns = ['Status', 'Component', 'Product', 'Summary', 'Assignee', 'BugID', 'Resolution', 'Changed']
df4 = df4[['BugID', 'Product', 'Component', 'Assignee', 'Status', 'Resolution', 'Summary', 'Changed']]
df4.loc[:,'ChangedDT'] = df4['Changed'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))
df5 = df4.sort_values('ChangedDT', inplace=False, ascending=True)
df5.head()
df5.shape
latestDatetime_df5 = df5['ChangedDT'].iloc[-1]
df6 = df5.drop(['ChangedDT'], axis=1)
df6.to_csv('bugs_7_14.csv', index=False, encoding="utf-8")

import time
tic = time.clock()
toc = time.clock()
toc - tic

def execute_many_selects(cursor, queries):
    return [cursor.execute(query).fetchall() for query in queries]

### extract 
bugIDs = df6['BugID']
bugIDs.shape
bugID_ls = [str(bugID) for bugID in bugIDs]
bugID_ls_str = ', '.join(bugID_ls)

# SELECT BugID FROM BUGS WHERE BUGID In (1474164, 1474137)

### checking
sql = """SELECT COUNT(*) FROM BUGS"""
cur = runQuery(cur, sql)
rows = cur.fetchall() # 10000 ==> 9971 => 10138
len(rows)
sql = """SELECT BugID FROM BUGS WHERE BugID in ({})""".format(bugID_ls_str)
cur = runQuery(cur, sql)
rows = cur.fetchall() # 10000 ==> 29 ==>9961 167  10,12
rows_str1 = rows
len(rows_str1)

### delete the rows
sql = """DELETE FROM BUGS WHERE BugID In ({})""".format(bugID_ls_str)
cur = runQuery(cur, sql)

###
df6.loc[:,'Changed'] = df6['Changed'].apply(lambda x: str(x))
df6.loc[:,'BugID'] = df6['BugID'].apply(lambda x: str(x))

col_names = df6.columns.values
df6.head()
df6.shape
stmt = "INSERT INTO BUGS (BugID, Product, Component, Assignee, Status, Resolution, Summary, Changed) values ({}, {}, {}, {}, {}, {}, {}, {})"
stmt = "INSERT INTO BUGS (BugID, Product, Component, Assignee, Status, Resolution, Summary, Changed) values (?, ?, ?, ?, ?, ?, ?, ?)"

parms = 


#values (?, ?, ?, ?, ?, ?, ?, ?)
data = list(map(tuple, df6.itertuples(index=False)))
"""
len(data[0])
stmt.format(data[0][0], data[0][1], data[0][2], data[0][3], data[0][4], data[0][5], data[0][6].replace("'","''"), data[0][7])

for ins_stmt in data: 
    cur.execute(stmt.format(table_name, tuple([str(ele) for ele in ins_stmt])))
"""
table_name = "bugs"
#for ins_stmt in data: cur.execute('insert into {} values {}'.format(table_name, ins_stmt))
sql = 'insert into {} values {}'.format(table_name, tuple([str(ele) for ele in data[0]]))

df6.head()

params = []
for row in df6.iterrows():
    params.append(tuple(row[1]))
params = tuple(params)
cur.executemany(stmt, params)


""" insert rows from csvs """
def insert_csv_to_database(csv_filename, conn, cursor, delimeter = ','):
    # import csv to pandas dataframe
    df = pd.read_csv(csv_filename, delimeter)
    # getting list of column names 
    col_names = df.columns.values
    #str_col_names = ", ".join(col_names)
    # get placeholder string
    place_holder_str = ''
    for i in range(len(col_names)):
        place_holder_str += ':{}, '.format(str(i+1))
    place_holder_str = place_holder_str[:-2]

    table_name = [x for x in map(str.strip, csv_filename.split('.')) if x][0]
    #insert_sql_str = 'insert into {} ({}) values ({})'.format(table_name, str_col_names, place_holder_str)

    data = list(map(tuple, df.itertuples(index=False)))

    for ins_stmt in data: cur.execute('insert into {} values {}'.format(table_name, ins_stmt))
    conn.commit()
    print 'SUCCESS!! {} rows are inserted to TABLE {}'.format(df.shape[0], table_name)


def execute_many_selects(cursor, queries):
    return [cursor.execute(query).fetchall() for query in queries]





len(rows)
type(rows)

cur.execute()

"""
fromDT1_str = '2018-07-05T18:01:30Z'
toDT1_str = '2018-07-08T18:01:30Z'
toDT1_str = '2018-07-15T00:00:00Z'#'Now'
res = getListOfDic(fromDT1_str, toDT1_str)
res[0].keys()
df3 = pd.DataFrame([dic.values() for dic in res], columns=res[0].keys())
df4 = df3.drop(['assigned_to_detail'], axis = 1)
df4.head()
df4.columns = ['Status', 'Component', 'Product', 'Summary', 'Assignee', 'Bug ID', 'Resolution', 'Changed']
df4 = df4[['Bug ID', 'Product', 'Component', 'Assignee', 'Status', 'Resolution', 'Summary', 'Changed']]
df4.loc[:,'ChangedDT'] = df4['Changed'].apply(lambda x: datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ"))
df5 = df4.sort_values('ChangedDT', inplace=False, ascending=True)
"""


    time.sleep(120)


Now = 'Now'

'chfieldfrom=2018-07-05T18:01:30Z&chfieldto=2018-07-08T18:01:30Z'


list_of_attr = ['id', 'product', 'component', 'assigned_to_status', 'resolution', 'summary', 'last_change_time']
str_attr = ','.join(list_of_attr)



r = requests.get(URL, headers=headers)




row = cur.fetchone()

type(row[0])
len(row[0])

temp1 = row[0]
temp1.split()




""" INITIAL LOADING """


""" insert rows from csvs """
def insert_csv_to_database(csv_filename, conn, cursor, delimeter = ','):
    # import csv to pandas dataframe
    df = pd.read_csv(csv_filename, delimeter)
    # getting list of column names 
    col_names = df.columns.values
    #str_col_names = ", ".join(col_names)
    # get placeholder string
    place_holder_str = ''
    for i in range(len(col_names)):
        place_holder_str += ':{}, '.format(str(i+1))
    place_holder_str = place_holder_str[:-2]

    table_name = [x for x in map(str.strip, csv_filename.split('.')) if x][0]
    #insert_sql_str = 'insert into {} ({}) values ({})'.format(table_name, str_col_names, place_holder_str)

    data = list(map(tuple, df.itertuples(index=False)))
    for ins_stmt in data: cur.execute('insert into {} values {}'.format(table_name, ins_stmt))
    conn.commit()
    print 'SUCCESS!! {} rows are inserted to TABLE {}'.format(df.shape[0], table_name)
    
def insert_csv_to_h2(csv_filename, conn, cursor):
    table_name = [x for x in map(str.strip, csv_filename.split('.')) if x][0]
    cursor.execute("CREATE TABLE IF NOT EXISTS {} FROM CSVREAD({})".format(table_name, csv_filename))
    conn.commit()
    print 'SUCCESS!! {} is crated from {}'.format(table_name, csv_filename)
    
def insert_csv_to_database_swap(csv_filename, conn, cursor, delimeter = ','):
    # import csv to pandas dataframe
    df = pd.read_csv(csv_filename, delimeter)
    # getting list of column names 
    col_names = df.columns.values
    #str_col_names = ", ".join(col_names)
    # get placeholder string
    place_holder_str = ''
    for i in range(len(col_names)):
        place_holder_str += ':{}, '.format(str(i+1))
    place_holder_str = place_holder_str[:-2]

    table_name = [x for x in map(str.strip, csv_filename.split('.')) if x][0]
    #insert_sql_str = 'insert into {} ({}) values ({})'.format(table_name, str_col_names, place_holder_str)

    data = list(map(tuple, df.itertuples(index=False)))
    data = [(tup[1], tup[0]) for tup in data]
    for ins_stmt in data: cur.execute('insert into {} values {}'.format(table_name, ins_stmt))
    conn.commit()
    print 'SUCCESS!! {} rows are inserted to TABLE {}'.format(df.shape[0], table_name)






insert_csv_to_database(csv_filename_bugs, conn, cur)

csv_filename_ec = 'EC.csv'
insert_csv_to_database(csv_filename_ec, conn, cur)
csv_filename_fl = 'FL.csv'
insert_csv_to_database(csv_filename_fl, conn, cur)
csv_filename_r = 'R.csv'
insert_csv_to_database(csv_filename_r, conn, cur)
csv_filename_ra = 'RA.csv'
insert_csv_to_database(csv_filename_ra, conn, cur)
csv_filename_tc = 'TC.csv'
insert_csv_to_database(csv_filename_tc, conn, cur)
csv_filename_tl = 'TL.csv'
insert_csv_to_database(csv_filename_tl, conn, cur)
csv_filename_uc = 'UC.csv'
insert_csv_to_database(csv_filename_uc, conn, cur)
csv_filename_cc = 'CC.csv'
insert_csv_to_database(csv_filename_cc, conn, cur)

csv_filename_ccaafl = 'CCaaFL.csv'
insert_csv_to_database(csv_filename_ccaafl, conn, cur)
csv_filename_ccaar = 'CCaaR.csv'
insert_csv_to_database_swap(csv_filename_ccaar, conn, cur)
csv_filename_ccaatc = 'CCaaTC.csv'
insert_csv_to_database(csv_filename_ccaatc, conn, cur)
csv_filename_ecaauc = 'ECaaUC.csv'
insert_csv_to_database_swap(csv_filename_ecaauc, conn, cur)
csv_filename_raara = 'RaaRA.csv'
insert_csv_to_database(csv_filename_raara, conn, cur)
csv_filename_raauc = 'RaaUC.csv'
insert_csv_to_database_swap(csv_filename_raauc, conn, cur)
csv_filename_tcaatl = 'TCaaTL.csv'
insert_csv_to_database(csv_filename_tcaatl, conn, cur)
csv_filename_gaauc = 'GaaUC.csv'
insert_csv_to_database_swap(csv_filename_gaauc, conn, cur)
 
delimeter = ','   
csv_filename = 'GaaUC.csv'
df = pd.read_csv(csv_filename, delimeter)
col_names = df.columns.values
place_holder_str = ''
for i in range(len(col_names)):
        place_holder_str += ':{}, '.format(str(i+1))
place_holder_str = place_holder_str[:-2]
table_name = [x for x in map(str.strip, csv_filename.split('.')) if x][0]
data = list(map(tuple, df.itertuples(index=False)))
data[:5]
data = [(tup[1], tup[0]) for tup in data]
for ins_stmt in data: cur.execute('insert into {} values {}'.format(table_name, ins_stmt))
conn.commit()


cur.execute("SELECT COUNT(*) FROM GaaUC;")
for value in cur.fetchall():
    # the values are returned as wrapped java.lang.Long instances
    # invoke the toString() method to print them
    print(value)
    

""" connection template """    
conn_test1 = jaydebeapi.connect("org.h2.Driver", # driver class
                           "jdbc:h2:tcp://140.192.38.17:9092/~/test", #"jdbc:h2:~/test1",#,#,:8082
                           #"jdbc:h2:tcp://140.192.38.136/~/test",
                           ["", ""], # credentials
                           "./h2-1.4.196.jar") # location of H2 jar


try:
        curs = conn.cursor()
        # Fetch the last 10 timestamps
        curs.execute("SELECT COUNT(*) FROM CC;")
        for value in curs.fetchall():
                # the values are returned as wrapped java.lang.Long instances
                # invoke the toString() method to print them
                print(value)
finally:
        if curs is not None:
                curs.close()
        if conn is not None:
                conn.close()
                
                
create_tables_str = """
CRATE TABLE A
(
     AID        VARCHAR(64)     NOT NULL    PRIMARY KEY,
     AAUTHUR    VARCHAR(128)    NOT NULL    
);

 

"""


"""
CREATE TABLE G 
(
    G_ID    VARCHAR(26)      NOT NULL    PRIMARY KEY,
    G_DESC  VARCHAR(1024)    NOT NULL
);

CREATE TABLE EC
(
    EC_ID       VARCHAR(26)      NOT NULL PRIMARY KEY,
    EC_PRC      VARCHAR(26)      NOT NULL,
    EC_POC      VARCHAR(26)      NOT NULL,
    EC_STEPS    VARCHAR(1024)    NOT NULL
);

CREATE TABLE FL
(
    FL_ID           VARCHAR(26)      NOT NULL    PRIMARY KEY,
    FL_DESC         VARCHAR(1024)    NOT NULL,
    FL_FO           VARCHAR(26)      NOT NULL,
    FL_REPORTEDBY   VARCHAR(26)     NOT NULL,
    FL_ASSIGNEDTO   VARCHAR(26)     NOT NULL,
    FL_SOLUTION     VARCHAR(1025)    NOT NULL,
    FL_STATUS       VARCHAR(26)      NOT NULL
);

CREATE TABLE R
(
    R_ID        VARCHAR(26)     NOT NULL    PRIMARY KEY,
    R_NAME      VARCHAR(26)     NOT NULL,
    R_DESC      VARCHAR(2048)   NOT NULL,
    R_OWNER     VARCHAR(1024)   NOT NULL,
    R_STATUS    VARCHAR(26)     NOT NULL
);

CREATE TABLE RA
(
    RA_ID           VARCHAR(26)      NOT NULL    PRIMARY KEY,
    RA_RISK         VARCHAR(26)      NOT NULL,
    RA_DESC         VARCHAR(1024)    NOT NULL,
    RA_LIKELIHOOD   VARCHAR(26)      NOT NULL,
    RA_IMPACT       VARCHAR(26)     NOT NULL,
    RA_MITIGATED    VARCHAR(26)      NOT NULL
);

CREATE TABLE TC
(
    TC_ID       VARCHAR(26)     NOT NULL    PRIMARY KEY,
    TC_NAME     VARCHAR(26)     NOT NULL,
    TC_LTR      VARCHAR(26)     NOT NULL,
    TC_CS       VARCHAR(26)      NOT NULL
);

CREATE TABLE TL
(
    TL_ID           VARCHAR(26)     NOT NULL    PRIMARY KEY,
    TL_RUNDATE      VARCHAR(26)     NOT NULL,
    TL_CS           VARCHAR(26)     NOT NULL,
    TL_TESTERNAME   VARCHAR(26)     NOT NULL,
    TL_ACTIONTAKEN  VARCHAR(26)      NOT NULL,
    TL_ASSIGNEDTO   VARCHAR(26)     NOT NULL
);

CREATE TABLE UC
(
    UC_ID       VARCHAR(40)  NOT NULL    PRIMARY KEY,
    UC_TITLE    VARCHAR(40) NOT NULL,
    UC_PRC      VARCHAR(40)  NOT NULL,
    UC_POC      VARCHAR(40)  NOT NULL,
    UC_STEPS    VARCHAR(516) NOT NULL    
);

CREATE TABLE CC(
    CC_ID       VARCHAR(26)     NOT NULL    PRIMARY KEY,
    CC_NAME     VARCHAR(26)     NOT NULL,
    CC_DATE     VARCHAR(26)     NOT NULL, -- IS IT OKAY THAT DATE IS REGARDED AS VARCHAR?
    CC_VER      VARCHAR(26)      NOT NULL,
    CC_DESC     VARCHAR(1024)    NOT NULL,
    CC_LOC      VARCHAR(516)    NOT NULL
);



CREATE TABLE CCaaFL
(
    CC_ID           VARCHAR(516)    NOT NULL,
    FL_ID           VARCHAR(516)    NOT NULL,
    PRIMARY KEY (CC_ID, FL_ID),
    FOREIGN KEY (CC_ID) REFERENCES CC(CC_ID),
    FOREIGN KEY (FL_ID) REFERENCES FL(FL_ID)
);

CREATE TABLE CCaaR
(
    CC_ID           VARCHAR(516)    NOT NULL,
    R_ID           VARCHAR(516)    NOT NULL,
    PRIMARY KEY (CC_ID, R_ID),
    FOREIGN KEY (CC_ID) REFERENCES CC(CC_ID),
    FOREIGN KEY (R_ID) REFERENCES R(R_ID)
);

CREATE TABLE CCaaTC
(
    CC_ID           VARCHAR(516)    NOT NULL,
    TC_ID           VARCHAR(516)    NOT NULL,
    PRIMARY KEY (CC_ID, TC_ID),
    FOREIGN KEY (CC_ID) REFERENCES CC(CC_ID),
    FOREIGN KEY (TC_ID) REFERENCES TC(TC_ID)
);

CREATE TABLE ECaaUC
(
    EC_ID           VARCHAR(516)    NOT NULL,
    UC_ID           VARCHAR(516)    NOT NULL,
    PRIMARY KEY (EC_ID, UC_ID),
    FOREIGN KEY (EC_ID) REFERENCES EC(EC_ID),
    FOREIGN KEY (UC_ID) REFERENCES UC(UC_ID)
);

CREATE TABLE GaaUC
(
    G_ID           VARCHAR(516)    NOT NULL,
    UC_ID           VARCHAR(516)    NOT NULL,
    PRIMARY KEY (G_ID, UC_ID),
    FOREIGN KEY (G_ID) REFERENCES G(G_ID),
    FOREIGN KEY (UC_ID) REFERENCES UC(UC_ID)
);

CREATE TABLE RaaRA
(
    R_ID           VARCHAR(516)    NOT NULL,
    RA_ID           VARCHAR(516)    NOT NULL,
    PRIMARY KEY (R_ID, RA_ID),
    FOREIGN KEY (R_ID) REFERENCES R(R_ID),
    FOREIGN KEY (RA_ID) REFERENCES RA(RA_ID)
);

CREATE TABLE RaaUC
(
    R_ID           VARCHAR(516)    NOT NULL,
    UC_ID           VARCHAR(516)    NOT NULL,
    PRIMARY KEY (R_ID, UC_ID),
    FOREIGN KEY (R_ID) REFERENCES R(R_ID),
    FOREIGN KEY (UC_ID) REFERENCES UC(UC_ID)
);

CREATE TABLE TCaaTL
(
    TC_ID           VARCHAR(516)    NOT NULL,
    TL_ID           VARCHAR(516)    NOT NULL,
    PRIMARY KEY (TC_ID, TL_ID),
    FOREIGN KEY (TC_ID) REFERENCES TC(TC_ID),
    FOREIGN KEY (TL_ID) REFERENCES TL(TL_ID)
);
"""


string = "AID1"
string = "1ID1"
[elem.encode("hex") for elem in string]


"""
import javabridge
import jpype
JVM = 'C:\\Program Files\\Java\\jdk-10.0.2\\bin\\client\\jvm.dll'
jpype.startJVM(JVM , '-ea')
"""
"""
url="tcp://140.192.38.17:8094"
dbName="test2"
idd = ""
pw = ""
jdbc_driver_name = "org.h2.Driver"
url = "jdbc:h2:{}/~/{}".format(url, dbName)
jdbc_driver_loc = 'C:\\Users\\hkim85\\Documents\\h2\\bin\\h2-1.4.196.jar'

import jpype
jar = jdbc_driver_loc
args='-Djava.class.path=%s' % jar
jvm_path = jpype.getDefaultJVMPath()
"""
