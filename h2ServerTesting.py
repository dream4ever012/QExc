# -*- coding: utf-8 -*-
"""
Created on Mon Jul 02 20:15:00 2018

@author: hkim85
"""
import os
import socket
PATH = 'C:\\Users\\hkim85\\Desktop\\2016_PhDinCS\\201603_IndStudy\\201709IndStu\\InfusionPump\\bugs'
os.chdir(PATH)
os.getcwd()
#mport H2SerConn as h2SerConn
from collections import defaultdict as defaultdict
from itertools import compress
import H2SerConn as h2SerConn

####
####
###### create statement for normal tables and TMs
### read csv file and dynamically reads column names and
### applies one and half of max length of column values to variable length
### creating TM is for two variables and then adds primary key 
import getCreateTblStmt as gSTS
gsts_ins = gSTS.getCreateTblStmt()

#def prepCreateStmt():
createStmtA = gsts_ins.createTableStmt("A.csv")
createStmtB = gsts_ins.createTableStmt("B.csv")
createStmtC = gsts_ins.createTableStmt("C.csv")
createStmtD = gsts_ins.createTableStmt("D.csv")
createStmtAB = gsts_ins.createTableStmt("AaaB.csv", isTM=True)
createStmtBC = gsts_ins.createTableStmt("BaaC.csv", isTM=True)
createStmtCD = gsts_ins.createTableStmt("CaaD.csv", isTM=True)

#### Table name hardcoded for testing set up t
### TO-DO: have to have appropriate 
A = "A"
B = "B"
C = "C"
D = "D"
AB = "AaaB"
BC = "BaaC"
CD = "CaaD"

#java -cp h2-1.4.197.jar org.h2.tools.Server -webAllowOthers -webPort 8092 -tcpAllowOthers -tcpPort 8094 -baseDir C:\Users\hkim85\Downloads -pgAllowOthers -pgPort 8096


"""
####################################
TO-DOs) not by priority
1) READCSV: almost done need to compile functions
   - work dynamically for varchar variables// TMs(primary keys are set up accordingly)
   -
2) WRITECSV: 
3) prepresentation
####################################
"""


h2Server8094 = h2SerConn.H2SerConn("8094", "test", "", "", 
                 ip = socket.gethostbyname(socket.gethostname()))
h2Server8094.OpenConnCur()
print "sucess! Conn/cur established@{}".format(h2Server8094)
import time
### create table from csv
h2Server8094.runQuery(createStmtA)
h2Server8094.runQuery(createStmtB)
h2Server8094.runQuery(createStmtC)
h2Server8094.runQuery(createStmtD)
h2Server8094.runQuery(createStmtAB)
tic = time.time()
h2Server8094.runQuery(createStmtBC)
toc = time.time()
h2Server8094.runQuery(createStmtCD)
print toc - tic


h2Server8094.insert_csv_to_database("A.csv")
h2Server8094.insert_csv_to_database("B.csv")
h2Server8094.insert_csv_to_database("C.csv")
h2Server8094.insert_csv_to_database("D.csv") 
h2Server8094.insert_csv_to_database("AaaB.csv")
h2Server8094.insert_csv_to_database("BaaC.csv")
h2Server8094.insert_csv_to_database("CaaD.csv")

#SUCCESS!! 3000 rows are inserted to TABLE A
#SUCCESS!! 3000 rows are inserted to TABLE B
#SUCCESS!! 1500 rows are inserted to TABLE C
#SUCCESS!! 1500 rows are inserted to TABLE D
#SUCCESS!! 6070 rows are inserted to TABLE AaaB
#SUCCESS!! 6120 rows are inserted to TABLE BaaC
#SUCCESS!! 6105 rows are inserted to TABLE CaaD

h2Server8094.runQuery("SELECT COUNT(*) FROM A")
res = h2Server8094.fetchone()
print res
h2Server8094.runQuery("SELECT COUNT(*) FROM B")
res = h2Server8094.fetchone()
print res
h2Server8094.runQuery("SELECT COUNT(*) FROM C")
res = h2Server8094.fetchone()
print res
h2Server8094.runQuery("SELECT COUNT(*) FROM AaaB")
res = h2Server8094.fetchone()
print res
h2Server8094.runQuery("SELECT COUNT(*) FROM BaaC")
res = h2Server8094.fetchone()
print res
h2Server8094.runQuery("SELECT COUNT(*) FROM CaaD")
res = h2Server8094.fetchone()
print res

res = h2Server8094.fetchone()




### representing tables: execution plan crude version
# need recursive 
# for this prototype, I am going to just print...
# rule always two sets...
# how to put levels
# the same level can be done in any ways
test = (((A,AB), B),BC)

# just want to see how this behaves
test = (((A,AB), B),(BC,C))
### findings: divide does not support bushy join
# remedy: has to make it a list 
# have a plan object for a unit of

import query1_1 as qry

conn = [(A, AB), (B, AB), (B, BC), (C, BC), (C, CD), (D, CD)]
query = qry.Query(conn, directed=True)
query.getQueryGraph()[A]

### problem: query graph) 
# 

# how can we express bushy join??

def divide(thing, temp):
    if type(thing) is tuple:
        divide(thing[0], temp)
        divide(thing[1], temp)
    else:
        #print thing
        temp.append(thing.upper())
        

test = ((AB,BC), CD)
test = (AB, (BC, CD))
joinSeq = []
divide(test, joinSeq)
joinSeq = ['BAAC', 'CAAD', 'AAAB'] #PPP
joinSeq = [tbl.upper() for tbl in [BC, CD, AB]]
#==> if not bushy join
# have to check inside the tuple



# how to manage join key



# - new table name
# - select
# - inner
# - join key

#### INPUT



"""\
INNER JOIN BaaC \
ON BaaC.BID = B.BID \
INNER JOIN C \
"""



### input1 =  SELECT cols, tblNames, joinPs // joinSeq

"""
0) if len(joinSeq) > 1
1) get select cols
2) 
"""

"""
UTILITIES
"""
### find if a str sequence is found from a list
def findEle(str1, list1): return any(True if str1 in ele else False for ele in list1)
### find if dynamic number of all strs found from list
def findEles1(list1, *argv): return sum([findEle(w,list1) for w in argv]) == len(argv)
def findEles(list1, argv): return sum([findEle(w,list1) for w in argv]) == len(argv) # True: joins keys are found
### matching table names with the join keys
def findElesLol(lol_joinKs, lol_tblNs): return [[findEles(list1, listTblNfor) for listTblNfor in lol_tblNs] for list1 in lol_joinKs]

def findJoinKey(join_dict, joinSeq0):
    """ assumption: there is only one join key between any two table(or TM)"""
    return join_dict[joinSeq0[0]].intersection(join_dict[joinSeq0[1]]).pop()

def findJoinKeyNs(join_dict, joinSeq0):  
    joinKey = findJoinKey(join_dict, joinSeq0)
    return [joinSeq0[0]+"."+joinKey, joinSeq0[1]+"."+joinKey]
    

#def findJoinKeyNs(lol_joinKs, lol_tblNs, joinSeq11): return lol_joinKs[[list(compress(xrange(len(t)), t))[0] for t in findElesLol(lol_joinKs, lol_tblNs)][lol_tblNs.index([joinSeq11[0], joinSeq11[1]])]]

#findJoinKeyNs(theRest_splt6, theRest_splt5, joinSeq)   
#lol_joinKs, lol_tblNs, joinSeq11 = theRest_splt6, theRest_splt5, joinSeq
#lol_joinKs[[list(compress(xrange(len(t)), t))[0] for t in findElesLol(lol_joinKs, lol_tblNs)]
"""
lol_joinKs = [['BAACCAAD.BID', 'AAAB.BID'], ['BAACCAAD.CID', 'BAACCAAD.CID']]
t = [False, False]
k = list(compress(xrange(len(t)), t))[0]
"""

def initParamGet_js1(joinSeq):
    if len(joinSeq) > 1:
        """ join table name """
        joinSeq0 = joinSeq[:2] ### this time of join #   
        """ new table name """
        newTblN = joinSeq0[0]+joinSeq0[1] ###########
        return joinSeq0, newTblN
    else:
        print 'ERROR CODE: len(joinSeq) <= 1'
        return joinSeq, 'XXXXX'

### for this version I would just work on INNER JOIN STATEMENT
#### right way: need system catalog beforehand
def initParamGet_(sql, joinSeq):
    sql_lst = sql.upper().split("FROM")
    select = sql_lst[0].upper().replace(",", "").rstrip().split(" ")[1:] # grab select stmt
    sel_cols_list = ([tbl_col.split('.') for tbl_col in select])
    
    # next join seq and newtbl name
    joinSeq0, newTblN = initParamGet_js1(joinSeq) 
    
    # tblN, colN in selection
    select_dict = defaultdict(set) #PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP
    for list1 in sel_cols_list: select_dict[list1[0]].add(list1[1])
       
    theRest_splt = sql_lst[1].replace(";", "").replace("\n", "").split("INNER JOIN")[1:]
    theRest_splt2 = [ele.split("ON")[1] for ele in theRest_splt] # remove carriage return and 
    theRest_splt3 = [chk.rstrip().lstrip()  for ele in theRest_splt2 for chk in ele.split("=")]

    join_lst_lst = []
    for ele in theRest_splt3: join_lst_lst.append(ele.split("."))
    join_dict = defaultdict(set)
    for lst in join_lst_lst: join_dict[lst[0]].add(lst[1])
    ### find the select preds #####################################
    
    joinKeyNs = findJoinKeyNs(join_dict, joinSeq0)
        
    return joinKeyNs, joinSeq0, joinSeq, join_dict, select_dict, newTblN



    
def swapdict(dict1):
    dict_swap = defaultdict(set)
    for node1, node2 in [(val,key) for key in dict1 for val in dict1[key]]: dict_swap[node1].add(node2)
    return dict_swap

def prj_cols_stmt(join_dict, select_dict, joinSeq0):
    # projected cols + join cols
    prj_tbls = join_dict.keys() ### 0) to check if the table appears
    prj_tbls_this = joinSeq0 #[tbl for tbl in joinSeq0 if tbl in prj_tbls] ### projected tables of this time
    
    from operator import or_
    join_cols = reduce(or_, [join_dict[tm] for tm in joinSeq0]) # cols necessary in this join
    prj_cols = reduce(or_, [select_dict[tm] for tm in joinSeq0]) # cols projected 
    #sel_cols = join_cols.union(prj_cols) # column names that should be in selection stmt in this phase of query
    join_cols_prj = join_cols.difference(prj_cols)
    
    join_dict_swap = swapdict(join_dict)
    sel_dict_swap = swapdict(select_dict)
    
    # projected tbl_name, col_name
    prj_tbl_col_names = [(list(sel_dict_swap[prj_col].intersection(joinSeq0))[0], prj_col) for prj_col in prj_cols] + \
    [(list(join_dict_swap[join_col].intersection(joinSeq0))[0], join_col) for join_col in join_cols_prj]
    # + necessary tbl_name, col_name for join 
    
    prj_tbl_col_names =  [tbl + "." + col for tbl, col in prj_tbl_col_names]
    
    prj_tbl_col_stmt = ""
    for prj_tbl_col in prj_tbl_col_names: prj_tbl_col_stmt = prj_tbl_col_stmt + prj_tbl_col + ", "
        ###################################
    prj_cols_this_stmt = prj_tbl_col_stmt[:-2]
    return prj_cols_this_stmt


def buildNappendSQL(SQL, newTblN, prj_cols_this_stmt, joinSeq0, joinKeyNs):
    """ build SQL query of creating table of the intermediate join results """
    SQL = \
    """DROP TABLE {0} IF EXISTS;\
    CREATE TABLE {0} \
        AS \
        SELECT {1} \
        FROM {2} \
        INNER JOIN {3} \
        ON {4} = {5}; """.format(newTblN, prj_cols_this_stmt,joinSeq0[0], joinSeq0[1], joinKeyNs[0], joinKeyNs[1])
    return SQL

def updateDict(dict11, tbl_names_this_join, newTblN):
    tbl_names_this_join
    dict1 = dict11
    # insert new table
    for tbl in tbl_names_this_join:
        tbl_vs = dict1[tbl]
        for v in tbl_vs: dict1[newTblN].add(v)
    return dict1        


# this has to be with Query object to find the join key with the cheapest join key



type(js_dict11)
js_dict11 = js_dict

def updateParam(joinSeq, join_dict, select_dict, newTblN):
    # 1) nextjoin key  # 2) next join joinSeq # 3) update js_dict
    # 4) update select_dict #### consider if len(joinSeq) ==2 == break
    joinSeq0 = joinSeq[:2]    
    join_dict = updateDict(join_dict, joinSeq0, newTblN)
    select_dict = updateDict(select_dict, joinSeq0, newTblN)
    # update joinSeq, this join (joinSeq0, newTblN)
    joinSeq = [newTblN] + joinSeq[2:]
    joinSeq0 = joinSeq[:2]
    joinKeyNs = ''
    newTblN = joinSeq[0] + joinSeq[1]
    joinKeyNs = findJoinKeyNs(join_dict, joinSeq0)
    return joinKeyNs, joinSeq0, joinSeq, join_dict, select_dict, newTblN

def buildSQL(joinSeq= ['BAAC', 'CAAD', 'AAAB']):
    joinSeq = [tblN.upper() for tblN in joinSeq] # uppercase all letters
    SQL = []
    #divide(test, joinSeq) # joinSeq
    joinKeyNs, joinSeq0, joinSeq, join_dict, select_dict, newTblN = initParamGet_(sql, joinSeq)
    select_dict_org = select_dict # keep copy just in case
    join_dict_org = join_dict
    
    prj_cols_this_stmt = prj_cols_stmt(join_dict, select_dict, joinSeq0)
    SQL.append(buildNappendSQL(SQL, newTblN, prj_cols_this_stmt, joinSeq0, joinKeyNs))

    while (len(joinSeq) > 2):
        joinKeyNs, joinSeq0, joinSeq, join_dict, select_dict, newTblN = \
        updateParam(joinSeq, join_dict, select_dict, newTblN)
        prj_cols_this_stmt = prj_cols_stmt(join_dict, join_dict, joinSeq0)
        SQL.append(buildNappendSQL(SQL, newTblN, prj_cols_this_stmt, joinSeq0, joinKeyNs))
    ###################################
    return SQL

def buildIdx_(indexName, tableName, colName):
    """ index with indexName """
    sql = "CREATE HASH IF NOT EXISTS {} INDEX ON {}({});".format(indexName, tableName, colName)
    h2Server8094.runQuery(sql)
    print 'Index created, {}.{}'.format(tableName, colName)

def buildIdx(tableName, colName):
    sql = "CREATE HASH INDEX ON {}({});".format(tableName, colName)
    h2Server8094.runQuery(sql)
    print 'Index created, {}.{}'.format(tableName, colName)

""" build and index: system catalog """
# table name
#   col name
# index catalog
#   index reference

""" OO of table import + """

tic = time.time()
h2Server8094.runQuery(SQL[0])
toc = time.time()
print toc - tic

def timeSQLs(SQL):
    timeLs = []
    for sql in SQL:
        tic = time.time()
        h2Server8094.runQuery(sql)
        toc = time.time()
        timeLs.append(toc-tic)
    return timeLs

sql = """\
SELECT AaaB.AID, BaaC.BID, CaaD.CID \
FROM AaaB \
INNER JOIN BaaC \
ON BaaC.BID = AaaB.BID \
INNER JOIN CaaD \
ON CaaD.CID = BaaC.CID;
"""
joinSeq= ['BAAC', 'CAAD', 'AAAB']
SQL1 = []
SQL1 = buildSQL(joinSeq)
timeLs1 = timeSQLs(SQL)
timeLs1

joinSeq= ['CAAD', 'BAAC', 'AAAB']
SQL0 = []
SQL0 = buildSQL(joinSeq)
timeLs0 = timeSQLs(SQL)
timeLs0


joinSeq= ['BAAC', 'AAAB', 'CAAD']
SQL2 = []
SQL2 = buildSQL(joinSeq)
timeLs2 = timeSQLs(SQL)
timeLs2

joinSeq= ['AAAB', 'BAAC', 'CAAD']
SQL3 = []
SQL3 = buildSQL(joinSeq)
timeLs3 = timeSQLs(SQL)
timeLs3



sql = """\
SELECT A.AID, B.BID \
FROM A \
INNER JOIN AaaB \
ON AaaB.AID =  A.AID \
INNER JOIN B \
ON B.BID = AaaB.BID;
"""
joinSeq= ['A', 'AAAB', 'B']
SQL4 = []
SQL4 = buildSQL(joinSeq)
timeLs4 = timeSQLs(SQL)
timeLs4

joinSeq= ['AAAB', 'B', 'A']
SQL5 = []
SQL5 = buildSQL(joinSeq)
timeLs5 = timeSQLs(SQL)
timeLs5



sql = """\
SELECT A.AID, B.BID, C.CID \
FROM A \
INNER JOIN AaaB \
ON AaaB.AID =  A.AID \
INNER JOIN B \
ON B.BID = AaaB.BID \
INNER JOIN BaaC \
ON BaaC.BID = B.BID \
INNER JOIN C \
ON C.CID = BaaC.CID;
"""
joinSeq= ['A', 'AAAB', 'B', 'BAAC', 'C']
SQL = []
SQL = buildSQL(joinSeq)

#idxDict = 


""" predicates """
sql = """\
SELECT A.AID, B.BID, C.CID \
FROM A \
INNER JOIN AaaB \
ON AaaB.AID =  A.AID \
INNER JOIN B \
ON B.BID = AaaB.BID \
INNER JOIN BaaC \
ON BaaC.BID = B.BID \
INNER JOIN C \
ON C.CID = BaaC.CID \
WHERE A.AUTHOR = ;
"""


def buildTblColDict():
    
    




"""SELECT 
         AaaB.AID, BaaC.BID, CaaD.CID 
   FROM AaaB 
   INNER JOIN BaaC 
        ON BaaC.BID = AaaB.BID 
   INNER JOIN CaaD 
        ON CaaD.CID = BaaC.CID;\n"""

#1) join seq 1) BAAC CAAD == prj_cpls
        


for sql in SQL:
    run


##### TO-DOs))
"""
1) UDF
    - set up at Java
    - call from python
    - register the alias from the optimizer as well
    - 
2) system catalog
    - structures: tables, TM, predicate, index
    - connection: query

3) TO-DOs
    - building indexes
    - non UDF predicates
    - UDF predicates
    
4) skip join for the sake of 
"""



SQL = []
SQL = run(joinSeq= ['A', 'AAAB', 'B', 'BaaC', 'C'])


SQL[1]

tic = time.time()
h2Server8094.runQuery(SQL[0])
toc = time.time()
print toc - tic

tic = time.time()
h2Server8094.runQuery(SQL[1])
toc = time.time()
print toc - tic


h2Server8094.runQuery("SELECT COUNT(*) FROM AAABBAAC")
res = h2Server8094.fetchone()
print res

h2Server8094.runQuery("SELECT COUNT(*) FROM AAABBAACCAAD")
res = h2Server8094.fetchone()
print res



    


    
""" first
0) new table names
1) projection predicates
2) inner join 
3) outer join
4) join key columns 3) and 4) 
"""

""" update
removed 

"""

t = findElesLol(theRest_splt35, theRest_splt5)[1]

from itertools import compress
#t = findElesLol(theRest_splt35, theRest_splt5)
list(compress(xrange(len(t)), t))[0]


for listTblNfor in theRest_splt5:
    for list1 in theRest_splt35:
        print list1, listTblNfor

for list1 in theRest_splt35:
    print findEles(list1, joinSeq[0], joinSeq[1])






"DROP TABLE {} IF EXISTS; ".format()


# have to 

""" we need a structure that can check join keys """




.pop(0)



"""
SELECT (table, col_name) ==> list of tuples
ON tblA, tblB, joinK
ON tblOne, tblTwo, joinK
"""

### TO-DOs 
Query)) 
single SELECT

SELECT 
SELECT table and column name
FROM INNER INNER)) tables
ON join key




#how to recursively print
type(test) is tuple
type((1,2)) == tuple

type((1,2))

res = [just for just in test]



"""
DOESNT NEED MUCH SYSTEM CATALOG IF WE REWRITE QUERY TO HAVE TABLENAME.COLNAME even if only having COLNAME
"""
#### OUTPUT
"DROP TABLE AaaBaaC IF EXISTS;\
CREATE TABLE AaaBaaC \
    AS \
    SELECT AaaB.AID, BaaC.BID, BaaC.CID \
    FROM AaaB \
    INNER JOIN BaaC \
    ON BaaC.BID = AaaB.BID; \
DROP TABLE AaaBaaCaaD IF EXISTS; \
CREATE TABLE AaaBaaCaaD \
    AS \
    SELECT AaaBaaC.AID, AaaBaaC.BID, AaaBaaC.CID, CaaD.DID \
    FROM AaaBaaC \
    INNER JOIN CaaD \
    ON CaaD.CID = AaaBaaC.CID"




h2Server8094.runQuery("DROP TABLE AaaB;")
h2Server8094.runQuery("DROP TABLE BaaC;")
h2Server8094.runQuery("DROP TABLE CaaD;")


### drop table by table name

#c:\Users\hkim85\Desktop\2016_PhDinCS\201603_IndStudy\201709IndStu\InfusionPump\bugs\

def rreplace(s, old, new, occurrence):
    li = s.rsplit(old, occurrence)
    return new.join(li)

csv_filename = 'A.csv'

df.head()

len(col_names)

#for col_name in col_names:
    
    
    


for col in df:
    print col

col_var_len = [len(elem)*3 for elem in df.iloc[1,:]]

    
place_holder_str = ''
for i in range(len(col_names)):
    place_holder_str += ':{}, '.format(str(i+1))
    place_holder_str = place_holder_str[:-2]
                
table_name = [x for x in map(str.strip, csv_filename.split('.')) if x][0]
#insert_sql_str = 'insert into {} ({}) values ({})'.format(table_name, str_col_names, place_holder_str)


""" to get create sql """
def getIDVarStr(table_name, col_name, col_val_len):
    return "CRATE TABLE {} (\
    {}      VARCHAR({})       NOT NULL    PRIMARY KEY, \
    {});".format(table_name, col_name, col_val_len, "{}")        

def getNonIDVarStr(col_name, col_val_len):
    ### create portion of insert str for non_ID varible
    ### all variable is NOT NULL
    temp = "{}  VARCHAR({})  NOT NULL, {}"
    return temp.format(col_name, col_val_len, "{}")

def createTableSql(csv_file_name):
    import pandas as pd
    df = pd.read_csv(csv_filename)
    col_names = df.columns.values
    col_val_lens = [int(len(max(df[col_name], key=len))*1.5) for col_name in df]
    create_tables_str = getIDVarStr(table_name, col_names[0], col_val_lens[0])
    for i in range(1, len(col_val_lens)): 
        create_tables_str = create_tables_str.format(getNonIDVarStr(col_names[i], col_val_lens[i]))
    create_tables_str = create_tables_str.replace(", {}", "")
    return create_tables_str

csv_file_name = "A.csv"
ans = createTableSql(csv_file_name)       



str1 = "19,flv,m12-vn,"
str1=str1[::-1].replace(",","",1)[::-1]

    
li = create_tables_str.rsplit(',', 1)
       
        data = list(map(tuple, df.itertuples(index=False)))
        for ins_stmt in data: self.cursor.execute('insert into {} values {}'.format(table_name, ins_stmt))
        self.conn.commit()
        print 'SUCCESS!! {} rows are inserted to TABLE {}'.format(df.shape[0], table_name)



import functools
import time
def timeit(func):
    # https://stackoverflow.com/questions/5478351/python-time-measure-function
    #@functools.wraps(func)
    def func(*args, **kwargs):
        startTime = time.time()
        func(*args, **kwargs)
        elapsedTime = time.time() - startTime
        print('function [{}] finished in {} ms'.format(func.__name__, int(elapsedTime * 1000)))
            
def timeit(f):
    startTime = time.time()
    f()
    elapsedTime = time.time() - startTime
    print('function [{}] finished in {} ms'.format(func.__name__, int(elapsedTime * 1000)))
    