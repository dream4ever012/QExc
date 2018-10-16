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
###### create statement for normal tables and TMs
### read csv file and dynamically reads column names and
### applies one and half of max length of column values to variable length
### creating TM is for two variables and then adds primary key 
import getCreateTblStmt as gSTS

gsts_ins = gSTS.getCreateTblStmt()
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

def findJoinKeyNs(lol_joinKs, lol_tblNs, joinSeq): 
    if len(joinSeq) > 1:
        a = [any(ele.startswith(joinSeq[0] + ".") for ele in lst) for lst in lol_joinKs]
        b = [any(ele.startswith(joinSeq[1] + ".") for ele in lst) for lst in lol_joinKs]
        t = [True if ((tf1 + tf2 == 2) and (l_joinKs[0] != l_joinKs[1])) else False for tf1, tf2, l_joinKs in zip(a, b, lol_joinKs)]
        if len(list(compress(xrange(len(t)), t))) > 0:
            print lol_joinKs
            print t
            return lol_joinKs[list(compress(xrange(len(t)), t))[0]]
        else: return None
    else: 
        return None
#def findJoinKeyNs(lol_joinKs, lol_tblNs, joinSeq11): return lol_joinKs[[list(compress(xrange(len(t)), t))[0] for t in findElesLol(lol_joinKs, lol_tblNs)][lol_tblNs.index([joinSeq11[0], joinSeq11[1]])]]

#findJoinKeyNs(theRest_splt6, theRest_splt5, joinSeq)   
#lol_joinKs, lol_tblNs, joinSeq11 = theRest_splt6, theRest_splt5, joinSeq
#lol_joinKs[[list(compress(xrange(len(t)), t))[0] for t in findElesLol(lol_joinKs, lol_tblNs)]
"""
lol_joinKs = [['BAACCAAD.BID', 'AAAB.BID'], ['BAACCAAD.CID', 'BAACCAAD.CID']]
t = [False, False]
k = list(compress(xrange(len(t)), t))[0]
"""
### for this version I would just work on INNER JOIN STATEMENT
def initParamGet_(sql, joinSeq):
    sql_lst = sql.upper().split("FROM")
    select = sql_lst[0].upper().replace(",", "").rstrip().split(" ")[1:] # grab select stmt
    select_lst_lst = []
    for ele in select: select_lst_lst.append(ele.split("."))
    #select_lst_lst =[['AAAB', 'AID'], ['AAAB', 'DID'], ['BAAC', 'BID'], ['CAAD', 'CID']]
    select_dict = defaultdict(set) #PPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP
    for list1 in select_lst_lst: select_dict[list1[0]].add(list1[1])
       
    theRest_splt = sql_lst[1].replace(";", "").replace("\n", "").split("INNER JOIN")[1:]
    theRest_splt2 = [ele.split("ON")[1] for ele in theRest_splt] # remove carriage return and 
    theRest_splt3 = [chk.rstrip().lstrip()  for ele in theRest_splt2 for chk in ele.split("=")]
    #theRest_splt35 = [sorted(ele.split("=")) for ele in theRest_splt2]
    theRest_splt6 = [[theRest_splt3[idx], theRest_splt3[idx+1]] for idx in range(0, len(theRest_splt3), 2)] ### select pred cols
    theRest_splt4 = [tblCol.split(".")[0] for list1 in theRest_splt6 for tblCol in list1]
    theRest_splt5 = [[theRest_splt4[idx], theRest_splt4[idx+1]] for idx in range(0, len(theRest_splt4), 2)]
    
    # join lst of lst
    join_lst_lst = []
    for ele in theRest_splt3: join_lst_lst.append(ele.split("."))
    join_dict = defaultdict(set)
    for lst in join_lst_lst: join_dict[lst[0]].add(lst[1])
    # => output join_dict
    js_lst_lst = join_lst_lst + select_lst_lst
    js_dict = defaultdict(set)
    for list1 in js_lst_lst: js_dict[list1[0]].add(list1[1])
    ### find the select preds #####################################
    joinKeyNs = findJoinKeyNs(theRest_splt6, theRest_splt5, joinSeq)
    #theRest_splt6[[list(compress(xrange(len(t)), t))[0] for t in findElesLol(theRest_splt6, theRest_splt5)][theRest_splt5.index([joinSeq[0], joinSeq[1]])]]
        
    return joinKeyNs, js_dict, theRest_splt5, theRest_splt6

sql  
    
def initParamGet_js1(joinSeq):
    """ join table name """
    joinSeq0 = joinSeq[:2] ### this time of join #   
    """ new table name """
    newTblN = joinSeq0[0]+joinSeq0[1] ###########
    return joinSeq0, newTblN

def initParamGet_js2(newTblN, joinSeq0):
    #joinSeq =  [newTblN] + [tbl for tbl in joinSeq if tbl not in joinSeq0]
    #joinSeq0 = joinSeq[:2]
    print 'joinSeq0', joinSeq0
    newTblN = newTblN + joinSeq0[1]
    return newTblN
    

def prj_cols_stmt(js_dict, joinSeq0):
    prj_tbls = js_dict.keys() ### 0) to check if the table appears
    prj_tbls_this = [tbl for tbl in joinSeq0 if tbl in prj_tbls] ### projected tables of this time
    js_dict.items()
    
    js_dict_lst = [(val,key) for key in js_dict for val in js_dict[key]]
    js_dict_swap = defaultdict(set)
    for node1, node2 in [(val,key) for key in js_dict for val in js_dict[key]]: js_dict_swap[node1].add(node2)
    temp = []
    for k, v in js_dict_swap.items(): # col: {tbls}
        if set(prj_tbls).issubset(v): temp.append((k, v))
    #prj_cols_this = [prj_tbl +"."+ col for prj_tbl in prj_tbls_this for col in js_dict[prj_tbl]]
    prj_cols_this = [prj_tbl +"."+ col for prj_tbl in prj_tbls for col in js_dict[prj_tbl]]
    if len(temp) > 0: [prj_cols_this.remove(tbl +"."+ temp[0][0]) for tbl in list(prj_tbls_this)[1:]]
    
    prj_cols_this_stmt = ""
    for prj_col in prj_cols_this: prj_cols_this_stmt = prj_cols_this_stmt + prj_col + ", "
        ###################################
    prj_cols_this_stmt = prj_cols_this_stmt[:-2]
    return prj_cols_this_stmt


def buildNappendSQL(SQL, newTblN, prj_cols_this_stmt, joinSeq0, joinKeyNs):
    SQL = \
    """DROP TABLE {0} IF EXISTS;\
    CREATE TABLE {0} \
        AS \
        SELECT {1} \
        FROM {2} \
        INNER JOIN {3} \
        ON {4} = {5}; """.format(newTblN, prj_cols_this_stmt,joinSeq0[0], joinSeq0[1], joinKeyNs[0], joinKeyNs[1])
    return SQL

    
def updateParam(joinKeyNs, joinSeq0, joinSeq, newTblN, js_dict, theRest_splt5, theRest_splt6):
    theRest_splt5 = [[newTblN if (ele == joinSeq[0] or ele == joinSeq[1]) else ele for ele in lst] for lst in theRest_splt5]
    theRest_splt6 = [[newTblN + ele[ele.find('.'):] if ele[:ele.find('.')] == joinSeq[0] else ele for ele in lst ] for lst in theRest_splt6]
    theRest_splt6 = [[newTblN + ele[ele.find('.'):] if ele[:ele.find('.')] == joinSeq[1] else ele for ele in lst ] for lst in theRest_splt6]
    if joinSeq>2:
        joinKeyNs = findJoinKeyNs(theRest_splt6, theRest_splt5, ([newTblN]+joinSeq[2:]))
    
    for tbl in joinSeq0:
        tbl_vs = js_dict[tbl]
        for v in tbl_vs: js_dict[newTblN].add(v)
        
        
    joinSeq  = [newTblN] + joinSeq[2:]
    if len(joinSeq) > 2:
        joinSeq0 = joinSeq[2:] ############
        joinSeq0.insert(0,joinSeq[0]+joinSeq[1])
    else: joinSeq0 = joinSeq

    return joinKeyNs, js_dict, joinSeq0, joinSeq, theRest_splt5, theRest_splt6


sql = """\
SELECT AaaB.AID, BaaC.BID, CaaD.CID \
FROM AaaB \
INNER JOIN BaaC \
ON BaaC.BID = AaaB.BID \
INNER JOIN CaaD \
ON CaaD.CID = BaaC.CID;
"""
joinSeq= ['BAAC', 'CAAD', 'AAAB']

sql = """\
SELECT A.AID, B.BID, C.CID \
FROM A \
INNER JOIN AaaB \
ON AaaB.AID =  A.AID \
INNER JOIN B \
ON B.BID = AaaB.BID;
"""
joinSeq= ['A', 'AAAB', 'B']

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
joinSeq= ['A', 'AAAB', 'B', 'BaaC', 'C']

def run(joinSeq= ['BAAC', 'CAAD', 'AAAB']):
    SQL = []
    #divide(test, joinSeq) # joinSeq
    joinKeyNs, js_dict, theRest_splt5, theRest_splt6 = initParamGet_(sql, joinSeq)
    joinSeq0, newTblN = initParamGet_js1(joinSeq)  
    prj_cols_this_stmt = prj_cols_stmt(js_dict, joinSeq0)
    print  prj_cols_this_stmt
    SQL.append(buildNappendSQL(SQL, newTblN, prj_cols_this_stmt, joinSeq0, joinKeyNs))
    
    print 'len', len(joinSeq)
    once = True
    while (len(joinSeq) > 2 or once == True):
        print 'len(joinSeq0)', len(joinSeq0)
        joinKeyNs, js_dict, joinSeq0, joinSeq, theRest_splt5, theRest_splt6 = updateParam(joinKeyNs, joinSeq0, joinSeq, newTblN, js_dict, theRest_splt5, theRest_splt6)
        if (len(joinSeq) > 2 or once == True):
            newTblN = initParamGet_js2(newTblN, joinSeq0)
            once = False        
        prj_cols_this_stmt = prj_cols_stmt(js_dict, joinSeq0)
        SQL.append(buildNappendSQL(SQL, newTblN, prj_cols_this_stmt, joinSeq0, joinKeyNs))
        ###################################
    return SQL

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
SQL = run(joinSeq= ['AAAB', 'BaaC', 'CaaD'])

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



joinSeq0, newTblN = initParamGet_js(joinSeq)    
prj_cols_this_stmt = prj_cols_stmt(select_dict, joinSeq0)
SQL = buildNappendSQL(SQL, newTblN, prj_cols_this_stmt, joinSeq0, joinKeyNs)

joinKeyNs, select_dict, joinSeq, theRest_splt5, theRest_splt6 = updateParam(joinKeyNs, joinSeq0, newTblN, select_dict, theRest_splt5, theRest_splt6, joinSeq)



joinKeyNs, select_dict, theRest_splt5, theRest_splt6 = initParamGet_(sql)


joinSeq0, newTblN = initParamGet_js(joinSeq)
prj_cols_this_stmt = prj_cols_stmt(select_dict, joinSeq0)
SQL = buildNappendSQL(SQL, newTblN, prj_cols_this_stmt, joinSeq0, joinKeyNs)
select_dict, joinSeq, theRest_splt5, theRest_splt6 = updateParam(joinKeyNs, joinSeq0, select_dict, theRest_splt5, theRest_splt6, joinSeq)



### updates
# select_dict = old name to new name 
# now just adds
for tbl in joinSeq0:
    tbl_vs = select_dict[tbl]
    for v in tbl_vs: select_dict[newTblN].add(v)

# joinSeq
joinSeq_nxt = joinSeq[2:]
joinSeq_nxt.insert(0,joinSeq[0]+joinSeq[1])





############################## PARAMETERS INHERIT
select_dict['AAAB']
select_dict[newTblN].add(select_dict[])





    


    
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





[w in str1 for str1 in theRest_splt35[0] for w in (joinSeq[0],joinSeq[1])]

[str1 for str1 in theRest_splt35[0] for w in (joinSeq[0],joinSeq[1])]

findEles(theRest_splt35[0], joinSeq[0],joinSeq[1])

any(joinSeq[0] in str1 in theRest_splt35[0])

findEle(joinSeq[0], theRest_splt35[0])
findEle(joinSeq[1], theRest_splt35[0])


findEles(theRest_splt35, joinSeq[0], joinSeq[1])

first = True

theRest_splt5   ### join pair table names
theRest_splt35  ### join key pair
joinSeq  ### talbe names in join sequence
joinSeq1 = joinSeq[2:]
joinSeq1.insert(0,joinSeq[0]+joinSeq[1])
joinSeq1 ### new table names in join sequence ### TO-DOs: update join pair table names and join key pair


theRest_splt35[0]

findEles(theRest_splt35, joinSeq[0], joinSeq[1])
[ for list1 in theRest_splt35]

print joinSeq[0], joinSeq[1]

for k in theRest_splt5: #zip(theRest_splt5, theRest_splt35):
    print k
    
('AAAB', 'BAAC')





str1 = 'AAAB'
list1 = theRest_splt35[0]
any(True if str1 in ele else False for ele in list1)
findEle('AAAB', theRest_splt35[0]) == True



# 
joinSeq[0], joinSeq[1]




TABLE = joinSeq[0]+joinSeq[1]
SELECT = 


"""
{0}:
{1}:
"""
temp 
join_seq = temp
a = ["B", "A"]
a.sort()
tuple(a)
a.sort()
a
k, v =zip(theRest_splt5, theRest_splt6)[0]
k


join_dict = collections.defaultdict(list)
for k, v in zip(theRest_splt5, theRest_splt6):
    print k, v
    #join_dict[k] = v

join_dict.keys()

temp 
for idx in range(0, len(temp), 2):


for idx in range(0, len(theRest_splt4), 2):
    print set([theRest_splt4[idx], theRest_splt4[idx+1]]), idx

theRest_splt4[0]

for idx in range(0, len(theRest_splt4), 2):
    set([theRest_splt4[idx][0], theRest_splt4[idx+1][0]])


[ele.split(".") for lst in [ele.split("=") for ele in theRest_splt2] for ele in lst]

.rstrip().lstrip()

### extract table name and col_name



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
    