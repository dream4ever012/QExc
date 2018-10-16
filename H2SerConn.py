# -*- coding: utf-8 -*-
"""
Created on Mon Sep 10 08:21:05 2018

@author: hkim85
"""

# grep connenction

import getCreateTblStmt as gsts


class H2SerConn(object):       
    import socket
    # tcp connections is implied
    import jaydebeapi
    import time
    import getCreateTblStmt as gsts
    
    def __init__(self, portNum, dbName, idd, pw, 
                 ip = socket.gethostbyname(socket.gethostname()), 
                 jarPath  = "C://Program Files (x86)//H2//bin//h2-1.4.197.jar"):
        
        ### will dynamically get ip address
        self.url = "tcp://{}:{}".format(ip, portNum)
        self.dbName = dbName
        self.idpw = [idd, pw]
        self.jarPath = jarPath
        self.conn = None
        self.cursor = None
        self.gsts = gsts.getCreateTblStmt()
        
    def get_connection(self): #, jarPath, id_ = "",pw = ""):
        import jaydebeapi
        try:
            self.conn = jaydebeapi.connect("org.h2.Driver", 
                                  "jdbc:h2:{}/~/Downloads/{}".format(self.url, self.dbName), 
                                  self.idpw, self.jarPath)
        except Exception as e:
            print e
            print "WARNING! Connection to {} failed.".format(self.url)
    
    def get_cursor(self):
        try:
            self.cursor = self.conn.cursor()
            #return cur
        except:
            print "WARNING! Cursor Failed @{}.".format(self.url)
            return None
    
    def OpenConnCur(self):
        self.get_connection()
        self.get_cursor()
    
    def runQuery(self, sql):
        # TO-DO: how to deal with errors
        self.cursor.execute(sql)
        
    def commit(self):
        self.conn.commit()
        
    def timeit(self, func):
        # https://stackoverflow.com/questions/5478351/python-time-measure-function
        @functools.wraps(func)
        def newfunc(*args, **kwargs):
            startTime = time.time()
            func(*args, **kwargs)
            elapsedTime = time.time() - startTime
            print('function [{}] finished in {} ms'.format(func.__name__, int(elapsedTime * 1000)))
            
        
    def fetchmany(self, size):
        return self.cursor.fetchmany(size)
        """
        try:
            return self.cursor.fetchmany(size)
        except Exception as e:
            print e
            print "WARNING! cursor doesn't have any result!"
        return "WARNING! cursor doesn't have any result!"
        """
    
    def fetchall(self):
        return self.cursor.fetchall()
    
    def fetchone(self):
        return self.cursor.fetchone()
    
    def closeCur(self):
        self.cursor.close()
        
    def closeConn(self):
        self.conn.close()
        
    def insert_csv_to_h2(self, csv_filename):
        import sys  
        reload(sys)  
        sys.setdefaultencoding('utf8')
        # TO-DO: make it more robust for various delimeters
        # may have to seperate into Utility class
        table_name = [x for x in map(str.strip, csv_filename.split('.')) if x][0]
        self.cursor.execute("DROP TABLE {} IF EXISTS; CREATE TABLE {} AS SELECT * FROM CSVREAD(C:\Users\hkim85\Desktop\2016_PhDinCS\201603_IndStudy\201709IndStu\InfusionPump\bugs\{});".format(table_name, table_name, csv_filename))
        self.conn.commit()
        print 'SUCCESS!! {} is crated from {}'.format(table_name, csv_filename)

    def getIDVarStr(self, table_name, col_name, col_val_len):
        return "CRATE TABLE {} (\
        {}      VARCHAR({})       NOT NULL    PRIMARY KEY, \
        {});".format(table_name, col_name, col_val_len, "{}")        
    
    def getNonIDVarStr(self, col_name, col_val_len):
        ### create portion of insert str for non_ID varible
        ### all variable is NOT NULL
        temp = "{}  VARCHAR({})  NOT NULL, {}"
        return temp.format(col_name, col_val_len, "{}")
    
    
    """
    def createTblStmt(self, csv_filename):
        return self.gsts.createTableStmt(csv_filename)
    
    def createTblAtH2(self, csv_filename):
        ### create 
        #crt_stmt = self.createTblStmt(csv_filename)
        crt_stmt = self.createTableStmt(csv_filename)
        self.timeit(self.runQuery(crt_stmt))
    """ 
        
    """ insert rows from csvs """
    def insert_csv_to_database(self, csv_filename, delimeter = ','):
        # import csv to pandas dataframe
        import pandas as pd
        df = pd.read_csv(csv_filename, delimeter)
        col_names = df.columns.values
   
        place_holder_str = ''
        for i in range(len(col_names)):
            place_holder_str += ':{}, '.format(str(i+1))
        place_holder_str = place_holder_str[:-2]
                
        table_name = [x for x in map(str.strip, csv_filename.split('.')) if x][0]
        #insert_sql_str = 'insert into {} ({}) values ({})'.format(table_name, str_col_names, place_holder_str)
        
        data = list(map(tuple, df.itertuples(index=False)))
        for ins_stmt in data: self.cursor.execute('insert into {} values {}'.format(table_name, ins_stmt))
        self.conn.commit()
        print 'SUCCESS!! {} rows are inserted to TABLE {}'.format(df.shape[0], table_name)
    
""" testing """                                  
h2Ser8094 = H2SerConn(8094, "test", "", "")
print "success"