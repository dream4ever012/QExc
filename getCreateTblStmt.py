# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 20:41:24 2018

@author: hkim85
"""
class getCreateTblStmt(object):  
    
    """ get create table sql """
    ### TO-DO: deal with error gracefully
    # if filename is incorrect
    def __init__(self):    
        pass          
    
    """ to get create sql """
    def getIDVarStr(self, table_name, col_name, col_val_len):
        return "DROP TABLE {} IF EXISTS; CREATE TABLE {} (\
        {}      VARCHAR({})       NOT NULL    PRIMARY KEY, \
        {});".format(table_name, table_name, col_name, col_val_len, "{}")        
        
    def getIDVarStrTM(self, table_name, col_name1, col_val_len1, col_name2, col_val_len2):
        temp = "DROP TABLE {} IF EXISTS; CREATE TABLE {} (\
        {}      VARCHAR({})       NOT NULL    , \
        {}      VARCHAR({})       NOT NULL); {};".format(table_name, table_name, col_name1, col_val_len1, col_name2, col_val_len2, "{}")        
        temp2 = "ALTER TABLE {} ADD PRIMARY KEY ({}, {})".format(table_name, col_name1, col_name2)
        return temp.format(temp2)
        # 
    
    def getNonIDVarStr(self, col_name, col_val_len):
        ### create portion of insert str for non_ID varible
        ### all variable is NOT NULL
        temp = "{}  VARCHAR({})  NOT NULL, {}"
        return temp.format(col_name, col_val_len, "{}")
    
    def createTableStmt(self, csv_filename, isTM=False):
        import pandas as pd
        df = pd.read_csv(csv_filename)
        col_names = df.columns.values
        col_val_lens = [int(len(max(df[col_name], key=len))*1.5) for col_name in df] # get max len *1.5 of each col
        table_name = [x for x in map(str.strip, csv_filename.split('.')) if x][0] # get table name
        if isTM == False:
            create_tables_str = self.getIDVarStr(table_name, col_names[0], col_val_lens[0])
            for i in range(1, len(col_val_lens)): 
                create_tables_str = create_tables_str.format(self.getNonIDVarStr(col_names[i], col_val_lens[i]))
            create_tables_str = create_tables_str.replace(", {}", "")
        else: ### if creating TM table
            create_tables_str = self.getIDVarStrTM(table_name, col_names[0], col_val_lens[0], col_names[1], col_val_lens[1])
        
        return create_tables_str
    
    
    def createTMStmt(self, csv_filename):
        import pandas as pd
        df = pd.read_csv(csv_filename)
        col_names = df.columns.values
        col_val_lens = [int(len(max(df[col_name], key=len))*1.5) for col_name in df] # get max len *1.5 of each col
        table_name = [x for x in map(str.strip, csv_filename.split('.')) if x][0] # get table name
        create_tables_str = self.getIDVarStrTM(table_name, col_names[0], col_val_lens[0], col_names[1], col_val_lens[1])
        return create_tables_str