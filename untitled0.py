# -*- coding: utf-8 -*-
"""
Created on Tue Sep 11 20:41:24 2018

@author: hkim85
"""
class GetCreateTblSQL(object):  
    import os
    """ get create table sql """
    ### TO-DO: deal with error gracefully
    # directory does not exist
    # file does not exist
    #
    def __init__(self, csvFileName, filePath=None):
        self.csvFileName = csvFileName
        self.filePath = filePath if filePath is None else 
        
isNone

string = 'hi' if 1 != 1 else 'no'
        
        
                
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