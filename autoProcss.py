# -*- coding: utf-8 -*-
"""
Created on Thu Aug 16 15:45:17 2018

@author: hkim85
"""

import os
import jaydebeapi
import pandas as pd
from datetime import datetime

cwd = os.getcwd()
PATH = 'C:\\Users\\hkim85.823WS2016-03\\OneDrive\\Documents\\Caleb\\2016_PhDinCS\\201603_IndStudy\\201709IndStu\\InfusionPump\\InfPump_Data_20170923\\'
PATH = 'C:\\Users\\hkim85\\Documents\\h2\\bin'#'C:\\Program Files (x86)\\H2\\bin'
os.chdir(PATH)
os.getcwd()


def get_connection(url="tcp://140.192.38.17:8094", dbName="test2", idd = "", pw = "",\
                   jarPath = "C://Program Files (x86)//H2//bin//h2-1.4.197.jar"):
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



########## start a 

import sys
print('version is', sys.version)
print('sys.argv is', sys.argv)

p = subprocess.Popen(["echo", "hello world"], stdout=subprocess.PIPE)


import subprocess  
subprocess.Popen('ls -la', shell=True) 

subprocess.check_output("echo Hello World!", shell = True)
subprocess.check_output(["echo", "Hello World!"])


import os
os.popen("Your command here")





subprocess.call(['java', '-cp', 'h2-1.4.196.jar', 'org.h2.tools.Server', '-webAllowOthers', '-webPort 8992', '-tcpAllowOthers', '-tcpPort', '8994', '-baseDir', 'C:\Users\hkim85.823WS2016-03\Downloads', '-pgAllowOthers', '-pgPort', '8996'])
subprocess.call('java -cp h2-1.4.196.jar org.h2.tools.Server -webAllowOthers -webPort 8992 -tcpAllowOthers -tcpPort 8994 -baseDir C:\Users\hkim85.823WS2016-03\Downloads -pgAllowOthers -pgPort 8996', shell=True)

import shlex, subprocess
command_line = raw_input()
args = shlex.split(command_line)



conn_test2 = get_connection("tcp://140.192.38.17:8094", "test2")
cur_test2 = get_cursor(conn_test2)