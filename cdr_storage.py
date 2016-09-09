#!/usr/bin/env python
#coding=utf-8

import re
import sys
import MySQLdb as md
import smtplib
import os
import datetime
import time

DB_HOST = 'localhost'
DB_USER = 'cdrlog'
DB_PASS = 'miJtwCsYIxNA4nR'
DB_NAME = 'huawei'
LOG_DIR = '/var/log/huawei/cdr'
current_time = str(datetime.datetime.now() - datetime.timedelta(minutes=10)).split()[0]
LOG_FILENAME = current_time + '.cdr'
SQL_TABLE = current_time.rsplit('-',1)[0]
LOG_ABS_PATH = LOG_DIR + os.sep + LOG_FILENAME
LOG_SEEK = '0'
SEEK_FILENAME = 'cdr.seek'
SEEK_ABS_PATH = LOG_DIR + os.sep + SEEK_FILENAME
list_2000=[]
list_1000=[]


def send_error(msg):
    username = 'xxxxx@xxx.com'
    to_list = ['xxx@xxx.com']
    sub = 'MTG CDR Storage failed !'
    mail = smtplib.SMTP()
    mail.connect('smtp.qq.com')
    mail.login(username, 'xxxxxxx')
    body = '\r\n'.join((
        'From:%s' % username,
        'To:%s' % to_list,
        'Subject:%s' % sub,
        '',
        str(msg),
    ))
    mail.sendmail(username, to_list, body)
    mail.quit()

if not os.access(LOG_ABS_PATH, os.R_OK):
    send_error('The log file is unreadable !')
    sys.exit('The log file is unreadable !')

if not os.path.exists(SEEK_ABS_PATH):
    try:
        os.system('touch %s' % SEEK_ABS_PATH)
    except:
        send_error('Seek file creation failed !')
        sys.exit('Seek file creation failed !')

if not os.access(SEEK_ABS_PATH, os.W_OK) or not os.access(SEEK_ABS_PATH, os.R_OK):
    send_error('Seek file read failed !')
    sys.exit('Seek file read failed !')
else:
    if os.path.getsize(SEEK_ABS_PATH) == 0:
        pass
    else:
        with file(SEEK_ABS_PATH, 'rb') as f: LOG_SEEK = f.read()
    
with file(LOG_ABS_PATH) as logfile:
    logfile.seek(int(LOG_SEEK))
    for line in logfile.xreadlines():
            list_1000.append(line)
    else:
        LOG_SEEK = logfile.tell()

def sql(list_,table):
    sql_list = []
    data3 = {}
    for data1 in list_:
        data2 = data1.split(',')
        data3['host']=data2[0].split(' ')[9]
        data3['callid']=data2[0].split(':')[-1]
        for i in data2[1:27]:
           data3[i.split(':',1)[0].strip()] = i.split(':',1)[1]
        keys = str(data3.keys()).strip("[]")
        values = [data3[x.strip('\',')] for x in keys.split()]
        sql = "INSERT INTO %s(%s) VALUES(%s)" % (table,keys.replace('\'', ''), str(values).strip('[]'))
        sql_list.append(sql)
    return sql_list

def exec_sql(sql_list):
    for sql in sql_list:
        try:		
            cursor.execute(sql)
        except:
            db.rollback()
            db.close()
            send_error('syntax error.')
            sys.exit('syntax error.')
    else:
        db.commit()

sql_list_1 = sql(list_1000,db_table)

if len(sql_list_1)==0:
    send_error("Insert 0 CDR To Mysql <1000>")

try:
    db = md.connect(DB_HOST,DB_USER,DB_PASS,DB_NAME)
except:
    send_error("Login failed.")
    sys.exit('Login failed.')
cursor = db.cursor()

exec_sql(sql_list_1)
db.close()
if str(datetime.datetime.now()).split()[0] != current_time:
	LOG_SEEK = 0
    
with file(SEEK_ABS_PATH, 'wb') as f:
    f.write(str(LOG_SEEK))
