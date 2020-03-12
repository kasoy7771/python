# -*- coding: utf-8 -*-
#!/usr/bin/env python

"""Скрипт предназначен для анализа вывода скрипта analyze_background_sessions.py
Скрипт находит в файлах записи по фильтру и заносит их в базу данных
"""

import sys
import pprint
import time
sys.path.append('/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/')
import json
import argparse
import pprint
import datetime
import psycopg2
import logsparseLib
import re

CHECK_BASE_TRUTH = re.compile('^[a-zA-Z]+_(demo_)?\d+$')

def parse_params():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true',
                        help='debug mode', default=False)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='verbose mode', default=False)
    parser.add_argument('-n', '--number_events', type=int,
                        help='number events for output, 10 by defaul', default=10)
    parser.add_argument('-t', '--table_name', type=str,
                        help='name of table for inserting', default='events')
    parser.add_argument('globs', type=str, nargs='*')
    loc_params = parser.parse_args()
    return loc_params

def get_cur_date():
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

def get_first_line(string):
    if string:
        for line in string.splitlines():
            if line.strip() != '' and line.strip() != "'":
                return line.strip()


def get_last_line(string):
    if string:
        lines = string.splitlines()
        if  lines[-1].strip() != '' and lines[-1].strip() != "'":
            return lines[-1].strip()
        else:
            return lines[-2].strip()

params = parse_params()

conn = psycopg2.connect("dbname={} user={} password={} host={}".format('tj', 'dba_qmcpg_27', 'aaaaaa', 'localhost'))
cur = conn.cursor()

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' and table_name = '{0}'".format(params.table_name))
answer = cur.fetchone()
if not answer:
    cur.execute("""CREATE TABLE {} (date timestamp, microsec bigint, duration bigint, event_type varchar(10),
process varchar(10), base varchar(100), clientID bigint, connectID bigint, applicationName varchar(100),
computerName varchar(100), Usr varchar(100), SessionID varchar(50), Context text, Context_First text, Context_Last text, Interface varchar(36),
IName varchar(100), Method varchar(100), CallID bigint, MName varchar(100), Memory bigint, MemoryPeak bigint, InBytes bigint, OutBytes bigint, CpuTime bigint, OSThread bigint,
Trans bigint, dbpid bigint, Sql text, RowsAffected bigint, Result varchar(100), conf varchar(10), real_base boolean, node bigint, is_lin boolean);""".format(params.table_name))
conn.commit()

str_events = logsparseLib.read_events_from_files(params.globs, filter='', filter_operation='', params=params)

for str_event in str_events:
    try:
        event = logsparseLib.Event(str_event)
        if params.verbose or params.debug:
            print str_event
        conf = ''
        real_base = False
        node = 0
        is_lin = True
        if event.get_property('p:processName'):
            real_base = (CHECK_BASE_TRUTH.match(event.get_property('p:processName')) != None)
            if real_base:
                parts = event.get_property('p:processName').split('_')
                conf = parts[0]
                node = int(parts[-1])
                is_lin = node >= 23


        cur.execute("""
INSERT INTO """ + params.table_name + """ (date,microsec,duration,event_type,process,base,clientID,connectID,applicationName,computerName,Usr,SessionID,Context,Context_First,Context_Last,Interface,IName,Method,CallID,MName,Memory,MemoryPeak,InBytes,OutBytes,CpuTime,OSThread,Trans,dbpid,Sql,RowsAffected,Result,conf,real_base,node,is_lin)
VALUES (%(date)s,%(microsec)s,%(duration)s,%(event_type)s,%(process)s,%(base)s,%(clientID)s,%(connectID)s,%(applicationName)s,%(computerName)s,%(Usr)s,%(SessionID)s,%(Context)s,%(Context_First)s,%(Context_Last)s,%(Interface)s,%(IName)s,%(Method)s,%(CallID)s,%(MName)s,%(Memory)s,%(MemoryPeak)s,%(InBytes)s,%(OutBytes)s,%(CpuTime)s,%(OSThread)s,%(Trans)s,%(dbpid)s,%(Sql)s,%(RowsAffected)s,%(Result)s,%(conf)s,%(real_base)s,%(node)s,%(is_lin)s);
""", {'date': event.datetime, 'microsec': event.microsec, 'duration': event.dur, 'event_type': event.type, 'process': event.get_property('process'), 'base': event.get_property('p:processName'), 'clientID': event.get_property('t:clientID'),
'connectID': event.get_property('t:connectID'), 'applicationName': event.get_property('t:applicationName'), 'computerName': event.get_property('t:computerName'),
'Usr': event.get_property('Usr'), 'SessionID': event.get_property('SessionID'), 'Context': event.get_property('Context'), 'Context_First': get_first_line(event.get_property('Context')),
'Context_Last': get_last_line(event.get_property('Context')), 'Interface': event.get_property('Interface'), 'IName': event.get_property('IName'), 'Method': event.get_property('Method'),
'CallID': event.get_property('CallID'), 'MName': event.get_property('MName'), 'Memory': event.get_property('Memory'), 'MemoryPeak': event.get_property('MemoryPeak'),
'InBytes': event.get_property('InBytes'), 'OutBytes': event.get_property('OutBytes'), 'CpuTime': event.get_property('CpuTime'), 'OSThread': event.get_property('OSThread'),
'Trans': event.get_property('Trans'), 'dbpid': event.get_property('dbpid') if event.get_property('dbpid') else 0, 'Sql': event.get_property('Sql'), 'RowsAffected': event.get_property('RowsAffected'),
'Result': event.get_property('Result'), 'conf': conf, 'real_base': real_base, 'node': node, 'is_lin': is_lin})
    except:
        print('problems')


conn.commit()
cur.close()

conn.close()

