# -*- coding: utf-8 -*-
#!/usr/bin/env python

import psycopg2
import os
import sys
import pprint
import argparse
import collections
import glob
from Config import *


# Парсим параметры
def parse_params():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--node', type=int, help = 'node number, like 35', required=True)
    parser.add_argument('-t', '--node_type', type=str, help = 'node type, either 1c, db',
                        required=True, choices=['1c', 'db'])
    parser.add_argument('-f', '--file', type=str, help = 'unix-like glob of parsed files', required=True)
    parser.add_argument('-v', '--verbose', action='store_true', help = 'parsed file', default=False)
    params = parser.parse_args()
    return params


def insert_tcpdump():
    # Определяю тупл для формирования инсерт-запроса
    sql_tuple = [params.node,
             params.node_type,
             "", #time
             "", #port
             "", #seq,
             ""] #acq

    # Получаем соединеие с базюлькой
    conn = psycopg2.connect("dbname={} user={} password={} host={}".format(config.base, config.user, 
                                                                           config.password, config.host))
    cur = conn.cursor()
    # Создаем таблицу, если еще не создана
    cur.execute("create table if not exists tcpdump (node int, node_type varchar(2), time numeric(18,6), port int, seq bigint, acq bigint);")

    # Открываем файлик с тисипидампиком
    for file in glob.glob(params.file):
    	print file
        f = open(file, 'rb')
        for line in f.xreadlines():
            fields = line.split()
            if params.verbose:
                print line
                print fields
            # Парсим строку из файла
            sql_tuple[2] = fields[0]
            sql_tuple[3] = int(fields[2].split('.')[-1])
            sql_tuple[4] = int(fields[8].split(':')[0])
            sql_tuple[5] = int(fields[10][0:-1])
            if params.verbose:
                print sql_tuple
            # Отправляем запросик на инсертик 
            cmd = """INSERT INTO tcpdump (node, node_type, time, port, seq, acq) 
            VALUES ({0}, '{1}', '{2}', {3}, {4}, {5});""".format(*sql_tuple)
            cur.execute(cmd)
        # Сохраняем результатик
        conn.commit()
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    params = parse_params()
    config = Config()
    insert_tcpdump()
