# -*- coding: utf-8 -*-
#!/usr/bin/env python

"""Скрипт предназначен для анализа вывода скрипта analyze_background_sessions.py
Скрипт analyze_background_sessions.py выводит файл с выводом:
"""

import sys
import pprint
import time
sys.path.append('/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/')
import json
import argparse
import pprint
import datetime
import logsparseLib

def parse_params():
    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--json_format', action='store_true',
                        help='output in json format', default=False)
    parser.add_argument('-a', '--all_contexts', action='store_true',
                        help='output all context, by default only 10 top', default=False)
    parser.add_argument('-d', '--debug', action='store_true',
                        help='debug mode', default=False)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='verbose mode', default=False)
    parser.add_argument('-o', '--out_file', type=argparse.FileType('w'),
                        help='output file', default=sys.stdout)
    parser.add_argument('-n', '--number_events', type=int,
                        help='number events for output, 10 by defaul', default=10)
    parser.add_argument('globs', type=str, nargs='*')
    loc_params = parser.parse_args()
    return loc_params


def define_empty_max_diffs(params):
	max_diffs = []
	for i in range(params.number_events - 1):
		max_diffs.append( {'diff': datetime.timedelta(microseconds=0), 'event': ''} )
	return max_diffs


def add_diff(diff, max_diffs, i, number_events):
	i+=1
	number_events = number_events-1
	if i > number_events:
		return
	for i in range(number_events):
		max_diffs[i+1] = max_diffs[i+1]


def define_max_diff(diff, event, max_diffs):
	'''Делаем что-то типо стека.
	Если находим значение большее чем текущий элемент массива, то
	этому элементу массива присваиваем это значение, а все остальные значения
	спускаем вниз по массиву'''
	number_events = len(max_diffs) - 1
	for i in range(number_events):
		if diff > max_diffs[i]['diff']:
			max_diffs[i]['diff'] = diff
			max_diffs[i]['event'] = event
			add_diff(diff, max_diffs, i, number_events)
			return

def print_max_diffs(max_diffs):
	for max_diff in max_diffs:
		res =  str(max_diff['diff'].total_seconds()) + ' ' 
		if max_diff['event']:
			res += max_diff['event'].get_str_event()
		print res

if __name__ == '__main__':

    params = parse_params()
    begin = time.time()

    # Объявляю глобальные переменные
    sessions = {}
    types = {}

    # Получаю строковый генератор событий из библиотеки передав список параметров - глоб
    str_events = logsparseLib.read_events_from_files(params.globs, filter='')
    i = 1
    last_date = ''
    diff = 0
    max_diffs = define_empty_max_diffs(params)
    for str_event in str_events:
        # print(str_event+'\n')
        if not i % 100:
            print i
            if params.debug:
            	print_max_diffs(max_diffs)
            	exit(0)
        event = logsparseLib.Event(str_event)
        if not last_date:
        	last_date = event.datetime
        diff = event.datetime - last_date
        define_max_diff(diff, event, max_diffs)
        last_date = event.datetime
        #print diff


        i += 1

    print_max_diffs(max_diffs)
    logsparseLib.log(str(time.time() - begin), params=params)