# -*- coding: utf-8 -*-
#!/usr/bin/env python

import sys
import pprint
import time
import base64
import hashlib
sys.path.append('/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/')
import json
import argparse
import logsparseLib


def parse_params():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', '--debug', action='store_true',
                        help='debug mode', default=False)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='verbose mode', default=False)
    parser.add_argument('-c', '--count', type=int,
                        help='print this count of queries of each context, 5 by default', default=5)
    parser.add_argument('globs', type=str, nargs='*')
    loc_params = parser.parse_args()
    return loc_params


def process_event(event):
    context = event.get_property('Context')
    if not context:
        return
    dur_sec = event.dur_sec
    hash_context = hashlib.sha1(context).hexdigest()
    if not contexts.has_key(hash_context):
        contexts[hash_context] = {}
        contexts[hash_context]['texts'] = []
        contexts[hash_context]['count'] = 0
        contexts[hash_context]['dur_sec'] = 0
        contexts[hash_context]['context'] = context
    contexts[hash_context]['count'] += 1
    contexts[hash_context]['dur_sec'] += dur_sec
    contexts[hash_context]['texts'].append(event.get_property('Sql'))
    
    
def print_result():
    for hash_context in sorted(contexts.keys(), key=lambda hash_context: contexts[hash_context]['dur_sec'], reverse=True):
        print("avg dur {:+f}s; dur {}s; count {}; hash {} \ncontext {}".format(contexts[hash_context]['dur_sec']/contexts[hash_context]['count'],
                                              str(contexts[hash_context]['dur_sec']),
                                              str(contexts[hash_context]['count']), 
                                              hash_context,
                                              contexts[hash_context]['context']) + '\n\n\n')
    

if __name__ == '__main__':

    params = parse_params()
    
    begin = time.time()

    # ќбъ€вл€ю глобальные переменные
    contexts = {}
    result = {}

    # ѕолучаю строковый генератор событий из библиотеки передав список параметров - глоб
    str_events = logsparseLib.read_events_from_files(params.globs, filter='DBPOSTGRS', filter_operation = 'eq')
    i = 0
    for str_event in str_events:
        i += 1
        if params.debug:
            print(str_event+'\n')
        if not i % 10000:
            print i
        try:
            event = logsparseLib.Event(str_event)
        except:
            logsparseLib.log('cant modify event \n' + str_event, params=params)
            next
        process_event(event)
        
    print_result()