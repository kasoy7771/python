# -*- coding: utf-8 -*-
#!/usr/bin/env python

import sys
import pprint
import time
sys.path.append('/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/')
import json
import argparse
import pprint
import re
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
    parser.add_argument('globs', type=str, nargs='*')
    loc_params = parser.parse_args()
    return loc_params

    
def process_event(event):
    global on_posting
    global new_doc
    context = event.get_property('Context')
    func = event.get_property('Func')
    
    # Ищем начало транзакции - это будет началом проведения документа
    if func == 'BeginTransaction' and context.find('ДокументОбъект.Записать') > 0:
        new_doc = {'begin' : event.datetime,
                    'end'  : '',
                    'doc_type' : '',
                    'types': {},
                    'dur'  : 0}
        logsparseLib.define_dur_count_dict(new_doc['types'], event)
        # Говорим, что началось перепроведение
        on_posting = True
    # Ищем конец транзакции - это будет конец перепроведения документа
    elif func == 'CommitTransaction' and context.find('ДокументОбъект.Записать') > 0 and on_posting:
        new_doc['dur'] = event.dur_sec
        new_doc['end'] = event.datetime
        posted_docs.append(new_doc)
    # Прочто очередное событие
    elif on_posting:
        # Обновляем словарь длительности в разрезе событий
        logsparseLib.define_dur_count_dict(new_doc['types'], event)
        if not new_doc['doc_type']:
            res = doc_type_regex.search(context)
            if res:
                new_doc['doc_type'] = res.groups()[0]
        

def analyze():
    for doc in posted_docs:
        pprint.pprint(doc)
        print(doc['doc_type'])
        
        
if __name__ == '__main__':

    params = parse_params()

    begin = time.time()
    
    # Формирую необходимые регулярки
    doc_type_regex = re.compile(r'Документ\.([^\.]+?)\.')

    # Объявляю глобальные переменные
    posted_docs = []
    new_doc = ''
    on_posting = False

    # Получаю строковый генератор событий из библиотеки передав список файлов - глоб
    str_events = logsparseLib.read_events_from_files(params.globs, filter='t:applicationName=BackgroundJob', filter_operation='eq')
    i = 1
    for str_event in str_events:
        # print(str_event+'\n')
        if not i % 10000:
            print i
        event = logsparseLib.Event(str_event)

        process_event(event)
        i += 1
        if i == 12673:
            a=1

    analyze()

    logsparseLib.log(str(time.time() - begin) + " " + str(i), params=params)