# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
Скрипт предназначен для анализ сеансов.
Доделать - впихнуть пример использования.

На выходе скрипт выводит информацию о сеансах. Дату начала, дату окончания, сколько ро.

Что мы должны сделать:
На вход мы должны передать список файлов глобом
Пройтись по всем файлам, выдернуть оттуда все события.
По событию понять, что это за сеанса.
    Выдернуть номер сеанса,
    выдернуть имя пользователя.
    Тип: ФЗ, расширение веб-сервиса, и т.д.
    Первое событие - для этого мы должны знать дату из названия файла.
    Последнее событие
    Длительность работы сеанса
    События:
        Тип события - длительность
    Выделить длительность чистого вызова
        Вызов минус Длительность сколлов, минус длительность СУБД
    Контекст
        Из всех строк контекста получаем первые 5.
            для каждой строки получаем количество вхождений.
            оставляем первые 5.

Первые задачи:
    Сделать непрерывный генератор для входящих файлов.
    Сделать универсальный парсер для событий.
'''

import sys
import pprint
import time
sys.path.append('/System/Library/Frameworks/Python.framework/Versions/2.7/lib/python2.7/')
import json
import argparse
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
    """Функция определяет для события начальную и конечную дату сессии,
	заполняет сколько сессия провела в каждом из типов событий (sdbl, call и т.д.),
	распарсивает контекст, в котором работает сессия.
		Как распарсиавает контекст. Она берет контекст, делит его по строкам
		добавляет каждую строку в словарь строк контекстов
		на выходе мы получаем для каждой сессии сколько раз встречалась каждая из строк контекста.
		В дальнейшем это позволит понять чем занималась сессия.
    Смущает то, что оказывается номер сессии может быть не уникаьлным, но посмотрим"""

    # Получаю номер сессии
    SessionID = event.get_property('SessionID')

    # Если в событии нет сессии, то выхожу
    if not SessionID:
        return

    # Если сессия еще не встречалась, то заполню необходимые свойства
    if SessionID not in sessions:
        sessions[SessionID] = {'begin': event.datetime,
                               'end': event.datetime,
                               'types': {},
                               'context_lines': {}}
        logsparseLib.define_dur_count_dict(sessions[SessionID]['types'], event)
        logsparseLib.define_dur_count_dict(types, event)
        return

    # Если дата подходит, то меняем ее в словаре сессий для определения начала и конца работы сессии
    if event.datetime < sessions[SessionID]['begin']:
        sessions[SessionID]['begin'] = event.datetime
    if event.datetime > sessions[SessionID]['end']:
        sessions[SessionID]['end'] = event.datetime

    # Добавляем информацию по длительности типов событий для сессии
    if event.type not in sessions[SessionID]['types'].keys():
        logsparseLib.define_dur_count_dict(sessions[SessionID]['types'], event)
    else:
        logsparseLib.update_dur_count_dict(sessions[SessionID]['types'], event)

    # Добавляем информацию по длиетльности типов событий по всеми сессиям
    logsparseLib.define_dur_count_dict(types, event)

    # Анализируем контекст
    # Для каждой строки контекста получаем количество попаданий в события
    context = event.get_property('Context')
    if context:
        # Избавляемся от апострофов
        if context[0] == "'" or context[0] == '"':
            context = context[1:-1]
        for line in context.splitlines():
            line = line.strip()
            if line not in sessions[SessionID]['context_lines']:
                sessions[SessionID]['context_lines'][line] = 1
                continue
            sessions[SessionID]['context_lines'][line] += 1


def analyze():
    """Функция анализирует количество уникальных сессий,
    понимает количество сессий,
    для каждой сессии вычисляет время работы : дата последнего события - дата первого события"""
    total_dur = 0
    for session in sessions:
        dur = sessions[session]['end'] - sessions[session]['begin']
        dur_sec = dur.total_seconds()
        sessions[session]['dur'] = dur_sec
        sessions[session]['begin'] = sessions[session]['begin'].__str__()
        sessions[session]['end'] = sessions[session]['end'].__str__()
        total_dur += dur_sec

    # Формируем выходной json с результатами анализа
    if params.json_format:
        result = {}
        result['total_dur'] = total_dur
        result['count'] = len(sessions.keys())
        result['sessions'] = {}

        if params.all_contexts:
            result['sessions'] = sessions
            json.dump(result, open(params.out_file, 'wb'))
            return

        for session in sessions.keys():
            result['sessions'][session] = sessions[session]
            sessions[session]['begin'].__str__()
            context_lines = result['sessions'][session]['context_lines'].copy()
            result['sessions'][session]['context_lines'] = {}
            for line in sorted(context_lines .keys(), key=lambda line: context_lines [line], reverse=True)[:11]:
                result['sessions'][session]['context_lines'][line] = context_lines[line]
        json.dump(result, params.out_file)
        return


    # Предыдущий вывод
    print total_dur
    print len(sessions.keys())
    # pprint.pprint(sessions)
    for session in sorted(sessions.keys(), key=lambda session: sessions[session]['dur'], reverse=True):
        print ('-' * 50)
        print ('-' * 50)
        print session
        print 'dur = ' + str(sessions[session]['dur'])
        pprint.pprint(sessions[session]['types'])
        for line in sorted(sessions[session]['context_lines'].keys(), key=lambda line: sessions[session]['context_lines'][line], reverse=True)[:11]:
            print (str(sessions[session]['context_lines'][line]) + " " + line)
    pprint.pprint(types)


if __name__ == '__main__':

    params = parse_params()

    begin = time.time()

    # Объявляю глобальные переменные
    sessions = {}
    types = {}

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
