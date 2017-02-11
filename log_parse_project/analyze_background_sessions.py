# -*- coding: utf-8 -*-
#!/usr/bin/env python

'''
Скрипт предназначен для анализ сеансов.
Доделать - впихнуть пример использования.

На выходе получаем список экземпляров класса сеансов.

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
import logsparseLib


def process_event(event):
    """Пока что функция определяет для события начальную и конечную дату сессии
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

    # Добавляем информацию по дилетльности типов событий по всеми сессиям
    logsparseLib.define_dur_count_dict(types, event)

    # Анализируем контекст
    # Для каждой строки контекста получаем количество попаданий в события
    context = event.get_property('Context')
    if context:
        # Избавляемся от апострофов
        if context[0] == "'":
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
    для каждой сесси вычисляет время работы : дата последнего события - дата первого события"""
    tsum = 0
    for session in sessions:
        dur = sessions[session]['end'] - sessions[session]['begin']
        dur_sec = dur.total_seconds()
        sessions[session]['dur'] = dur_sec
        tsum += dur_sec

    print tsum
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
    begin = time.time()

    # Объявляю глобальные переменные
    sessions = {}
    types = {}

    # Получаю строковый генератор событий из библиотеки передав список параметров - глоб
    str_events = logsparseLib.read_events_from_files(sys.argv[1:], filter='t:applicationName=BackgroundJob')
    i = 1
    for str_event in str_events:
        # print(str_event+'\n')
        if not i % 10000:
            print i
        event = logsparseLib.Event(str_event)

        process_event(event)
        i += 1

    analyze()

    print(time.time() - begin)
