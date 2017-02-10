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
import logsparseLib


def process_event(event):
    '''Пока что функция определяет для события начальную и конечную дату сессии
    Смущает то, что оказывается номер сессии может быть не уникаьлным, но посмотрим'''

    # Получаю номер сессии
    SessionID = event.get_property('SessionID')

    # Если в событии нет сессии, то выхожу
    if not SessionID:

        return

    # Если сессия еще не встречалась, то заполню необходимые свойства
    if SessionID not in sessions:
        sessions[SessionID] = {'begin': event.datetime,
                               'end': event.datetime}
        return

    # Если дата подходит, то меняем ее в словаре сессий
    if event.datetime < sessions[SessionID]['begin']:
        sessions[SessionID]['begin'] = event.datetime
    if event.datetime > sessions[SessionID]['end']:
        sessions[SessionID]['end'] = event.datetime


def analyze():
    '''Функция анализирует количество уникальных сессий,
    понимает количество сессий,
    для каждой сесси вычисляет время работы : дата последнего события - дата первого события'''
    sum = 0
    for session in sessions:
        dur = sessions[session]['end'] - sessions[session]['begin']
        sessions[session]['dur'] = dur.microseconds
        sum += dur.microseconds

    print sum
    print len(sessions.keys())
    pprint.pprint(sessions)

if __name__ == '__main__':
    # Объявляю глобальные переменные
    sessions = {}

    # Получаю строковый генератор событий из библиотеки передав список параметров - глоб
    str_events = logsparseLib.read_events_from_files(sys.argv[1:], filter='t:applicationName=BackgroundJob')
    i=1
    for str_event in str_events:
        # print(str_event+'\n')
        if not i % 10000:
            print i
        event = logsparseLib.Event(str_event)

        process_event(event)
        i+=1

    analyze()