# -*- coding: utf-8 -*-
#!/usr/bin/env python

"""Скрипт предназначен для анализа вывода скрипта analyze_background_sessions.py
Скрипт analyze_background_sessions.py выводит файл с выводом:

Этот вывод надо проанилизировать, а именно:
определить по какому контексту сколько ушло времени, например, сколько времени уходит на перепроведение документов, а сколько на очередь заданий и т.д
На вход скрипту надо подавать имя файла с результатом analyze_background_sessions.py

Что необходимо проанализировать:
1. По каким контекстам сколько уходит времени
    В абсолютных цифрах
    В относительном выражении от общего времени выполнения
2. Распределение времени по видам событий
    Сколько времени уходит на чистое процессороное время
        В абсолютных цифрах
        В относительных от общего времени выполнения по контексту"""

import sys
import os
import pprint
import json


def define_context_sign():
    """Определяем список строк-признаков, по которым однозначно можно определить чем занималось фоновое задание"""
    context_signs = (
      u"ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль", #  Работа воркеров очереди заданий
      u"РегистрСведений.ЗадачиБухгалтера.МодульМенеджера", # Фоновое задание обновление начального экрана в БП для руководителя
      u"Обработка.ГрупповоеПерепроведениеДокументов.МодульМенеджера", # Групповое перепроведение, в представлении не нуждается
      u"РегламентированнаяОтчетность.ВыполнитьПроверку", # Работа с регламентированной отчетностью
      u"БухгалтерскиеОтчетыВызовСервера.СформироватьОтчет", # Формирование отчетов
      u"ПроверитьВебСервисомФНС", # проверка контрагентов
      u"Обработка.ЗакрытиеМесяца.МодульМенеджера"
     )
    for sign in context_signs:
        context_result[sign] = {'count': 0,
                                'dur': 0,
                                'event_types': {}}
    return context_signs


def print_session(session):
    print 'dur = ' + str(session['dur'])
    pprint.pprint(session['types'])
    for line in sorted(session['context_lines'].keys(),
                       key=lambda line: session['context_lines'][line], reverse=True)[:11]:
        print(str(session['context_lines'][line]) + " " + line)

if __name__ == '__main__':
    context_result = {}
    context_signs = define_context_sign()
    bad_dur = 0

    fd = open(sys.argv[1], 'rb')
    main_str = fd.read()
    result = json.loads(main_str)
    sessions = result['sessions']
    for session in sessions:
        flag = 0
        for line in sessions[session]['context_lines'].keys():
            if flag:
                break
            for sign in context_signs:
                if line.find(sign) > -1:
                    context_result[sign]['count'] += 1
                    context_result[sign]['dur'] += sessions[session]['dur']
                    for event_type in sessions[session]['types']:
                        if event_type not in context_result[sign]['event_types'].keys():
                            context_result[sign]['event_types'][event_type] = {'count': 0, 'dur': 0}
                        context_result[sign]['event_types'][event_type]['count'] += sessions[session]['types'][event_type]['count']
                        context_result[sign]['event_types'][event_type]['dur'] += sessions[session]['types'][event_type]['dur']
                    flag = 1
                    break
        # Если не нашли ни одного события, то выводим информацию по сессии
        if not flag:
            bad_dur += sessions[session]['dur']
            print_session(sessions[session])

    # Вывожу результаты
    print('\n\ntotal duration : {0}s'.format(result['total_dur']))
    print(str(bad_dur) + u" нераспределенное время")
    for res in sorted(context_result.keys(), key=lambda res: context_result[res]['dur'], reverse=True):
        print("{:.2%}; dur {}s; count {} ".format(context_result[res]['dur']/result['total_dur'],
                                              str(context_result[res]['dur']),
                                              str(context_result[res]['count'])) + res)
        for event_type in context_result[res]['event_types']:
            print("\t{}; dur {}s ({:.2%}); count {}".format(event_type,
                            context_result[res]['event_types'][event_type]['dur'],
                            context_result[res]['event_types'][event_type]['dur']/context_result[res]['dur'],
                            context_result[res]['event_types'][event_type]['count']))
        print("")

    pass

# total duration : 7929.80716
# 1129.456284
# 4.70548974863 dur 373.136263 count 8 РегламентированнаяОтчетность.ВыполнитьПроверку
# 25.9092541791 dur 2054.553893 count 34 РегистрСведений.ЗадачиБухгалтера.МодульМенеджера
# 4.52700805652 dur 358.983009 count 3 Обработка.ГрупповоеПерепроведениеДокументов.МодульМенеджера
# 3.1964969877 dur 253.476047 count 41 БухгалтерскиеОтчетыВызовСервера.СформироватьОтчет
# 27.7176027948 dur 2197.952451 count 105 ПроверитьВебСервисомФНС
# 19.7009735732 dur 1562.249213 count 4 ВыполнитьЗаданиеОчереди

# total duration : 23665.488344
# 2502.398896
# 5.10581110534 dur 1208.315132 count 18 РегламентированнаяОтчетность.ВыполнитьПроверку
# 19.6105818673 dur 4640.939966 count 71 РегистрСведений.ЗадачиБухгалтера.МодульМенеджера
# 14.5822989023 dur 3450.972247 count 13 Обработка.ГрупповоеПерепроведениеДокументов.МодульМенеджера
# 4.39863507936 dur 1040.958472 count 117 БухгалтерскиеОтчетыВызовСервера.СформироватьОтчет
# 31.5572694695 dur 7468.181928 count 374 ПроверитьВебСервисомФНС
# 14.1713606508 dur 3353.721703 count 19 ВыполнитьЗаданиеОчереди
