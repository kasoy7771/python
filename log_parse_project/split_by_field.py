# -*- coding: utf-8 -*-
#!/usr/bin/env python

''' Скипт предназначен для парсинга логов технологического журнала платформы 1С:Предприятие.
Пример использования:
cat /home/Kosilov_N/Investigation/170125/all/rphost_*/* | python split_by_field.py --property t:connection
Скипт для каждого значения <property> скрипт создаст файл со всеми событиями, содержащими это значение.
Если скрипт не находит в событии свойства, но событие будет записано в файл none.
Указание --property допускается без учета регистра'''

import argparse
import sys
import re
import time
import logsparseLib


def parse_params():
    '''Фукнция пасит параметры'''
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--property', type=str,
                        help='property for splitting', required=True)
    parser.add_argument('-d', '--debug', action='store_true',
                        help='debug mode', default=False)
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='verbose mode', default=False)
    loc_params = parser.parse_args()
    return loc_params


def delete_oldest_file():
    '''Функция удаляет самый старый файл из словаря файлов'''
    k = files.keys()
    k.sort(key=lambda i: files[i]['access'])
    last_file = k[0]
    files[last_file]['fd'].close()
    del files[last_file]


def write_in_file(loc_event, loc_property):
    '''Функция добавляет в файл найденное свойство'''

    # Если мы не нашли нужный файл среди открытых
    if loc_property not in files:
        # Если максимальное количество открытых файлов ичерпано, то удаялем самый старый файл
        if len(files) >= FILES_MAX:
            delete_oldest_file()
        # Теперь добавляем новый файл
        files[loc_property] = {}
        # Если свойство уже встречалось, то открываем файл на добавление
        if loc_property in properties:
            files[loc_property]['fd'] = open(
                params.property + '=' + loc_property, 'ab')
        # Если свойство не встречалось, то открываем файл на создание нового
        else:
            # Добавялем в словарь в свойств значение, чтобы понимать, что файл уже существует
            properties[loc_property] = ''
            files[loc_property]['fd'] = open(
                params.property + '=' + loc_property, 'wb')
    # На этом этапе файл точно существует, пишем в него
    files[loc_property]['fd'].write(loc_event)
    # Обновляем время доступа к файлу
    files[loc_property]['access'] = time.time()

    if loc_property in properties:
        f = open(params.property + '=' + loc_property, 'ab')
    # Если свойство не встречалось, то открываем файл на создание нового
    else:
        properties[loc_property] = ''
        f = open(params.property + '=' + loc_property, 'wb')
    f.write(loc_event)
    f.close()


if __name__ == '__main__':
    # Парсим параметры
    params = parse_params()
    # Создаем генератор на cобытия ТЖ
    events = logsparseLib.read_events_from_steam(sys.stdin)
    # Формируем регулярку для поиска свойства и вычленения его значения
    property_finder = re.compile(',{0}=([^,]+?),'.format(params.property),
                                 re.IGNORECASE)
    # Инициализируем словарь с найденными свойствами
    properties = {}
    # Инициализируем словарь для работы с файлами
    files = {}
    # Инициализируем максимальное количество открытых файлов
    FILES_MAX = 512

    # Для каждого события ищем свойство
    for event in events:
        logsparseLib.log(event, params, 'debug')
        result = property_finder.findall(event)
        # Если нашли, то формируем файл с именем - значением свойства
        if result:
            logsparseLib.log('FIND' + event, params, 'debug')
            write_in_file(event, result[0])
        # Если не нашли, то формируем файл с именем none
        else:
            logsparseLib.log('NOT FIND' + event, params, 'debug')
            write_in_file(event, 'none')

    # Закрываем все открытые файлы в конце
    for file in files.iterkeys():
       files[file]['fd'].close()
