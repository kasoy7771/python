# -*- coding: utf-8 -*-
#!/usr/bin/env python
'''Эта библиотека предназначена для использования
скриптами по парсингу различных скриптов.
Содержит различные методы для облегчения быстрого
написания скриптов для парсинга'''

import re
import time
import datetime
import glob
import os

# Переменные, необходимые для работы
sec_quote = re.compile('("(?:""|[^"])*"),')
uno_quote = re.compile("('(?:''|[^'])*'),")
regexs = {"'": uno_quote,
          '"': sec_quote}
find_pairs = re.compile("""((?:[\w:]+)=(?:(?:"(?:(?:""|[^"])*)")|(?:'(?:(?:''|[^'])*)')|(?:[^,]*)))""")

'''
Функции для генерации цельных событий технологического
журнала платформы 1С:Предприятие 8.3
'''

# @profile
def steam_from_globs(globs):
    '''
    Функция создает генератор строк из файлов,
    получившихся из переданных глоб
    '''
    # Проходим по каждой переданной глобе
    for cur_glob in globs:
        # Из глобы получаем файлы
        files = glob.glob(cur_glob)
        # Проходим по каждому файлу
        for file in files:
            # Получаем имя файла без учета каталога
            basename = os.path.basename(file)
            # Из имени файла получаем дату ТЖ
            file_date = basename[:8]
            fd = open(file, 'rb')
            for line in fd.xreadlines():
                yield (line, file_date)
            fd.close()


# @profile
def read_events_from_files(globs, filter=''):
    '''
    Функция из переданных глоб создает генератор цельных
    событий технологического журнала платформы 1С:Предприятие 8.3
    в строковом виде
    '''

    # Комплируем регулярку для определения начала события
    new_line_regex = re.compile(r'\d\d:\d\d\.\d{6}-\d+,')
    first = True
    event = ''

    for cur_glob in globs:
        # Из глобы получаем файлы
        files = glob.glob(cur_glob)

        # Проходим по каждому файлу
        for file in files:

            basename = os.path.basename(file)
            # Из имени файла получаем дату ТЖ
            file_date = basename[:8]

            fd = open(file, 'rb')
            # Проходим по каждой строке из потока, чтобы сформировать из них цельные события
            for line in fd.xreadlines():

                # Если совпали с регуляркой, то собыите закончилось, можно отдавать
                if new_line_regex.search(line[:30]):
                    # Но только не для первой строки
                    if first:
                        # Особый случай для первой строки, она не является разделителем событий, надо пропустить передачу события
                        first = False
                        # Надо избавиться от BOM байта
                        if not line[0].isdigit():
                            garb, line = re.split('\D+', line, 1)
                        event += line
                        continue

                    if filter and filter not in event:
                        event = ''
                    else:
                        yield (file_date + event)
                    # Возвращаем дату файла + событие

                    # Обнуляем переменную с событием, так как началось новое
                    event = ''
                # Прибавляем строку, чтобы софрмировать цельное событие
                event += line
            # Возвращаем последнее событие
            yield (file_date + event)


def read_events_from_steam(stream):
    '''Функция создает генератор для возвращения событий технологического журнала в текстовом виде.

    На вход подается файловый дескриптор из которого можно читать'''
    new_line_regex = re.compile(r'\d\d:\d\d\.\d{6}-\d+,')

    first = True
    event = ''

    for line in stream.xreadlines():
        try:
            # Если мы совпали с регуляркой, или закочнился вывод, то строки события закончились, можно отдавать
            if not line or new_line_regex.search(line[:30]):

                # Особый случай для первой строки, она не является разделителем событий, надо пропустить передачу события
                if first:
                    first = False
                    event += line
                    continue

                # Отправляем событие вверх по стеку
                yield (event)
                # Обнуляем событие, чтобы начать следующее
                event = ''
            # Если с регуляркой не совпали, значит это продолжение начавшегося события
            event += line
        except KeyboardInterrupt:
            break

    # Старый работающий вариант
    # while 1:
    #     try:
    #         line = stream.readline()
    #         # Если мы совпали с регуляркой, или закочнился вывод, то строки события закончились, можно отдавать
    #         if not line or new_line_regex.search(line[:30]):
    #             # Особый случай для первой строки, она не является разделителем событий, надо пропустить передачу события
    #             if first:
    #                 first = False
    #                 event += line
    #                 continue
    #             yield(event)
    #             # Обнуляем событие, чтобы начать следующее
    #             event = ''
    #         # Прибавляем строку, чтобы софрмировать цельное событие
    #         event += line
    #     # Прерывамся, если это прерывание с ком. строки
    #     except KeyboardInterrupt:
    #         break
    #     # Прерываемся, если вывод закончился
    #     if not line:
    #         break


class Event(object):
    def __init__(self, str_event):
        self.parse2(str_event)
        self.def_date()

    #   @profile
    def parse2(self, str_event):
        # 07:39.109005-1,SCALL,6,process=rphost,p:processName=ea_5,t:clientID=189,t:applicationName=BackgroundJob,t:computerName=SRV1C5-1,t:connectID=21996,SessionID=12565,Usr=DefUser,ClientID=137,Interface=2ebdaa8c-4a75-48f7-94bf-8206623aa9bb,IName=IClusterLogMngr,Method=0,CallID=41271,MName=writeLogEntryData,Context='
        # ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 680 : ЗапланироватьЗадание(Выборка);
        #         ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 1559 : НСтр("ru = ''Исполняющее задание было принудительно завершено''"));
        #                 ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 1426 : + ?(ОбщегоНазначенияПовтИсп.ЭтоРазделеннаяКонфигурация() И ОбщегоНазначения.ЭтоРазделенныйОбъектМетаданных(ЗаписьЗадания.Метаданные().ПолноеИмя(),'
        str_event = str_event.strip()
        str_event = str_event + ','
        s = ''
        i = 0
        self.date = ''
        self.dur = ''
        self.type = ''
        self.nesting_level = ''
        self.properties = {}

        # Определяем дату
        self.date, other = str_event.split('-', 1)

        # Определяем длительность, тип, уровень вложенности
        self.dur, self.type, self.nesting_level, other = other.split(',', 3)
        self.dur = int(self.dur)
        self.dur_sec = float(self.dur)/1000000

        # while True:
        #     if other == '\r\n' or not other:
        #         break
        #     property, other = other.split('=',1)
        #     sep = other[0]
        #     if sep != '"' and sep != "'":
        #         parts = other.split(',',1)
        #         if len(parts) > 1:
        #             value, other = parts
        #         else:
        #             self.properties[property] = parts[0]
        #             return
        #     else:
        #         parts = regexs[sep].split(other, 1)
        #         if len(parts) > 2:
        #             value, other = parts[1:]
        #         else:
        #             self.properties[property] = parts[0]
        #             return
        #     self.properties[property] = value


        # Способ с файндоллом одной регуляркой
        pairs = find_pairs.findall(other)
        for pair in pairs:
            property, self.properties[property] = pair.split('=',1)

        # Способ с файндитером
        # for pair in find_pairs.finditer(other):
        #     self.properties[pair.group('property')] =

            # s = ''
        # sep = ''
        # while True:
        #     property, other = other.split('=', 1)
        #     if other[0] == '"':
        #         pass
        #     elif other[0] == '"':
        #         pass
        #     else:
        #         value, other = other.split(',', 1)

    def parse(self):
        s = ''
        i = 0
        event_len = len(self.str_event)
        self.date = ''
        self.dur  = ''
        self.type = ''
        self.nesting_level = ''
        self.properties = {}
        # Если событие не заканчивается переносом строки, значит событие повреждено
        if self.str_event[-2]+self.str_event[-1] != '\r\n':
            self.corrupter = True
            return

        # Определяем дату
        while True:
            s = self.str_event[i]
            if s == '-':
                i += 1
                break
            self.date += (s if s.isdigit() else '')
            i += 1

        # Определяем длительность
        s = ''
        while True:
            s = self.str_event[i]
            if s == ',':
                i += 1
                break
            self.dur += s
            i+=1

        # Определяем тип события
        s = ''
        while True:
            s = self.str_event[i]
            if s == ',':
                i += 1
                break
            self.type += s
            i += 1

        while True:
            s = self.str_event[i]
            if s == ',':
                i += 1
                break
            self.nesting_level += s
            i += 1

        s = ''
        sep = ''
        while True:
            # Условие выхода из цикла. Если мы перевалили за длину события. +1 здесь из-за того,
            # что в конец события всегда присутствует \n\r
            if i + 1 >= event_len:
                break

            property = ''
            while True:
                try:
                    s = self.str_event[i]
                except:
                    pass
                if s == '=':
                    i += 1
                    break
                property += s
                i += 1

            if self.str_event[i] in (r"'", r'"'):
                sep = (self.str_event[i])
                i += 1
            else:
                # Значение свойства может закончится либо запятой, либо переносом строки
                sep = (",",'\r')

            value = ''
            while True:
                try:
                    s = self.str_event[i]
                except:
                    pass
                # Почему такое условие смотри в сносках внизу
                if s in sep and self.str_event[i+1] not in sep and self.str_event[i-1] not in sep:
                    i += 1
                    if sep in (r"'", r'"'):
                        i += 1
                    break
                value += s
                i += 1

            self.properties[property] = value

    def def_date(self):
        #time.struct_time(tm_year=2017, tm_mon=2, tm_mday=2, tm_hour=21, tm_min=52, tm_sec=20, tm_wday=3, tm_yday=33,
        #                 tm_isdst=0)
        self.datetime = datetime.datetime(2000+int(self.date[0:2]), int(self.date[2:4]), int(self.date[4:6]),
                              int(self.date[6:8]), int(self.date[8:10]), int(self.date[11:13]),
                              int(self.date[14:]))
        pass

    def print_event(self):
        if self.str_event:
            print(self.str_event)
        else:
            log('Event is not defiened')

    def get_property(self, name):
        if name in self.properties:
            return self.properties[name]
        else:
            return None


'''
Служебные процедуры и функции
'''


def log(message, params=None, log_type='always', add_time=True):
    """Функция логгирует свойства в зависимости от переданных params,
    которые задаются в скрипт.
    Для корретной работы params должен содержать 2 атрибута
    --verbose
    --debug
    log_type может быть одним из 4х значений:
    - 'always'
    - 'debug'
    - 'verbose'
    - 'debug_verbose' """

    if add_time:
        message = time.ctime() + ' ' + message

    if log_type == 'always' or not params:
        print(message)
    elif log_type == 'debug' and params.debug:
        print(message)
    elif log_type == 'verbose' and params.verbose:
        print(message)
    elif log_type == 'debug_verbose' and (params.verbose or params.debug):
        print(message)


def define_dur_count_dict(dir_count_dict, event):
    """
    Функция предназначается для определения экземпряров словаря для подсчета
    количества и суммарной длительности событий определенного вида.
    Фунеция принимает на вход словарь и экземпляр класса event.
    Если в словаре нет ключа event.type, то функция добавляет в словарь этот ключ
    со значением
    {'dur': event.dur,
     'count': 1}
    """
    if event.type not in dir_count_dict.keys():
        dir_count_dict[event.type] = {'dur': event.dur_sec,
                                      'count': 1}
    else:
        update_dur_count_dict(dir_count_dict, event)


def update_dur_count_dict(dir_count_dict, event):
    """
    Функция предназначается для суммирования экземпряров словаря для подсчета
    количества и суммарной длительности событий определенного вида.
    Фунеция принимает на вход словарь и экземпляр класса event.
    Функция добавляет к значениям словаря dir_count_dict[] занчения из event
    """
    dir_count_dict[event.type]['dur'] += event.dur_sec
    dir_count_dict[event.type]['count'] += 1



# Чтобы распарсить строку события в объект событие нужно учитывать следующие возможные варианты:
# 1. тип_события=событие
# 2. тип_события='событие' - здесь могут встречаться апострофы внутри события, они экранируются еще одним апострофом
# 3. тип_события="событие"
# Пример:
# 07:39.109005-1,SCALL,6,process=rphost,p:processName=ea_5,t:clientID=189,t:applicationName=BackgroundJob,t:computerName=SRV1C5-1,t:connectID=21996,SessionID=12565,Usr=DefUser,ClientID=137,Interface=2ebdaa8c-4a75-48f7-94bf-8206623aa9bb,IName=IClusterLogMngr,Method=0,CallID=41271,MName=writeLogEntryData,Context='
# ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 680 : ЗапланироватьЗадание(Выборка);
#         ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 1559 : НСтр("ru = ''Исполняющее задание было принудительно завершено''"));
#                 ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 1426 : + ?(ОбщегоНазначенияПовтИсп.ЭтоРазделеннаяКонфигурация() И ОбщегоНазначения.ЭтоРазделенныйОбъектМетаданных(ЗаписьЗадания.Метаданные().ПолноеИмя(),'
# 07:39.109008-1,DBMSSQL,7,process=rphost,p:processName=ea_5,t:clientID=189,t:applicationName=BackgroundJob,t:computerName=SRV1C5-1,t:connectID=21996,SessionID=12565,Usr=DefUser,Trans=1,dbpid=556,Sql='SELECT
# T1._Fld13275
# FROM dbo._Const13274 T1
# WHERE T1._RecordKey = ?
# p_0: 0x31
# ',Rows=1,RowsAffected=-1,Context='
# ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 680 : ЗапланироватьЗадание(Выборка);
#         ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 1562 : ПараметрыОбработчика = ПолучитьПараметрыОбработчикаОшибок(Задание);
#                 ОбщийМодуль.ОчередьЗаданийСлужебный.Модуль : 1273 : ИмяМетодаОбработчикаОшибки =
#                         ОбщийМодуль.ОчередьЗаданийСлужебныйПовтИсп.Модуль : 66 : ОбработчикиСобытия = ОбщегоНазначения.ОбработчикиСлужебногоСобытия(
#                                 ОбщийМодуль.ОбщегоНазначения.Модуль : 5110 : Возврат СтандартныеПодсистемыПовтИсп.ОбработчикиСерверногоСобытия(Событие, Истина);
#                                         ОбщийМодуль.СтандартныеПодсистемыПовтИсп.Модуль : 463 : ПодготовленныеОбработчики = ПодготовленныеОбработчикиСерверногоСобытия(Событие, Служебное);
#                                                 ОбщийМодуль.СтандартныеПодсистемыПовтИсп.Модуль : 645 : Параметры = СтандартныеПодсистемыПовтИсп.ПараметрыПрограммныхСобытий().ОбработчикиСобытий.НаСервере;
#                                                         ОбщийМодуль.СтандартныеПодсистемыПовтИсп.Модуль : 423 : СохраненныеПараметры = СтандартныеПодсистемыСервер.ПараметрыРаботыПрограммы(
#                                                                 ОбщийМодуль.СтандартныеПодсистемыСервер.Модуль : 1045 : Возврат СтандартныеПодсистемыПовтИсп.ПараметрыРаботыПрограммы(ИмяКонстанты);
#                                                                         ОбщийМодуль.СтандартныеПодсистемыПовтИсп.Модуль : 495 : Параметры = Константы[ИмяКонстанты].Получить().Получить();'
