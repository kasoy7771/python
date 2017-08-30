 # -*- coding: utf-8 -*-

from glob import glob
import os.path
from datetime import datetime
import re
import parser

class LazyReader(object):
	
	def __init__(self, file_name):
		super(LazyReader, self).__init__()
		self.file = open(file_name)
		self.data = ''
		self.data_len = 0
		self.pos = 0
		self.is_end = False
		self.read_chunk()


	def read_chunk(self,size = 1024):
		self.pos = 0
		self.data = self.file.read(size)
		self.data_len = len(self.data)
		if not self.data:
			self.is_end = True

	def get_ch(self):
		if self.is_end:
			return None
		rez = self.data[self.pos]
		self.pos += 1
		if self.pos == self.data_len:
			self.read_chunk()
		return rez

	def look_up(self):
		if self.pos == len(self.data):
			self.is_end = True
			return None
		return self.data[self.pos]

	def drop_to_symbol(self, symbol):
		while not self.is_end:
			char = self.get_ch()
			if char == symbol:
				return
		
		
class EventReader():

	def __init__(self, event):
		self.pos = 0
		self.event = event
		self.is_end = False
		self.true_event_len = len(self.event)
		self.drop_to_symbol('{')
		
	def get_ch(self):
		if self.is_end:
			return None
		if self.pos < self.event_len:
			rez = self.event[self.pos]
			self.pos += 1
			return rez	
	
	def look_up(self):
		if self.pos == self.event_len:
			self.is_end = True
			return None
		return self.event[self.pos]
		
	def drop_to_symbol(self, symbol):
		garb, self.event = re.split('{', self.event, 1)
		self.event_len = len(self.event)
		
		
def append_if_not_empty(collection,string):
	if len(string)==0:
		return
	if not string.strip():
		return
	collection.append(string)


def read_str(reader):
	string_buffer = ''
	while True:
		char = reader.get_ch()
		if char == None:
			raise EOFError("Unexpected end of file")
		elif char=='"':
			if reader.look_up()=='"':
				string_buffer += '""'
				reader.get_ch()
			else:
				if not string_buffer:
					return "0"
				else:
					return string_buffer
		else:
			string_buffer += char


def read_entry(reader):
	result = []
	string_buffer = ''
	while True:
		char = reader.get_ch()
		if char == None:
			raise EOFError("Unexpected end of file")
			#append_if_not_empty(result,string_buffer)
			#return result
		elif char == ',':
			append_if_not_empty(result,string_buffer)
			string_buffer = ''
		elif char.isspace():
			pass
		elif char == '{':
			append_if_not_empty(result,string_buffer)
			string_buffer = ''
			result.append(read_entry(reader))
		elif char == '}':
			append_if_not_empty(result,string_buffer)
			return result
		elif char == '"':
			append_if_not_empty(result,string_buffer)
			string_buffer = ''
			append_if_not_empty(result,read_str(reader))
		else:
			string_buffer += char
		
		
def read_entry2(reader):
	result = []
	string_buffer = ''
	while True:
		char = reader.get_ch()
		if char == None:
			raise EOFError("Unexpected end of file")
			#append_if_not_empty(result,string_buffer)
			#return result
		elif char == ',':
			append_if_not_empty(result,string_buffer)
			string_buffer = ''
		elif char.isspace():
			pass
		elif char == '{':
			append_if_not_empty(result,string_buffer)
			string_buffer = ''
			result.append(read_entry2(reader))
		elif char == '}':
			append_if_not_empty(result,string_buffer)
			return result
		elif char == '"':
			append_if_not_empty(result,string_buffer)
			string_buffer = ''
			append_if_not_empty(result,read_str(reader))
		else:
			string_buffer += char


def read_file(file_name):
	reader = LazyReader(file_name)
	#rez = []
	while True:
		reader.drop_to_symbol('{')
		if reader.is_end:
			return
		#rez.append(read_entry(reader))
		yield read_entry(reader)
	
	
def read_events_from_file(file_name):
	'''
	Функция из переданных глоб создает генератор цельных
	событий журнала регистрации платформы 1С:Предприятие 8.3
	в строковом виде
	'''

	# Комплируем регулярку для определения начала события
	new_line_regex = re.compile(r'^\{\d{14},[NCUR],')
	first = True
	event = ''
				
	fd = open(file_name, 'rb')
	# Проходим по каждой строке из потока, чтобы сформировать из них цельные события
	for line in fd.xreadlines():
		# Если совпали с регуляркой, то собыите закончилось, можно отдавать
		if new_line_regex.search(line[:30]):
			# Но только не для первой строки
			if first:
				# Особый случай для первой строки, она не является разделителем событий, надо пропустить передачу события
				first = False
				# Надо избавиться от BOM байта
				if not line[0] == '{':
					garb, line = re.split('\\{', line, 1)
				event += line
				continue
				
			# Возвращаем событие
			er = EventReader(event)
			entry = read_entry2(er)
			yield (entry, er.true_event_len)

			# Обнуляем переменную с событием, так как началось новое
			event = ''
		# Прибавляем строку, чтобы софрмировать цельное событие
		event += line
		
	# Возвращаем последнее событие
	er = EventReader(event)
	entry = read_entry2(er)
	yield (entry, er.true_event_len)


def hist(list_par):
	rez = {}
	for el in list_par:
		rez[el] = rez.get(el,0)+1
	return rez


class Dictionary(object):
	"""docstring for Dictionary"""
	def __init__(self, file_name):
		super(Dictionary, self).__init__()
		self.users = {}
		self.apps = {}
		self.events = {}
		self.seps = {}

		#for entry in read_file(LazyReader(file_name)):
		for entry in read_file(file_name):
			if entry[0] == '1': #пользователь
				if len(entry) < 4:
					self.users[entry[2]] = ""
				else:	
					self.users[entry[3]] = entry[2].decode('utf')
			elif entry[0] == '3': #приложение
				self.apps[entry[2]] = entry[1].decode('utf')
			elif entry[0] == '4': #событие
				self.events[entry[2]] = entry[1].decode('utf')
			elif entry[0] == '10' and entry[2] == '2': #разделитель
				self.seps[entry[3]] = entry[1][1]
			else:
				pass

class Event(object):
	def __init__(self):
		pass

	def pprint(self):
		rez = []
		rez.append("Time="+self.time.strftime("%Y-%m-%d %H:%M:%S"))
		rez.append("User="+self.user)
		rez.append("AppId="+self.applicationId)
		rez.append("ConnKind="+self.connection_kind)
		rez.append("Conn="+self.connectionId)
		rez.append("Kind="+self.kind)
		rez.append("Event="+self.event)
		rez.append("Session="+self.sessionId)
		rez.append("Bytes="+self.bytes)
		comment = self.comment.replace("\r\n","|")
		comment = comment.replace("\n","|")
		rez.append("Comment="+comment)
		print ",".join(rez).encode("utf-8")

		
def file_parse(dic, file_name):
	for entry, event_len in read_events_from_file(file_name):
		#if entry[8] != "E":
		#	continue
		#print entry
		event = Event()
		event.time = datetime.strptime(entry[0], '%Y%m%d%H%M%S')
		event.user = dic.users.get(entry[3], '')
		event.connection_kind = dic.apps.get(entry[5], '')
		event.connectionId = entry[6]
		event.event = dic.events.get(entry[7], '')
		event.kind = entry[8]
		event.comment = entry[9].decode('utf')
		event.sessionId = entry[-3]
		event.applicationId = dic.seps.get(entry[-1][-1], '0')
		event.bytes = event_len
		yield event

		
def analyze_event_applicationId(event):
	global sum_bytes
	global sum_count
	if not result.has_key(event.applicationId):
		result[event.applicationId] = {'bytes': 0, 'count': 0}
	
	result[event.applicationId]['bytes'] += event.bytes
	result[event.applicationId]['count'] += 1
	
	sum_bytes += event.bytes
	sum_count += 1
	
def analyze_event_eventtype(event):
	global sum_bytes
	global sum_count
	if not result.has_key(event.event):
		result[event.event] = {'bytes': 0, 'count': 0}
	
	result[event.event]['bytes'] += event.bytes
	result[event.event]['count'] += 1
	
	sum_bytes += event.bytes
	sum_count += 1
		
		
def print_result():
	global sum_bytes
	global sum_count
	for field in sorted(result.keys(), key=lambda reg: result[reg]['bytes'], reverse=True):
		print ('-' * 50)
		print ('-' * 50)
		print field
		print 'bytes = ' + str(result[field]['bytes']) + str(result[field]['bytes']/sum_bytes) + '%'
		print 'count = ' + str(result[field]['count']) + str(result[field]['count']/sum_count) + '%'
		
	print ('-' * 50)
	print ('-' * 50)
	print 'summary bytes = ' + str(sum_bytes)
	print 'summary count = ' + str(sum_count)
		
		
	#pprint.pprint(types)
		
		
def show_events(dic_file, files):
	dic = Dictionary(dic_file)
	for file_name in files:
		for event in file_parse(dic, file_name):
			#if event.kind != "E":
			#	continue
			event.pprint()
			
			
def process_events(dic_file, files):
	dic = Dictionary(dic_file)
	for file_name in files:
		for event in file_parse(dic, file_name):
			#if event.kind != "E":
			#	continue
			#analyze_event_applicationId(event)
			analyze_event_eventtype(event)


if __name__ == '__main__':
	import sys
	#show_events(sys.argv[1], sys.argv[2:])
	result = {}
	sum_bytes = 0
	sum_count = 0
	process_events(sys.argv[1], sys.argv[2:])
	print_result()
