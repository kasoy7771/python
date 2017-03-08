# -*- coding: utf-8 -*-
#!/usr/bin/env python

import psycopg2
import time
import argparse
import os, re

def parse_params():
	parser = argparse.ArgumentParser()
	parser.add_argument('-b', '--base', type=str, help = 'base name (ea_35)')
	parser.add_argument('-s', '--server', type=str, help="db server name (srv1c35-1)")
	parser.add_argument('-p', '--password', type=str, help="password of dba")
	parser.add_argument('-u', '--user', type=str, help="dba user. dba_<base_name> by default", default='')
	parser.add_argument('-c', '--count', type=int, help="count of query loops", default=1000)
	parser.add_argument('-d', '--daemon_mode', action='store_true', help="daemon mode", default=False)
	parser.add_argument('-t', '--timeout', type=int, help="timeout for daemon mode, default=60", default=60)
	parser.add_argument('-l', '--log_file', type=str, help="path of log file, default='/tmp/qloop.log'", default='/tmp/qloop.log')
	parser.add_argument('-v', '--verbose', action='store_true', help="verbose", default=False)
	parser.add_argument('-f', '--full_mode', action='store_true', help="mode for cheking all pg bases", default=False)
	params = parser.parse_args()
	if not params.user and params.base:
		params.user = 'dba_' + params.base 
	return params

def get_cur_date():
	return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())

def log(message = ''):
	logf = open(params.log_file, 'a')
	if params.verbose:
		print message
	logf.write(message)
	logf.close()

def get_bases_passwords():
	bases = {}

	result = {}
	for base in bases.keys():
		cur_base = bases[base]
		if cur_base['host'].find(u'srvdb') > -1 and cur_base.has_key('number'):
			# print bases[base]
			result[cur_base['number']] = {'base' : base, 'host': cur_base['host'], 'username': cur_base['username'], 'password': cur_base['password']}
	return result

def make_measurement(params, base, user, password, server):
	conn = psycopg2.connect("dbname={} user={} password={} host={}".format(base, user, password, server))
	cur = conn.cursor()
	begin = time.time()
	for i in range(1,params.count):
		cur.execute("""SELECT 1""")
	end = time.time()
	cur.close()
	conn.close()
	return end - begin

def make_measurement_full_check(params, db_servers):
	for host_number in db_servers.keys():
		if host_number in (u'14', 'demo', '49', '46', '47', '42'): 
			continue
		#Для проверки плана счетов	
		if db_servers[host_number]['base'][0:2] != 'ea':
			continue
		cur_host = db_servers[host_number]
		password = cur_host['password']
		user = cur_host['username']
		base = cur_host['base']
		server = cur_host['host']
		duration = make_measurement(params, base, user, password, server)
		result = cur_host['host'] + " " + get_cur_date() + " duraton " + str(duration)
		log(result)
	log("\n\n")

def daemonize(params, db_servers = None):
	while True:
		pid = os.fork()
		if pid:
			time.sleep(params.timeout)
		else:
			if not params.full_mode:
				# Делаем обычную проверку
				duration = make_measurement(params, params.base, params.user, params.password, params.server)
				result = get_cur_date() + " duraton " + str(duration)
				log(result)
				exit(0)
			else:
				# Делаем проверку по всем базам
				make_measurement_full_check(params, db_servers)
				exit(0)


def full_checking(params, db_servers):
	if params.daemon_mode:
		daemonize(params, db_servers)
	else:
		make_measurement_full_check(params, db_servers)

if __name__ == "__main__":
	params = parse_params()
	if not params.full_mode:
		# Проверка в обычном режиме
		if not params.daemon_mode:
			duration = make_measurement(params, params.base, params.user, params.password, params.server)
			print get_cur_date() + " duraton " + str(duration)
		else:
			daemonize(params)
	else:
		db_servers = get_bases_passwords()
		full_checking(params, db_servers)	

	


# yum install python-psycopg2 -y

# * * * * * python /tmp/qloop.py -b ea_23 -s srvdb23-1 -p 1111asdf -c 1000 >> /tmp/qloop.log
# * * * * * python /tmp/qloop.py -b ea_35 -s srvdb35-1 -p 1sdfa7w3 -c 1000 >> /tmp/qloop.log
