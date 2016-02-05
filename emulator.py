#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# SYSTEM
from datetime import datetime, timedelta
from time import mktime
from sys import stdout

# DATABASE
from MySQLdb import escape_string
from redis.exceptions import ResponseError

# SELF IMPORT
from settings import TIME_SLIDING_WINDOW
from utilities import exec_mysql

class CollectorEmulator():
	"""
	Emulator for real-time messages Collector (currently implemented in backend.py)
	Goal: online clustering methods testing.
	Fast-forward ratio - the more ratio, the faster messages arrive (compared to IRL)
	Start timeout - step several seconds on initialisation
	When data ends, puts into queue special tweet with id=0 and text='TheEnd'.
	"""

	def __init__(self, mysql_con, redis_con, dataset=None, fast_forward_ratio = 1, start_timeout = 10, run_on_init = True, truncate_on_init = True):
		"""
		dataset (List[Dict]): tweets database dump, collected using PySQLPool. Available fields:
			id, text, lat, lng, tstamp, user, network, iscopy
		"""
		self.redis = redis_con
		self.mysql = mysql_con

		# Loading default dataset
		if not dataset:
			if truncate_on_init and not run_on_init:
				q = '''SELECT * FROM tweets_origins LIMIT 1;'''
			else:
				q = '''SELECT * FROM tweets_origins WHERE tstamp >= '2015-07-01 12:00:00' AND tstamp <= '2015-07-02 12:00:00';'''
			dataset = exec_mysql(q, self.mysql)[0]

		# Recalculating publish timestamp for messages, according to current time
		self.fast_forward_ratio = fast_forward_ratio
		self.raw_data = sorted(list(dataset), key=lambda x: x['tstamp'], reverse=False)
		self.old_init = self.raw_data[0]['tstamp']
		self.new_init = datetime.now() + timedelta(seconds = start_timeout)
		for i in range(len(self.raw_data)):
			seconds2add = (self.raw_data[i]['tstamp'] - self.old_init).total_seconds()/fast_forward_ratio
			self.raw_data[i]['pub_tstamp'] = self.new_init + timedelta(seconds = seconds2add)
	
		# These vars are required for logging and writing status updates
		self.new_end = self.raw_data[-1]['pub_tstamp']
		self.duration = (self.new_end - self.new_init).total_seconds()
		self.total_msgs = len(self.raw_data)
		self.i = 0
		self.rotator = ('\\', '|', '/', '-')
		self.previous_out = ''

		# Starting emulation (turned on by default)
		if truncate_on_init:
			self.truncate_db()
		if run_on_init:
			self.run()

	def truncate_db(self):
		"""
		Clear database before emulation start. 
		Truncate tweets, events, and event_msgs db's in MySQL;
		Delete all "message:*" and "event:*" hashes from Redis.
		"""
		exec_mysql('TRUNCATE event_msgs;', self.mysql);
		exec_mysql('TRUNCATE events;', self.mysql);
		exec_mysql('TRUNCATE tweets;', self.mysql);
		try:
			self.redis.delete(*self.redis.keys('message:*'))
		except ResponseError:
			pass
		try:
			self.redis.delete(*self.redis.keys('event:*'))
		except ResponseError:
			pass
		try:
			self.redis.delete(*self.redis.keys('dumped:*'))
		except ResponseError:
			pass

	def run(self):
		"""
		Loop: Poping and publishing messages with tstamp <= current
		Runs until raw_data list is empty, then pushes "final" message
		"""
		while True:
			try:
				if self.raw_data[0]['pub_tstamp'] <= datetime.now():
					msg = self.raw_data.pop(0)
					self.push_msg(msg)
			except IndexError:
				message = {'id':0, 'lat':0, 'lng':0, 'tstamp':datetime.now(), 'network':0}
				self.redis.hmset("message:{}".format(message['id']), message)
				break

	def push_msg(self, message):
		"""
		Function for processing every single meaasge as if it is real Collector.
		Currently message is:
		(1) being sent to redis with lifetime for 1 hour (from settings - TIME_SLIDING_WINDOW)
		(2) being dumped in MySQL datatable
		"""
		redis_message = {
			'id':message['id'], 
			'lat':message['lat'], 
			'lng': message['lng'], 
			'tstamp': int(mktime(message['tstamp'].timetuple())), 
			'network': message['network']
		}
		self.redis.hmset("message:{}".format(message['id']), redis_message)
		self.redis.expire("message:{}".format(message['id']), int(TIME_SLIDING_WINDOW/self.fast_forward_ratio))
		message['text'] = escape_string(message['text'])
		q = 'INSERT IGNORE INTO tweets(id, text, lat, lng, tstamp, user, network, iscopy) VALUES ("{id}", "{text}", {lat}, {lng}, "{tstamp}", {user}, "{network}", {iscopy});'.format(**message)
		exec_mysql(q, self.mysql)
		self.log_process(message)

	def log_process(self, message):
		"""
		Logging the whole process: using stdout, all logging is being made in one line. 
		Percentage is computed from start time to end time, also current msg # is shown.
		"""
		percent = '{}%'.format(round(100*float((message['pub_tstamp'] - self.new_init).total_seconds())/self.duration, 2))
		stdout.write('\r'.rjust(len(self.previous_out), ' '))
		self.previous_out = '\rEmulating: ('+self.rotator[self.i%4]+') Complete: ' + percent.ljust(7, ' ') + 'Messages: {}/{}'.format(self.i, self.total_msgs)
		stdout.flush()
		stdout.write(self.previous_out)
		self.i+=1
		if message['id'] == 0:
			stdout.write('\n\n')

if __name__ == "__main__":
	from argparse import ArgumentParser
	parser = ArgumentParser(description="City.Pulse Collector Emulator")
	parser.add_argument("-ff", "--fastforward", help="speed ratio to speed up emulation", type=float)
	parser.add_argument("-p", "--period", help="time interval to use in emulation", choices=['hour', 'day', 'week', 'month'])
	parser.add_argument("-t", "--timeout", help="seconds to pause before starting emulation", type=int)
	parser.add_argument("action", help="what should emulator do", choices=['run', 'truncate', 'runclean'])
	args = parser.parse_args()
	if not args.fastforward:
		args.fastforward = 1
	if not args.timeout:
		args.timeout = 0
	if args.action == 'truncate':
		q = '''SELECT * FROM tweets_origins LIMIT 1;'''
	elif not args.period:
		q = '''SELECT * FROM tweets_origins;'''
	elif args.period == "hour":
		q = '''SELECT * FROM tweets_origins WHERE tstamp >= '2015-06-22 19:00:00' AND tstamp <= '2015-06-23 20:00:00';'''
	elif args.period == "day":
		q = '''SELECT * FROM tweets_origins WHERE tstamp >= '2015-06-22 00:00:00' AND tstamp <= '2015-06-23 00:00:00';'''
	elif args.period == "week":
		q = '''SELECT * FROM tweets_origins WHERE tstamp >= '2015-06-29 00:00:00' AND tstamp <= '2015-07-06 00:00:00';'''
	elif args.period == "month":
		q = '''SELECT * FROM tweets_origins WHERE tstamp >= '2015-06-21 00:00:00' AND tstamp <= '2015-07-18 00:00:00';'''
	if args.action:
		from settings import REDIS_HOST, REDIS_PORT, REDIS_DB
		from redis import StrictRedis
		from utilities import get_mysql_con
		redis_db = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
		mysql_db = get_mysql_con()
		dataset = exec_mysql(q, mysql_db)[0]
		if args.action == 'run':
			emulator = CollectorEmulator(mysql_db, redis_db, dataset=dataset, fast_forward_ratio=args.fastforward, start_timeout=args.timeout, run_on_init = True, truncate_on_init = False)
		elif args.action == 'runclean':
			emulator = CollectorEmulator(mysql_db, redis_db, dataset=dataset, fast_forward_ratio=args.fastforward, start_timeout=args.timeout, run_on_init = True, truncate_on_init = True)
		elif args.action == 'truncate':
			emulator = CollectorEmulator(mysql_db, redis_db, dataset=dataset, fast_forward_ratio=args.fastforward, start_timeout=args.timeout, run_on_init = False, truncate_on_init = True)

