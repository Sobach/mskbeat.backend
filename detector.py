#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

from datetime import datetime, timedelta
from pprint import pprint
from re import compile, sub, match, UNICODE, IGNORECASE
from itertools import groupby, combinations
from pickle import dumps as pdumps
from sys import stdout

# DB
from redis import StrictRedis
from PySQLPool import getNewPool, getNewConnection, getNewQuery
from MySQLdb import escape_string

# MATH
from numpy import array, mean, std, absolute
from networkx import Graph, connected_components, find_cliques
from math import radians, cos, sin, asin, sqrt
from sklearn.neighbors import KDTree
from sklearn.cluster import DBSCAN
from scipy.stats import entropy

# NLTK
from pymorphy2 import MorphAnalyzer
from nltk.tokenize import TreebankWordTokenizer

# CONSTANTS
from settings import *

class EventDetector():
	"""Event detector Object: used to discover events both online and offline."""

	def __init__(self, mysql_connection, bbox):
		"""
		Initialization.

		Args:
			redis_connection (StrictRedis): RedisDB connection Object
			mysql_connection (PySQLPoolConnection): MySQL connection Object
			bbox (List[float]): min longitude, min latitude, max longitude, max latitude
			nets (Dict[str, int]): accordance between one-letter network marks (in MySQL) and ints in numpy arrays
		"""
		self.bbox = bbox
		self.mysql = mysql_connection
		self.calcualte_eps_dbscan()

	def calcualte_eps_dbscan(self, max_dist = 0.2):
		"""
		Calculate maximum "distance" in coordinates for DBSCAN between points,
		given bounding box and maximum distance in km.
		Result:
			self.eps (float)
		"""
		lon1, lat1, lon2, lat2 = map(radians, self.bbox)
		dlon = lon2 - lon1 
		dlat = lat2 - lat1
		a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
		c = 2 * asin(sqrt(a)) 
		km = c * 6371
		dist = sqrt((self.bbox[0] - self.bbox[2])**2 + (self.bbox[1] - self.bbox[3])**2)
		self.eps = dist * max_dist / km

	def build_reference_trees(self, time, days=14, from_time=True):
		"""
		Create kNN-trees (KDTrees) for previous period - 1 day = 1 tree. 
		These trees are used as reference, when comparing with current kNN-distance.
		Result:
			self.trees (List[KDTree])
		"""
		lower_bound = time - timedelta(minutes = 30)
		upper_bound = time + timedelta(minutes = 30)
		if not from_time:
			d = exec_mysql('SELECT tstamp FROM tweets ORDER BY tstamp DESC LIMIT 1;', self.mysql)
			max_date = d[0][0]['tstamp']
		else:
			max_date = time
		min_date = (max_date - timedelta(days = days)).replace(hour = lower_bound.hour, minute = lower_bound.minute)
		if lower_bound.time() < upper_bound.time():
			q = '''SELECT DATE(tstamp), lat, lng, network FROM tweets WHERE tstamp >= '{}' AND tstamp <= '{}' AND TIME(tstamp) >= '{}' AND TIME(tstamp) <= '{}';'''.format(min_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%Y-%m-%d %H:%M:%S'), min_date.strftime('%H:%M:%S'), max_date.strftime('%H:%M:%S'))
			data, i = exec_mysql(q, self.mysql)
		else:
			q = '''SELECT DATE(tstamp), lat, lng, network FROM tweets WHERE tstamp >= '{}' AND tstamp <= '{}' AND TIME(tstamp) >= '{}' AND TIME(tstamp) <= '23:59:59';'''.format(min_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%Y-%m-%d %H:%M:%S'), min_date.strftime('%H:%M:%S'))
			data = exec_mysql(q, self.mysql)[0]
			q = '''SELECT DATE_ADD(DATE(tstamp),INTERVAL -1 DAY) AS 'DATE(tstamp)', lat, lng, network FROM tweets WHERE tstamp >= '{}' AND tstamp <= '{}' AND TIME(tstamp) >= '00:00:00' AND TIME(tstamp) <= '{}';'''.format(min_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%H:%M:%S'))
			data += exec_mysql(q, self.mysql)[0]
		preproc = {}
		for item in data:
			try: 
				preproc[item['DATE(tstamp)']].append([item['lng'], item['lat']])
			except KeyError: 
				preproc[item['DATE(tstamp)']] = [[item['lng'], item['lat']]]
		self.reference_trees = []
		for element in preproc.values():
			self.reference_trees.append(KDTree(array(element)))

	def build_tree_from_mysql(self, start_date = None):
		if not start_date:
			start_date = datetime.now()
		q = '''SELECT lat, lng, network FROM tweets WHERE tstamp >= '{}' AND tstamp <= '{}';'''.format(
			start_date, start_date+timedelta(hours=1))
		data = exec_mysql(q, self.mysql)[0]
		self.tree = KDTree(array([[x['lng'], x['lat']] for x in data]))

	def get_outliers_tweets(self, datapoints):
		cur_knn_data = mean(self.tree.query(array([[x['lng'], x['lat']] for x in datapoints]), k=5, return_distance=True)[0], axis=1)
		ref_knn_data = mean(array([x.query(array([[x['lng'], x['lat']] for x in datapoints]), k=5, return_distance=True)[0] for x in self.reference_trees]), axis=2)
		thr_knn_mean = mean(ref_knn_data, axis=0)
		thr_knn_std = std(ref_knn_data, axis=0)
		thr_knn_data =  thr_knn_mean - thr_knn_std  * 3
		points = list(array(datapoints)[cur_knn_data < thr_knn_data])
		weights = (absolute(cur_knn_data - thr_knn_mean)/thr_knn_std)[cur_knn_data < thr_knn_data]
		for i in range(len(weights)):
			points[i]['weight'] = weights[i]
		return points

	def dbscan_tweets(self, points):
		clustering = DBSCAN(eps=self.eps, min_samples=5)
		labels = clustering.fit_predict(array([[x['lng'], x['lat']] for x in points]))
		print labels
		ret_tab = [dict(points[i].items() + {'cluster':labels[i], 'is_core': i in clustering.core_sample_indices_}.items()) for i in range(len(labels)) if labels[i] > -1]
		ret_tab = { x[0]:tuple(x[1]) for x in groupby(sorted(ret_tab, key=lambda x:x['cluster']), lambda x: x['cluster']) }
		return ret_tab

class Event():
	"""
	Attributes:
		self.messages (Dict): raw tweets from database, enriched with weight, is_core params (on init), tokens (after add_stem_texts)
		self.media (Dict): raw media objects from database
		self.vocabulary (Set): tokens, that form the largest connected component in tokens graph
		self.core (Set): tokens, that form the largest clique (maximal connected component) in tokens graph
		self.entropy (float): entropy for authorship: 0 for mono-authored cluster; computed in event_summary_stats()
		self.ppa (float): average number of posts per one author; computed in event_summary_stats()

	Message keys:
		cluster (int): legacy from DBSCAN - number of cluster, event ancestor
		id (str): DB message id; unique
		is_core (bool): True, if tweet belongs to the core of ancestor cluster
		iscopy (int): 1, if message is shared from another network
		lat (float): latitude
		lng (float): longitude
		network (str): 'i' for Instagram, 't' for Twitter, 'v' for VKontakte
		text (str): raw text of the message
		tokens (Set[str]): collection of stemmed tokens from raw text; created in add_stem_texts()
		tstamp (datetime): 'created at' timestamp
		user (int): user id, absolutely unique for one network, but matches between networks are possible
		tokens_score (float): agreement estimation with average cluster text [0:2]
		weight (float): legacy from density outliers detector - how many standart deviations between current and reference distances to kNN
	"""

	def __init__(self, mysql_connection, points):
		self.mysql = mysql_connection
		self.morph = MorphAnalyzer()
		self.tokenizer = TreebankWordTokenizer()
		self.word = compile(r'^\w+$', flags = UNICODE | IGNORECASE)
		self.url_re = compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')
		
		self.messages = { x['id']:x for x in points }
		self.get_messages_data()
		self.add_stem_texts()

		self.build_tokens_graph()
		self.score_messages_by_text()
		self.event_summary_stats()

		self.media = {}
		self.get_media_data()

	def get_messages_data(self):
		q = '''SELECT * FROM tweets WHERE id in ({});'''.format(','.join(['"'+str(x['id'])+'"' for x in self.messages.values()]))
		data = exec_mysql(q, self.mysql)[0]
		for item in data:
			self.messages[item['id']].update(item)

	def get_media_data(self):
		q = '''SELECT * FROM media WHERE tweet_id in ({});'''.format(','.join(['"'+str(x['id'])+'"' for x in self.messages.values()]))
		data = exec_mysql(q, self.mysql)[0]
		for item in data:
			self.media[item['id']] = item

	def event_summary_stats(self):
		authorsip_stats = [len(tuple(i[1])) for i in groupby(sorted(self.messages.values(), key=lambda x:x['user']), lambda z: z['user'])]
		self.entropy = entropy(authorsip_stats)
		self.ppa = mean(authorsip_stats)

	def add_stem_texts(self):
		for i in self.messages.keys():
			txt = self.messages[i]['text']
			txt = sub(self.url_re, '', txt)
			print txt
			self.messages[i]['tokens'] = set([self.morph.parse(token.decode('utf-8'))[0].normal_form for token in self.tokenizer.tokenize(txt) if match(self.word, token.decode('utf-8'))])

	def build_tokens_graph(self, threshold=0):
		G = Graph()
		edges = {}
		for item in self.messages.values():
			tokens = set([x for x in item['tokens'] if len(x) > 2])
			if len(tokens) < 2:
				continue
			for pair in combinations(tokens, 2):
				try:
					edges[tuple(sorted(pair))] += 1
				except KeyError:
					edges[tuple(sorted(pair))] = 1
		edges = {k:v for k,v in edges.items() if v > threshold}
		G.add_edges_from(edges.keys())
		try:
			self.vocabulary = set(sorted(connected_components(G), key = len, reverse=True)[0])
		except IndexError:
			self.vocabulary = set([])
		try:
			self.core = set(sorted(find_cliques(G), key = len, reverse=True)[0])
		except IndexError:
			self.core = set([])

	def score_messages_by_text(self):
		for msg_id in self.messages:
			try:
				vocab_score = float(len(self.vocabulary.intersection(self.messages[msg_id]['tokens'])))/\
				len(self.vocabulary.union(self.messages[msg_id]['tokens']))
			except ZeroDivisionError:
				vocab_score = 0
			try:
				core_score = float(len(self.core.intersection(self.messages[msg_id]['tokens'])))/\
				len(self.core.union(self.messages[msg_id]['tokens']))
			except ZeroDivisionError:
				core_score = 0
			self.messages[msg_id]['tokens_score'] = vocab_score + core_score

class CollectorEmulator():
	"""
	Emulator for real-time messages Collector (currently implemented in backend.py)
	Goal: online clustering methods testing.
	Fast-forward ratio - the more ratio, the faster messages arrive (compared to IRL)
	Start timeout - step several seconds on initialisation
	When data ends, puts into queue special tweet with id=0 and text='TheEnd'.
	"""

	def __init__(self, mysql_con, redis_con, dataset=None, fast_forward_ratio = 1, start_timeout = 10, run_on_init = True):
		"""
		dataset (List[Dict]): tweets database dump, collected using PySQLPool. Available fields:
			id, text, lat, lng, tstamp, user, network, iscopy
		"""
		self.redis = redis_con
		self.mysql = mysql_con

		# Loading default dataset
		if not dataset:
			q = '''SELECT * FROM tweets_origins WHERE tstamp >= '2015-07-01 17:00:00' AND tstamp <= '2015-07-01 17:01:00';'''
			dataset = exec_mysql(q, self.mysql)[0]

		# Recalculating publish timestamp for messages, according to current time
		self.fast_forward_ratio = fast_forward_ratio
		self.raw_data = sorted(list(dataset), key=lambda x: x['tstamp'], reverse=False)
		self.old_init = self.raw_data[0]['tstamp']
		self.new_init = datetime.now() + timedelta(seconds = start_timeout)
		for i in range(len(self.raw_data)):
			seconds2add = (self.raw_data[i]['tstamp'] - self.old_init).total_seconds()/fast_forward_ratio
			self.raw_data[i]['tstamp'] = self.new_init + timedelta(seconds = seconds2add)
	
		# These vars are required for logging and writing status updates
		self.new_end = self.raw_data[-1]['tstamp']
		self.duration = (self.new_end - self.new_init).total_seconds()
		self.total_msgs = len(self.raw_data)
		self.i = 0
		self.rotator = ('\\', '|', '/', '-')
		self.previous_out = ''

		# Starting emulation (turned on by default)
		if run_on_init:
			self.run()

	def run(self):
		"""
		Loop: Poping and publishing messages with tstamp <= current
		Runs until raw_data list is empty, then pushes "final" message
		"""
		while True:
			try:
				if self.raw_data[0]['tstamp'] <= datetime.now():
					self.push_msg(self.raw_data.pop(0))
			except IndexError:
				self.push_msg({'id':0, 'text':'TheEnd', 'lat':0, 'lng':0, 'tstamp':datetime.now(), 'user':0, 'network':'u', 'iscopy':0})
				break

	def push_msg(self, message):
		"""
		Function for processing every single meaasge as if it is real Collector.
		Currently message is:
		(1) being sent to redis with lifetime for 1 hour (from settings - TIME_SLIDING_WINDOW)
		(2) being dumped in MySQL datatable
		"""
		self.redis.set("message:{}".format(message['id']), pdumps(message))
		self.redis.expire("message:{}".format(message['id']), TIME_SLIDING_WINDOW/self.fast_forward_ratio)
		message['text'] = escape_string(message['text'])
		q = 'INSERT IGNORE INTO tweets(id, text, lat, lng, tstamp, user, network, iscopy) VALUES ("{id}", "{text}", {lat}, {lng}, "{tstamp}", {user}, "{network}", {iscopy});'.format(**message)
		exec_mysql(q, self.mysql)
		self.log_process(message)

	def log_process(self, message):
		"""
		Logging the whole process: using stdout, all logging is being made in one line. 
		Percentage is computed from start time to end time, also current msg # is shown.
		"""
		percent = '{}%'.format(round(100*float((message['tstamp'] - self.new_init).total_seconds())/self.duration, 2))
		stdout.write('\r'.rjust(len(self.previous_out), ' '))
		self.previous_out = '\rEmulating: ('+self.rotator[self.i%4]+') Complete: ' + percent.ljust(7, ' ') + 'Messages: {}/{}'.format(self.i, self.total_msgs)
		stdout.flush()
		stdout.write(self.previous_out)
		self.i+=1
		if message['id'] == 0:
			stdout.write('\n')

def exec_mysql(cmd, connection):
	query = getNewQuery(connection, commitOnEnd=True)
	result = query.Query(cmd)
	return query.record, result

if __name__ == '__main__':
	redis_db = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
	getNewPool().maxActiveConnections = 1
	mysql_db = getNewConnection(username=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, db=MYSQL_DB)
	exec_mysql('SET NAMES utf8mb4;', mysql_db)
	exec_mysql('SET CHARACTER SET utf8mb4;', mysql_db)
	exec_mysql('SET character_set_connection=utf8mb4;', mysql_db)

	emulator = CollectorEmulator(mysql_db, redis_db, fast_forward_ratio=2, start_timeout=0)

	"""
	ed = EventDetector(mysql_db, BBOX)
	ed.build_reference_trees(time = datetime.now(), from_time=False)
	ed.build_tree_from_mysql(start_date = datetime(2015, 07, 01, 17, 0, 0))
	q = '''SELECT id, lat, lng, network FROM tweets WHERE tstamp >= '2015-07-01 17:00:00' AND tstamp <= '2015-07-01 18:00:00';'''
	data = exec_mysql(q, mysql_db)[0]
	points = ed.get_outliers_tweets(data)
	cluster_candidates = ed.dbscan_tweets(points)
	print datetime.now()
	e = Event(mysql_db, cluster_candidates[0])
	print e.vocabulary, e.core, e.entropy, e.ppa
	pprint(e.messages)
	"""