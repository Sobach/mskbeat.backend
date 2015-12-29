#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# SYSTEM
from datetime import datetime, timedelta
from pprint import pprint
from re import compile, sub, match, UNICODE, IGNORECASE
from itertools import groupby, combinations
from pickle import dumps as pdumps, loads as ploads
from sys import stdout
from time import sleep
from json import dumps as jdumps
from uuid import uuid4

# DATABASE
from redis import StrictRedis
from PySQLPool import getNewPool, getNewConnection
from MySQLdb import escape_string

# MATH
from numpy import array, mean, std, absolute
from networkx import Graph, connected_components, find_cliques
from sklearn.neighbors import KDTree
from sklearn.cluster import DBSCAN
from scipy.stats import entropy

# SYSTEM MATH
from math import radians, cos, sin, asin, sqrt

# NLTK
from pymorphy2 import MorphAnalyzer
from nltk.tokenize import TreebankWordTokenizer

# CONSTANTS
from settings import *
from utilities import exec_mysql

class EventDetector():
	"""
	Event detector Object: used to discover events both online and offline.
	When self.run() method starts, initializes infinity loop, where every time
	takes messages from redis db, filters and clusters them, and merges with
	clusters from previous steps.
	"""

	def __init__(self, mysql_con, redis_con, bbox, fast_forward_ratio=1.0):
		"""
		Initialization.

		Args:
			redis_con (StrictRedis): RedisDB connection Object
			mysql_con (PySQLPoolConnection): MySQL connection Object
			bbox (List[float]): min longitude, min latitude, max longitude, max latitude
			fast_forward_ratio (float): parameter for emulation - the greater param - the faster emulation and
		"""
		self.bbox = bbox
		self.mysql = mysql_con
		self.redis = redis_con
		self.calcualte_eps_dbscan()
		self.interrupter = False

	def run(self):
		while True:
			self.build_reference_trees(datetime.now(), take_origins = True)
			self.build_current_tree()
			points = self.get_current_outliers()
			slice_clusters = self.dbscan_tweets(points)
			self.get_previous_events()
			self.merge_slices_to_events(slice_clusters)
			self.dump_current_events()
			if self.interrupter:
				for event in self.current_events:
					event.backup()
				break
			sleep(3)

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

	def build_reference_trees(self, time, days = 14, take_origins = False):
		"""
		Create kNN-trees (KDTrees) for previous period - 1 day = 1 tree. 
		These trees are used as reference, when comparing with current kNN-distance.
		Args:
			time (datetime): timestamp for data of interest. 75% of data is taken from the past, and 25% - from the future.
			days (int): how many days should be used for reference (by default 2 weeks)
			take_origins (bool): whether to use actual dynamic data (default), or training dataset

		Result:
			self.trees (List[KDTree])
		"""
		data = self.get_reference_data(time, days, take_origins)
		preproc = {}
		for item in data:
			try: 
				preproc[item['DATE(tstamp)']].append([item['lng'], item['lat']])
			except KeyError: 
				preproc[item['DATE(tstamp)']] = [[item['lng'], item['lat']]]
		self.reference_trees = []
		for element in preproc.values():
			self.reference_trees.append(KDTree(array(element)))

	def get_reference_data(self, time, days = 14, take_origins = False):
		"""
		Load historical data from MySQL.
		If take_origins, use constant tweets from tweets_origins table,
		otherwise - use dynamic data from tweets table.
		Returns MySQL dict
		Args:
			time (datetime): timestamp for data of interest. 75% of data is taken from the past, and 25% - from the future.
			days (int): how many days should be used for reference (by default 2 weeks)
			take_origins (bool): whether to use actual dynamic data (default), or training dataset
		"""
		lower_bound = time - timedelta(seconds = TIME_SLIDING_WINDOW * 0.75)
		upper_bound = time + timedelta(seconds = TIME_SLIDING_WINDOW * 0.25)
		if take_origins:
			database = 'tweets_origins'
			d = exec_mysql('SELECT tstamp FROM tweets_origins ORDER BY tstamp DESC LIMIT 1;', self.mysql)
			max_date = d[0][0]['tstamp']
		else:
			database = 'tweets'
			max_date = time
		min_date = (max_date - timedelta(days = days)).replace(hour = lower_bound.hour, minute = lower_bound.minute)
		if lower_bound.time() < upper_bound.time():
			q = '''SELECT DATE(tstamp), lat, lng, network FROM {} WHERE tstamp >= '{}' AND tstamp <= '{}' AND TIME(tstamp) >= '{}' AND TIME(tstamp) <= '{}';'''.format(database, min_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%Y-%m-%d %H:%M:%S'), min_date.strftime('%H:%M:%S'), max_date.strftime('%H:%M:%S'))
			data, i = exec_mysql(q, self.mysql)
		else:
			q = '''SELECT DATE(tstamp), lat, lng, network FROM {} WHERE tstamp >= '{}' AND tstamp <= '{}' AND TIME(tstamp) >= '{}' AND TIME(tstamp) <= '23:59:59';'''.format(database, min_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%Y-%m-%d %H:%M:%S'), min_date.strftime('%H:%M:%S'))
			data = exec_mysql(q, self.mysql)[0]
			q = '''SELECT DATE_ADD(DATE(tstamp),INTERVAL -1 DAY) AS 'DATE(tstamp)', lat, lng, network FROM {} WHERE tstamp >= '{}' AND tstamp <= '{}' AND TIME(tstamp) >= '00:00:00' AND TIME(tstamp) <= '{}';'''.format(database, min_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%H:%M:%S'))
			data += exec_mysql(q, self.mysql)[0]
		return data

	def build_current_tree(self):
		"""
		Building current KDTree from data, stored in Redis database.
		Every tweet there has expiration time: TIME_SLIDING_WINDOW/fast_forward_ratio
		So at every time only currently active tweets are selected.
		"""
		data = []
		for key in self.redis.keys("message:*"):
			try:
				message = ploads(self.redis.get(key))
			except TypeError:
				pass
			else:
				if message['id'] == 0 and message['text'] == 'TheEnd':
					self.interrupter = True
				data.append(message)
		self.current_datapoints = data
		if len(data):
			self.tree = KDTree(array([[x['lng'], x['lat']] for x in data]))

	def get_current_outliers(self):
		"""
		Computational part:
		- calculate mean and standart deviation for kNN distance for current points using referenced KDTrees
		- compare referenced values with current tree, find outliers (3 standart deviations from mean)
		Result: returns points without noise (outliers only)
		"""
		if len(self.current_datapoints):
			cur_knn_data = mean(self.tree.query(array([[x['lng'], x['lat']] for x in self.current_datapoints]), k=5, return_distance=True)[0], axis=1)
			#print self.reference_trees
			#print array([x.query(array([[x['lng'], x['lat']] for x in self.current_datapoints]), k=5, return_distance=True)[0] for x in self.reference_trees])
			ref_knn_data = mean(array([x.query(array([[x['lng'], x['lat']] for x in self.current_datapoints]), k=5, return_distance=True)[0] for x in self.reference_trees]), axis=2)
			thr_knn_mean = mean(ref_knn_data, axis=0)
			thr_knn_std = std(ref_knn_data, axis=0)
			thr_knn_data =  thr_knn_mean - thr_knn_std  * 3
			points = list(array(self.current_datapoints)[cur_knn_data < thr_knn_data])
			weights = (absolute(cur_knn_data - thr_knn_mean)/thr_knn_std)[cur_knn_data < thr_knn_data]
			for i in range(len(weights)):
				points[i]['weight'] = weights[i]
			return points
		else:
			return []

	def dbscan_tweets(self, points):
		"""
		Method clusters points-outliers into slice-clusters using DBSCAN.
		Returns dict of slice-clusters - base for event-candidates.
		"""
		if len(points):
			clustering = DBSCAN(eps=self.eps, min_samples=5)
			labels = clustering.fit_predict(array([[x['lng'], x['lat']] for x in points]))
			ret_tab = [dict(points[i].items() + {'cluster':labels[i], 'is_core': i in clustering.core_sample_indices_}.items()) for i in range(len(labels)) if labels[i] > -1]
			ret_tab = { x[0]:tuple(x[1]) for x in groupby(sorted(ret_tab, key=lambda x:x['cluster']), lambda x: x['cluster']) }
			return ret_tab
		else:
			return {}

	def get_previous_events(self):
		"""
		Loading previously saved events from Redis database - 
		to have data to merge with currently created slice-clusters
		"""
		self.current_events = {}
		for key in self.redis.keys("event:*"):
			event = ploads(self.redis.get(key))
			self.current_events[event.id] = event

	def merge_slices_to_events(current_slices):
		"""
		Looking for comparation between slices and events.
		Updating events, if needed; creating new events.
		"""
		prev_events = self.current_events.values()
		for event_slice in current_slices:
			slice_ids = set([x['id'] for x in event_slice])
			ancestors = []
			for event in prev_events:
				if event.is_successor(slice_ids):
					ancestors.append(event.id)
			if len(ancestors) == 0:
				new_event = Event(self.mysql, event_slice)
				self.current_events[new_event.id] = new_event
			elif len(ancestors) == 1:
				self.current_events[ancestors[0]].add_slice(event_slice)
			else:
				for ancestor in ancestors[1:]:
					self.current_events[ancestors[0]].merge(self.current_events[ancestor])
					del self.current_events[ancestor]
				self.current_events[ancestors[0]].add_slice(event_slice)

	def dump_current_events(self):
		"""
		Saves events to Redis after adding new slices and removing expired events.
		In parallel looks through self.current_events dict: searches for events without updates
		for TIME_SLIDING_WINDOW/fast_forward_ratio time.

		"""
		for event in self.current_events.values():
			if (datetime.now() - event.updated).total_seconds() > TIME_SLIDING_WINDOW/fast_forward_ratio:
				event.backup()
			else:
				self.redis.set("event:{}".format(event.id), pdumps(event))

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

	def __init__(self, mysql_con, redis_con, points):
		self.id = uuid4()
		self.created = datetime.now()
		self.updated = datetime.now()

		self.mysql = mysql_con
		self.redis = redis_con
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

	def is_successor(self, slice_ids, threshold = 0):
		if len(set(self.messages.keys()).intersection(slice_ids)) > threshold:
			return True
		return False

	def merge(self, other_event):
		self.messages.update(other_event.messages)
		self.media.update(other_event.media)

		self.build_tokens_graph()
		self.score_messages_by_text()
		self.event_summary_stats()

		self.updated = datetime.now()

	def add_slice(self, new_slice):
		self.messages.update({ x['id']:x for x in new_slice })
		self.get_messages_data([x['id'] for x in new_slice])
		self.get_media_data([x['id'] for x in new_slice])

		self.build_tokens_graph()
		self.score_messages_by_text()
		self.event_summary_stats()

		self.updated = datetime.now()

	def backup(self):
		q = '''INSERT INTO events(id, start, end, vocabulary, core) VALUES ("{}", "{}", "{}", "{}", "{}");'''.format(			self.id, self.start, self.end, escape_string(', '.join(self.vocabulary)), escape_string(', '.join(self.core)))
		exec_mysql(q, self.mysql)
		q = '''INSERT INTO event_msgs(msg_id, event_id) VALUES {};'''.format(','.join(['({},{})'.format(x, self.id) for x in self.messages.keys()]))
		exec_mysql(q, self.mysql)
		self.redis.delete("event:{}".format(self.id))

	def get_messages_data(self, ids=None):
		if not ids:
			ids = [x['id'] for x in self.messages.values()]
		q = '''SELECT * FROM tweets WHERE id in ({});'''.format(','.join(['"'+str(x)+'"' for x in ids]))
		data = exec_mysql(q, self.mysql)[0]
		for item in data:
			self.messages[item['id']].update(item)

	def get_media_data(self, ids=None):
		if not ids:
			ids = [x['id'] for x in self.messages.values()]
		q = '''SELECT * FROM media WHERE tweet_id in ({});'''.format(','.join(['"'+str(x)+'"' for x in ids]))
		data = exec_mysql(q, self.mysql)[0]
		for item in data:
			self.media[item['id']] = item

	def event_summary_stats(self):
		authorsip_stats = [len(tuple(i[1])) for i in groupby(sorted(self.messages.values(), key=lambda x:x['user']), lambda z: z['user'])]
		self.entropy = entropy(authorsip_stats)
		self.ppa = mean(authorsip_stats)
		self.start = min([x['tstamp'] for x in self.messages.values()])
		self.end = max([x['tstamp'] for x in self.messages.values()])

	def add_stem_texts(self):
		for i in self.messages.keys():
			txt = self.messages[i]['text']
			txt = sub(self.url_re, '', txt)
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

if __name__ == '__main__':
	redis_db = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
	getNewPool().maxActiveConnections = 1
	mysql_db = getNewConnection(username=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, db=MYSQL_DB)
	exec_mysql('SET NAMES utf8mb4;', mysql_db)
	exec_mysql('SET CHARACTER SET utf8mb4;', mysql_db)
	exec_mysql('SET character_set_connection=utf8mb4;', mysql_db)

	#emulator = CollectorEmulator(mysql_db, redis_db, fast_forward_ratio=60, start_timeout=0)
	detector = EventDetector(mysql_db, redis_db, BBOX, fast_forward_ratio=60)
	detector.run()