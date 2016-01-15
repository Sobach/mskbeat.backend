#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# SYSTEM
from datetime import datetime, timedelta
from re import compile, sub, match, UNICODE, IGNORECASE
from itertools import groupby, combinations
from pickle import loads as ploads, dumps as pdumps
from time import sleep
from json import dumps as jdumps
from uuid import uuid4

# DATABASE
from redis import StrictRedis
from MySQLdb import escape_string

# MATH
from numpy import array, mean, std, absolute, seterr
from networkx import Graph, connected_components, find_cliques
from sklearn.neighbors import KDTree
from sklearn.cluster import DBSCAN
from scipy.stats import entropy
from shapely.geometry import MultiPoint

# SYSTEM MATH
from math import radians, cos, sin, asin, sqrt

# NLTK
from pymorphy2 import MorphAnalyzer
from nltk.tokenize import TreebankWordTokenizer

# CONSTANTS
from settings import *
from utilities import get_mysql_con, exec_mysql

seterr(divide='ignore', invalid='ignore')

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
		self.ffr = fast_forward_ratio
		self.events = {}

	def run(self):
		"""
		Main object loop. Looks for actual messages, DBSCANs them, and merges with previously computed events.
		Interrupts if self.interrupter is set to True.
		"""
		while True:
			self.build_reference_trees(datetime.now(), take_origins = True)
			self.build_current_trees()
			points = self.get_current_outliers()
			slice_clusters = self.dbscan_tweets(points)
			self.get_previous_events()
			self.merge_slices_to_events(slice_clusters)
			self.dump_current_events()
			if self.interrupter:
				for event in self.events.values():
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

	def build_reference_trees(self, time, days = 14, take_origins = False, min_points = 10):
		"""
		Create kNN-trees (KDTrees) for previous period - 1 day = 1 tree. 
		These trees are used as reference, when comparing with current kNN-distance.
		Trees are created for each network separately.
		Args:
			time (datetime): timestamp for data of interest. 75% of data is taken from the past, and 25% - from the future.
			days (int): how many days should be used for reference (by default 2 weeks)
			take_origins (bool): whether to use actual dynamic data (default), or training dataset
			min_points (int): minimum number of points for every tree. if there is not enough data, points (0,0) are used

		Result:
			self.trees (List[KDTree])
		"""
		data = self.get_reference_data(time, days, take_origins)
		networks = [1,2,3]
		preproc = {net:{} for net in networks}
		for item in data:
			try: 
				preproc[item['network']][item['DATE(tstamp)']].append([item['lng'], item['lat']])
			except KeyError: 
				preproc[item['network']][item['DATE(tstamp)']] = [[item['lng'], item['lat']]]
		self.reference_trees = {net:[] for net in networks}
		for net in networks:
			if not preproc[net]:
				self.reference_trees[net] = [KDTree(array([(0,0)]*min_points))]*days
				continue
			#try:
			for element in preproc[net].values():
				if len(element) < min_points:
					element += [(0,0)]*(min_points - len(element))
				self.reference_trees[net].append(KDTree(array(element)))
			#except KeyError:
			#	self.reference_trees[net] = [KDTree(array([(0,0)]*min_points))]*days

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

	def build_current_trees(self):
		"""
		Building current KDTree from data, stored in Redis database.
		Every tweet there has expiration time: TIME_SLIDING_WINDOW/fast_forward_ratio
		So at every time only currently active tweets are selected.
		"""
		data = {}
		for key in self.redis.keys("message:*"):
			try:
				message = ploads(self.redis.get(key))
			except TypeError:
				pass
			else:
				if message['id'] == 0 and message['text'] == 'TheEnd':
					self.interrupter = True
				try:
					data[message['network']].append(message)
				except KeyError:
					data[message['network']] = [message]
		self.current_datapoints = data
		if data:
			self.current_trees = {}
			for net in data.keys():
				self.current_trees[net] = KDTree(array([[x['lng'], x['lat']] for x in data[net]]))

	def get_current_outliers(self, neighbour_points = 5):
		"""
		Computational part:
		- calculate mean and standart deviation for kNN distance for current points using referenced KDTrees
		- compare referenced values with current tree, find outliers (3 standart deviations from mean)
		Result: returns points without noise (outliers only)

		Args:
			neighbour_points (int)
		"""
		points = []
		if self.current_datapoints:
			for net in self.current_datapoints.keys():
				if len(self.current_datapoints[net]) < neighbour_points:
					continue
				cur_knn_data = mean(self.current_trees[net].query(array([[x['lng'], x['lat']] for x in self.current_datapoints[net]]), k=5, return_distance=True)[0], axis=1)
				ref_knn_data = mean(array([x.query(array([[x['lng'], x['lat']] for x in self.current_datapoints[net]]), k=5, return_distance=True)[0] for x in self.reference_trees[net]]), axis=2)
				thr_knn_mean = mean(ref_knn_data, axis=0)
				thr_knn_std = std(ref_knn_data, axis=0)
				thr_knn_data =  thr_knn_mean - thr_knn_std  * 3
				new_points = list(array(self.current_datapoints[net])[cur_knn_data < thr_knn_data])
				weights = (absolute(cur_knn_data - thr_knn_mean)/thr_knn_std)[cur_knn_data < thr_knn_data]
				for i in range(len(weights)):
					new_points[i]['weight'] = weights[i]
				points += new_points
		return points

	def dbscan_tweets(self, points):
		"""
		Method clusters points-outliers into slice-clusters using DBSCAN.
		Returns dict of slice-clusters - base for event-candidates.
		"""
		if points:
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
		self.events = {}
		for key in self.redis.keys("event:*"):
			event = Event(self.mysql, self.redis)
			event.loads(key[6:])
			self.events[event.id] = event

	def merge_slices_to_events(self, current_slices):
		"""
		Looking for comparation between slices and events.
		Updating events, if needed; creating new events.
		"""
		prev_events = self.events.values()
		for event_slice in current_slices.values():
			slice_ids = {x['id'] for x in event_slice}
			ancestors = []
			for event in prev_events:
				if event.is_successor(slice_ids):
					ancestors.append(event.id)
			if len(ancestors) == 0:
				new_event = Event(self.mysql, self.redis, event_slice)
				self.events[new_event.id] = new_event
			elif len(ancestors) == 1:
				self.events[ancestors[0]].add_slice(event_slice)
			else:
				for ancestor in ancestors[1:]:
					self.events[ancestors[0]].merge(self.events[ancestor])
					del self.events[ancestor]
					self.redis.delete("event:{}".format(ancestor))
				self.events[ancestors[0]].add_slice(event_slice)

	def dump_current_events(self):
		"""
		Saves events to Redis after adding new slices and removing expired events.
		In parallel looks through self.current_events dict: searches for events without updates
		for TIME_SLIDING_WINDOW/fast_forward_ratio time.

		"""
		for event in self.events.values():
			if (datetime.now() - event.updated).total_seconds() > TIME_SLIDING_WINDOW/self.ffr:
				event.backup()
			else:
				event.dumps()

class Event():
	"""
	Event object - class for working with event candidates.
	Collects all data on event candidate, stores it between clustering slices; merges slices, if required.
	TBD: constructs and saves description, scores texts and media, scores and descripts event itself 
	(probability, that candidate is real, event buzz, event category).

	Attributes:
		self.created (datetime): creation timestamp
		self.updated (datetime): last update timestamp
		self.start (datetime): timestamp of the first message in the self.messages dict
		self.end (datetime): timestamp of the last message in the self.messages dict
		self.messages (Dict[dict]): raw tweets from database, enriched with weight, is_core params (on init), tokens (after add_stem_texts)
		self.media (Dict[dict]): raw media objects from database
		self.vocabulary (Set): tokens, that form the largest connected component in tokens graph; computed in build_tokens_graph() method
		self.core (Set): tokens, that form the largest clique (maximal connected component) in tokens graph; computed in build_tokens_graph() method
		self.entropy (float): entropy for authorship: 0 for mono-authored cluster; computed in event_summary_stats() method
		self.ppa (float): average number of posts per one author; computed in event_summary_stats() method
		self.event_hull (shapely.geometry.polygon.Polygon): convex hull for all messages of the event; computed in add_geo_shapes() method
		self.core_hull (shapely.geometry.polygon.Polygon): convex hull for core messages of the event (recognized as core by DBSCAN); computed in add_geo_shapes() method

	Methods:
		self.event_update: commands to calculate all data on event, based on messages and media
		self.is_successor: examines, if current event have common messages with specified event slice
		self.is_mono_network: examines, if current event is mononetwork, i.e. all the messages are from one network
		self.convex_hull_intersection: calculates Jaccard distance for convex hulls and core convex hulls of two events
		self.merge: merge current event with another event, update stat Attributes
		self.add_slice: add messages and media to the event, recompute statistics
		self.backup: dump event to MySQL long-term storage, used for non-evaluating events
		self.loads / self.dumps: serialize/deserialize event and put/get it to Redis
		self.get_messages_data: get MySQL data for messages ids
		self.get_media_data: get MySQL data for media using existing messages ids
		self.event_summary_stats: calculate entropy, ppa, start, and end statistics
		self.add_stem_texts: add tokens lists to self.messages
		self.add_geo_shapes: add convex hull representations of the event
		self.build_tokens_graph: method constructs tokens co-occurrance undirected graph to determine graph vocabulary (largest connected component) and core (largest clique)
		self.score_messages_by_text: method calculates token_score for messages elements. Jaccard distance is used (between message tokens and event vocabulary + core)

	Message keys:
		cluster (int): legacy from DBSCAN - number of cluster (event ancestor)
		id (str): DB message id; unique
		is_core (bool): True, if tweet belongs to the core of ancestor cluster
		iscopy (int): 1, if message is shared from another network
		lat (float): latitude
		lng (float): longitude
		network (int): 1 for Instagram, 0 for Twitter, 2 for VKontakte
		text (str): raw text of the message
		tokens (Set[str]): collection of stemmed tokens from raw text; created in add_stem_texts()
		tstamp (datetime): 'created at' timestamp
		user (int): user id, absolutely unique for one network, but matches between networks are possible
		tokens_score (float): agreement estimation with average cluster text, based on Jaccard distance [0:2]
	"""

	def __init__(self, mysql_con, redis_con, points = []):
		"""
		Initialization.

		Args:
			mysql_con (PySQLPoolConnection): MySQL connection Object
			redis_con (StrictRedis): RedisDB connection Object
			points (list[dict]): raw messages from event detector
		"""
		self.mysql = mysql_con
		self.redis = redis_con
		self.morph = MorphAnalyzer()
		self.tokenizer = TreebankWordTokenizer()
		self.word = compile(r'^\w+$', flags = UNICODE | IGNORECASE)
		self.url_re = compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

		if points:
			self.id = uuid4()
			self.created = datetime.now()
			self.updated = datetime.now()

			self.messages = { x['id']:x for x in points }
			self.get_messages_data()
			self.media = {}
			self.get_media_data()
			self.event_update()

	def __str__(self):
		txt = '<Event {}: {} msgs [{} -- {}]>'.format(self.id, len(self.messages), self.start.strftime("%Y-%m-%d %H:%M"), self.end.strftime("%H:%M"))
		return txt

	def __unicode__(self):
		txt = u'<Event {}: {} msgs [{} -- {}]>'.format(self.id, len(self.messages), self.start.strftime("%Y-%m-%d %H:%M"), self.end.strftime("%H:%M"))
		return txt

	def __repr__(self):
		txt = '<Event {}: {} msgs [{} -- {}]>'.format(self.id, len(self.messages), self.start.strftime("%Y-%m-%d %H:%M"), self.end.strftime("%H:%M"))
		return txt

	def event_update(self):
		"""
		Commands to calculate all data on event, based on messages and media.
		"""
		self.add_stem_texts()
		self.build_tokens_graph()
		self.score_messages_by_text()
		self.event_summary_stats()
		self.add_geo_shapes()

	def is_successor(self, slice_ids, threshold = 0):
		"""
		Method examines, if current event have common messages with specified event slice.

		Args:
			slice_ids (Set): set if message id's to compare with
			threshold (int): to recognize connection intersection between event messages and new slice should be more than threshold
		"""
		if len(set(self.messages.keys()).intersection(slice_ids)) > threshold:
			return True
		return False

	def is_mono_network(self):
		"""
		Method examines, if current event is mono networked, i.e. all the messages are from one network (Instagram, Twitter, or Facebook)
		"""
		if len({x['network'] for x in self.messages.values()}) <= 1:
			return True
		return False

	def convex_hull_intersection(self, other_event):
		"""
		Method calculates Jaccard distance for convex hulls and core convex hulls of two events.

		Args:
			other_event (Event): another event object - to intersect with
		"""
		if self.event_hull.disjoint(other_event.event_hull):
			return 0, 0
		hull_intersection = self.event_hull.intersection(other_event.event_hull).area / self.event_hull.union(other_event.event_hull).area
		core_intersection = self.core_hull.intersection(other_event.core_hull).area / self.core_hull.union(other_event.core_hull).area
		return hull_intersection, core_intersection

	def merge(self, other_event):
		"""
		Method merges current event with another event, update stat Attributes.

		Args:
			other_event (Event): another event object - to merge with
		"""
		self.messages.update(other_event.messages)
		self.media.update(other_event.media)
		self.event_update()
		self.updated = datetime.now()

	def add_slice(self, new_slice):
		"""
		Method adds messages and media to the event, recompute statistics.

		Args:
			new_slice (List[dict]): initial list with messages to be added
		"""
		self.messages.update({ x['id']:x for x in new_slice })
		self.get_messages_data([x['id'] for x in new_slice])
		self.get_media_data([x['id'] for x in new_slice])
		self.event_update()
		self.updated = datetime.now()

	def backup(self):
		"""
		Method dumps event to MySQL long-term storage, used for non-evaluating events.
		"""
		q = u'''INSERT INTO events(id, start, end) VALUES ("{}", "{}", "{}");'''.format(self.id, self.start, self.end)
		exec_mysql(q, self.mysql)
		q = '''INSERT INTO event_msgs(msg_id, event_id) VALUES {};'''.format(','.join(['("{}","{}")'.format(x, self.id) for x in self.messages.keys()]))
		exec_mysql(q, self.mysql)
		self.redis.delete("event:{}".format(self.id))

		# Dump to Redis event to restore it in case
		todump = {'id':self.id, 'created':self.created, 'updated':self.updated, 'messages':self.messages, 'media':self.media}
		data = pdumps(todump)
		self.redis.set("dumped:{}".format(self.id), data)

	def loads(self, event_id):
		"""
		Method for deserializing and loading event from Redis database.
		"""
		event_data = ploads(self.redis.get('event:{}'.format(event_id)))
		self.id = event_data['id']
		self.created = event_data['created']
		self.updated = event_data['updated']
		self.messages = event_data['messages']
		self.media = event_data['media']
		self.event_update()

	def dumps(self):
		"""
		Method for serializing and dumping event to Redis database.
		"""
		todump = {'id':self.id, 'created':self.created, 'updated':self.updated, 'messages':self.messages, 'media':self.media}
		data = pdumps(todump)
		self.redis.set("event:{}".format(self.id), data)

	def get_messages_data(self, ids=None):
		"""
		Method loads MySQL data for messages ids and adds it to the self.messagea argument.

		Args:
			ids (List[str]): list of messages ids to load. If not provided, all ids from self.messages are used 
		"""
		if not ids:
			ids = [x['id'] for x in self.messages.values()]
		q = '''SELECT * FROM tweets WHERE id in ({});'''.format(','.join(['"'+str(x)+'"' for x in ids]))
		data = exec_mysql(q, self.mysql)[0]
		for item in data:
			self.messages[item['id']].update(item)

	def get_media_data(self, ids=None):
		"""
		Method loads MySQL data for media using existing messages ids and adds it to the self.media argument.

		Args:
			ids (List[str]): list of messages ids to load. If not provided, all ids from self.messages are used 
		"""
		if not ids:
			ids = [x['id'] for x in self.messages.values()]
		q = '''SELECT * FROM media WHERE tweet_id in ({});'''.format(','.join(['"'+str(x)+'"' for x in ids]))
		data = exec_mysql(q, self.mysql)[0]
		for item in data:
			self.media[item['id']] = item

	def event_summary_stats(self):
		"""
		Method calculates self.entropy and self.ppa statistics, updates self.start and self.end timestamps.
		"""
		authorsip_stats = [len(tuple(i[1])) for i in groupby(sorted(self.messages.values(), key=lambda x:x['user']), lambda z: z['user'])]
		self.entropy = entropy(authorsip_stats)
		self.ppa = mean(authorsip_stats)
		self.start = min([x['tstamp'] for x in self.messages.values()])
		self.end = max([x['tstamp'] for x in self.messages.values()])

	def add_stem_texts(self):
		"""
		Method adds tokens lists to self.messages.
		"""
		for i in self.messages.keys():
			if 'tokens' not in self.messages[i].keys():
				txt = self.messages[i]['text']
				txt = sub(self.url_re, '', txt)
				self.messages[i]['tokens'] = {self.morph.parse(token.decode('utf-8'))[0].normal_form for token in self.tokenizer.tokenize(txt) if match(self.word, token.decode('utf-8'))}

	def add_geo_shapes(self):
		"""
		Method adds convex hull representations of the event (self.event_hull and self.core_hull).
		"""
		self.event_hull = MultiPoint([(x['lng'], x['lat']) for x in self.messages.values()]).convex_hull
		self.core_hull = MultiPoint([(x['lng'], x['lat']) for x in self.messages.values() if x['is_core']]).convex_hull

	def build_tokens_graph(self, threshold=0):
		"""
		Method constructs tokens co-occurrance undirected graph to determine graph vocabulary (largest connected component) and core (largest clique).
		TBD: add graph to event arguments (?)

		Args:
			threshold (int): minimal number of edge weight (co-occurrences count) to rely on the edge.
		"""
		G = Graph()
		edges = {}
		for item in self.messages.values():
			tokens = {x for x in item['tokens'] if len(x) > 2}
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
			self.vocabulary = set()
		try:
			self.core = set(sorted(find_cliques(G), key = len, reverse=True)[0])
		except IndexError:
			self.core = set()

	def score_messages_by_text(self):
		"""
		Method calculates token_score parameter for self.messages. Two Jaccard distances are used (between message tokens and event vocabulary + core).
		"""
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
	mysql_db = get_mysql_con()
	detector = EventDetector(mysql_db, redis_db, BBOX, fast_forward_ratio=60)
	detector.run()