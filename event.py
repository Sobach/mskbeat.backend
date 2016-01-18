#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# SYSTEM
from datetime import datetime
from re import compile, sub, match, UNICODE, IGNORECASE
from itertools import groupby, combinations
from pickle import loads as ploads, dumps as pdumps
from uuid import uuid4

# MATH
from numpy import mean
from networkx import Graph, connected_components, find_cliques
from scipy.stats import entropy
from shapely.geometry import MultiPoint

# NLTK
from pymorphy2 import MorphAnalyzer
from nltk.tokenize import TreebankWordTokenizer

# SELF IMPORT
from utilities import exec_mysql

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
			self.id = str(uuid4())
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