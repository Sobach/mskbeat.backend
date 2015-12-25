#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# DATABASES
import redis
import PySQLPool
import MySQLdb

import datetime
import threading
import time

# NEW
import math
import itertools
from sklearn.neighbors import BallTree
from sklearn.cluster import DBSCAN
from scipy.stats import entropy
import networkx as nx
import json
import uuid
import copy
import pymorphy2
from nltk.tokenize import TreebankWordTokenizer
import re
import ast
import pickle
import igraph

# CONSTANTS
from settings import *
from functions import *

# MATH
import numpy as np

import pprint

class EventDetector():
	"""Event detector Object: used to discover events both online and offline."""

	def __init__(self, redis_connection, mysql_connection, bbox, nets):
		"""Initialization.

		Args:
			redis_connection (StrictRedis): RedisDB connection Object
			mysql_connection (PySQLPoolConnection): MySQL connection Object
			bbox (List[float]): min longitude, min latitude, max longitude, max latitude
			nets (Dict[str, int]): accordance between one-letter network marks (in MySQL) and ints in numpy arrays"""
		self.redis = redis_connection
		self.mysql = mysql_connection
		self.bbox = bbox
		self.nets = nets
		self.inv_nets = {self.nets[k]:k for k in self.nets.keys()}

	def detect_offline(self, start, end, grain=100, min_threshold=3, thres_nnd=1.8, thres_deviation=3):
		"""Method implementing offline events detecting and clusterisation. Used on historical data, stored in MySQL database.

		Args:
			start (Datetime): min timestamp for historical data
			end (Datetime): max timestamp for historical data
			grain (Optional[int]): number of geo grid cells both for latitude and longitude
			min_threshold (Optional[int]): minimal number of messages in averages grid cell
			thres_nnd (Optional[float]): maximal nearest neighbor distance
			thres_deviation (Optional[float]): minimal excess for number of messages in cell above threshold to consider one-cell event"""
		print str(datetime.datetime.now()), 'Loading messages...'
		self.get_data(start, end)
		print str(datetime.datetime.now()), 'Calculating thresholds...'
		self.build_average_for_day(grain=grain, min_threshold=min_threshold)
		print str(datetime.datetime.now()), 'Calculating event grains...'
		self.get_grains_offline(grain=grain)
		print str(datetime.datetime.now()), 'Filtering event grains...'
		grains = self.load_grains()
		self.filter_grains(grains = grains, thres_nnd=thres_nnd, thres_deviation=thres_deviation)
		print str(datetime.datetime.now()), 'Clustering event grains...'
		candidates = self.create_candidates(self.cluster_grains(thres_nnd=thres_nnd))
		print str(datetime.datetime.now()), 'Adding content to events...'
		candidates = self.rate_candidates(candidates)
		for key in self.redis.keys("candidates:*"):
			self.redis.delete(key)
		output = open('event_candidates_rich.pkl', 'wb')
		pickle.dump(candidates, output)
		output.close()

	def get_data(self, start, end):
		"""Loads historical data from MySQL database. Creates properties: 
			self.raw_data, self.min_data, self.max_date

		Args:
			start (Datetime): min timestamp for historical data
			end (Datetime): max timestamp for historical data"""
		q = 'SELECT lat, lng, tstamp, network FROM tweets WHERE tstamp <= "{}" AND tstamp >= "{}" AND lng > {} AND lat > {} AND lng <  {} AND lat < {}'.format(datetime.datetime.strftime(end, '%Y-%m-%d %H:%M:%S'), datetime.datetime.strftime(start, '%Y-%m-%d %H:%M:%S'), self.bbox[0], self.bbox[1], self.bbox[2], self.bbox[3])
		self.raw_data = exec_mysql(q, self.mysql)[0]
		self.min_date = min([x['tstamp'] for x in self.raw_data])
		self.max_date = max([x['tstamp'] for x in self.raw_data])

	def build_average(self, hour, grain=100, min_threshold=3, expire=24*60*60):
		"""Method builds averages/thresholds for concrete hour, based on self.data and provided args. 
		Result is being dumped to RedisDB, folder "thresholds", and subfolder - the hour, thresholds created for.

		Args:
			hour (int): the hour, thresholds created for. From 0 to 23
			grain (Optional[int]): number of geo grid cells both for latitude and longitude
			min_threshold (Optional[int]): minimal number of messages in averages grid cell
			expire (Optional[int]): time, pre-computed values should exist, in seconds
			"""
		days = int((self.max_date - self.min_date).days) + 1
		self.threshold_dict = {}
		data = [x for x in self.raw_data if x['tstamp'].hour == hour]
		get_coords = np.vectorize(online_cube_coords, excluded=['density', 'bbox', 'min_date', 'nets'], otypes=[np.uint8])
		density = np.zeros((3, days, grain, grain), dtype=np.uint16)
		get_coords(req_item=np.array(data), density=density, bbox=self.bbox, min_date=self.min_date, nets=self.nets)
		thresholds = (np.mean(density, axis=1) + 2 * np.std(density, axis=1)).astype(np.uint16)
		coords = np.where(thresholds > min_threshold)
		thresholds = {tuple([x[i] for x in coords]):thresholds[tuple([y[i] for y in coords])] for i in range(len(coords[0]))}
		self.redis.hmset('threshold:{}'.format(hour), thresholds)
		self.redis.expire('threshold:{}'.format(hour), 60*60*24)

	def build_average_for_day(self, grain=100, min_threshold=3, expire=24*60*60):
		"""Method for creating a full thresholds grid for a whole day (24 hours)

		Args:
			grain (Optional[int]): number of geo grid cells both for latitude and longitude
			min_threshold (Optional[int]): minimal number of messages in averages grid cell
			expire (Optional[int]): time, pre-computed values should exist, in seconds"""
		for i in range(24):
			self.build_average(i, grain, min_threshold, expire)

	def get_threshold(self, coord, min_threshold=3):
		"""Function to get threshold for a concrete cell.
		Cell coordinates format: hour, net, longitude, latitude

		Args:
			coord (List[int]): list with grid coordinates for cell of interst
			min_threshold (Optional[int]): minimal number of messages in averages grid cell

		Returns:
			int: threshold for a given cell in a given hour"""
		hour = coord[0]
		coord = '({})'.format(', '.join([str(x) for x in coord[1:]]))
		try:
			return(int(self.redis.hmget('threshold:{}'.format(hour), coord)[0]))
		except TypeError:
			return(min_threshold)

	def get_grains_offline(self, grain=100, data=None, expire=24*60*60):
		if not data: data = self.raw_data
		days = int((self.max_date - self.min_date).days) + 1
		min_day = int(math.floor(time.mktime(self.min_date.timetuple())/(60*60*24))) + 1
		get_coords = np.vectorize(offline_cube_coords, excluded=['density', 'bbox', 'min_date', 'nets'], otypes=[np.uint8])
		density = np.zeros((3, days, grain, grain, 24), dtype=np.uint16)
		get_coords(req_item=np.array(data), density=density, bbox=self.bbox, min_date=self.min_date, nets=self.nets)
		coords = np.where(density > 0)
		grain_candidates = {tuple([x[i] for x in coords]):density[tuple([y[i] for y in coords])] for i in range(len(coords[0]))}
		grains = {}
		for k, v in grain_candidates.items():
			"""k = network, day, longitude, latitude, hour"""
			expect = self.get_threshold([k[4], k[0], k[2], k[3]])
			if v > expect:
				k = list(k)
				k[1] += min_day
				grains[tuple(k)] = (v, float(v)/expect)
		self.redis.delete('grains:offline')
		self.redis.hmset('grains:offline', grains)
		self.redis.expire('grains:offline', expire)

	def load_grains(self, name='grains:offline'):
		grains = self.redis.hgetall(name)
		grains = {ast.literal_eval(k):ast.literal_eval(v) for k, v in grains.items()}
		return grains

	def filter_grains(self, grains, thres_nnd=1.8, thres_deviation=3, expire=24*60*60):
		if len(grains) > 1:
			distance_matrix = []
			for k, v in grains.items():
				distance_matrix.append([k[0], k[1]*24+k[4], k[2], k[3]])
			tree = BallTree(np.array(distance_matrix))
			distances = tree.query(distance_matrix, k=2, return_distance=True)[0][:,1]
		else:
			distances = np.array([thres_nnd + 1])
		clean_grains = {}
		for i in range(len(grains.items())):
			if grains.items()[i][1][1] >= thres_deviation or distances[i] <= thres_nnd:
				clean_grains[grains.items()[i][0]] = grains.items()[i][1]
		self.redis.delete('grains:filtered')
		self.redis.hmset('grains:filtered', clean_grains)
		self.redis.expire('grains:filtered', expire)

	def cluster_grains(self, gr_name='filtered', thres_nnd=1.8):
		grains = self.load_grains('grains:{}'.format(gr_name))
		grains = [[k[0], k[1]*24+k[4], k[2], k[3]] for k in grains.keys()]
		cluster = DBSCAN(eps=thres_nnd, min_samples=1)
		clusters = cluster.fit_predict(np.array(grains))
		return(clusters)

	def create_candidates(self, clusters, gr_name='filtered', expire=24*60*60):
		grains = self.load_grains('grains:{}'.format(gr_name))
		event_candidates = {}
		for element in set(clusters):
			coords = [grains.keys()[i] for i in range(len(grains.items())) if clusters[i] == element]
			weight = sum([grains[x][1] for x in coords])
			event_candidates[str(uuid.uuid4())] = {'coords':[tuple([x[0], x[3], x[2], x[1]*24+x[4]]) for x in coords], 'weight':weight, 'created':int(time.time()), 'updated':int(time.time())}
		return event_candidates

	def rate_candidates(self, event_candidates, filter = True):
		e_describer = EventContent(self.redis, self.mysql, self.bbox, self.inv_nets)
		e_total = len(event_candidates.keys())
		counter = 1
		for event_key in event_candidates.keys():
			print datetime.datetime.now(), 'Event {}/{}'.format(counter, e_total), 'Grains: ', len(event_candidates[event_key]['coords'])
			counter += 1
			e_describer.process_event(event_candidates[event_key]['coords'])
			event_candidates[event_key]['messages'] = e_describer.raw_messages
			event_candidates[event_key]['media'] = e_describer.media
			event_candidates[event_key]['entropy'] = {x[0]:entropy([len(tuple(i[1])) for i in itertools.groupby([y for y in x[1]], lambda z: z['user'])]) for x in itertools.groupby(event_candidates[event_key]['messages'], lambda x: x['network'])}
			event_candidates[event_key]['ppa'] = {x[0]:np.mean([len(tuple(i[1])) for i in itertools.groupby([y for y in x[1]], lambda z: z['user'])]) for x in itertools.groupby(event_candidates[event_key]['messages'], lambda x: x['network'])}
		return event_candidates

class EventContent():
	def __init__(self, redis_connection, mysql_connection, bbox, inv_nets, grain = 100):
		self.redis = redis_connection
		self.mysql = mysql_connection
		self.grain = grain
		self.bbox = bbox
		self.morph = pymorphy2.MorphAnalyzer()
		self.tokenizer = TreebankWordTokenizer()
		self.inv_nets = inv_nets
		self.word = re.compile(r'^\w+$', flags = re.UNICODE | re.IGNORECASE)
		self.timecounters = {'getting_msgs':0, 'stemming_msgs':0, 'building_graph':0, 'scoring_msgs':0, 'getting_media':0, 'scoring_media':0}

	def __reset__(self):
		self.raw_messages = []
		self.media = []
		self.vocabulary = []
		self.core = []
		self.engagement_weight_min = 0
		self.engagement_weight_max = 0

	def process_event(self, coords, sort_messages = True, filter_messages = True):
		self.__reset__()

		self.get_messages(coords)
		self.stem_texts()
		self.build_text_graph()
		self.score_messages(sort_messages = sort_messages, filter_messages = filter_messages)
		self.get_media()
		self.score_media(sort_messages = sort_messages)

	def get_messages(self, coords):
		self.raw_messages = []
		for coord in coords:
			coords_transformed = cell_coords(coord, self.bbox, self.inv_nets, self.grain)
			q = 'SELECT * FROM tweets WHERE network = "{}" AND lat >= {} AND lat <= {} AND lng >= {} AND lng <= {} AND tstamp >= "{}" AND tstamp <= "{}";'.format(
				coords_transformed['network'], 
				coords_transformed['lat'][0], coords_transformed['lat'][1], 
				coords_transformed['lng'][0], coords_transformed['lng'][1], 
				coords_transformed['tstamp'][0].strftime('%Y-%m-%d %H:%M:%S'), coords_transformed['tstamp'][1].strftime('%Y-%m-%d %H:%M:%S'))
			self.raw_messages += exec_mysql(q, self.mysql)[0]

	def get_media(self):
		if self.raw_messages == []:
			self.media = []
			return
		q = 'SELECT * FROM media WHERE tweet_id IN ({});'.format(','.join(["'"+x['id']+"'" for x in self.raw_messages]))
		self.media = exec_mysql(q, self.mysql)[0]

	def stem_texts(self):
		for i in range(len(self.raw_messages)):
			self.raw_messages[i]['tokens'] = set([self.morph.parse(token.decode('utf-8'))[0].normal_form for token in self.tokenizer.tokenize(self.raw_messages[i]['text']) if re.match(self.word, token.decode('utf-8'))])

	def build_text_graph(self, threshold=0):
		G = nx.Graph()
		edges = {}
		for item in self.raw_messages:
			item['tokens'] = set([x for x in item['tokens'] if len(x) > 2])
			if len(item['tokens']) < 2:
				continue
			for pair in itertools.combinations(item['tokens'], 2):
				try:
					edges[tuple(sorted(pair))] += 1
				except KeyError:
					edges[tuple(sorted(pair))] = 1
		edges = {k:v for k,v in edges.items() if v > threshold}
		G.add_edges_from(edges.keys())
		try:
			self.vocabulary = set(sorted(nx.connected_components(G), key = len, reverse=True)[0])
		except IndexError:
			self.vocabulary = set([])
		try:
			self.core = set(sorted(nx.find_cliques(G), key = len, reverse=True)[0])
		except IndexError:
			self.core = set([])

	def score_message_by_text(self, message):
		try:
			return float(len(self.vocabulary.intersection(message['tokens'])))/len(self.vocabulary.union(message['tokens'])) + \
				float(len(self.core.intersection(message['tokens'])))/len(self.core.union(message['tokens']))
		except ZeroDivisionError:
			return 0

	def score_message_by_engagement(self, message):
		if message['text_weight'] > 0:
			data = self.redis.hgetall('message:{}:{}'.format(message['network'], message['id']))
			if not data:
				self.redis.hmset('message:{}:{}'.format(message['network'], message['id']), {'likes':0, 'shares':0, 'comments':0, 'weight':message['text_weight'], 'updated':int(time.time())})
				self.redis.expire('message:{}:{}'.format(message['network'], message['id']), 25*60*60)
				return 0
			else:
				return int(data['likes']) + int(data['shares']) + int(data['comments'])
		return 0

	def score_messages(self, sort_messages = True, filter_messages = True):
		for i in range(len(self.raw_messages)):
			self.raw_messages[i]['text_weight'] = self.score_message_by_text(self.raw_messages[i])
			self.raw_messages[i]['engagement_weight'] = self.score_message_by_engagement(self.raw_messages[i])
			if self.engagement_weight_min > self.raw_messages[i]['engagement_weight']:
				self.engagement_weight_min = self.raw_messages[i]['engagement_weight']
			if self.engagement_weight_max < self.raw_messages[i]['engagement_weight']:
				self.engagement_weight_max = self.raw_messages[i]['engagement_weight']
		if filter_messages:
			self.raw_messages = [x for x in self.raw_messages if x['text_weight'] > 0]
		if sort_messages:
			self.raw_messages = sorted(self.raw_messages, key = self.sorting_key, reverse = True)

	def score_media(self, sort_messages = True):
		score_dict = {x['id']:{'text_weight':x['text_weight'], 'engagement_weight':x['engagement_weight']} for x in self.raw_messages}
		for i in range(len(self.media)):
			self.media[i].update(score_dict[self.media[i]['tweet_id']])
		if sort_messages:
			self.media = sorted(self.media, key = self.sorting_key, reverse = True)

	def sorting_key(self, message):
		return message['text_weight'] + math.log1p(message['engagement_weight']) - ((math.log1p(self.engagement_weight_max) - math.log1p(self.engagement_weight_min))/2 + math.log1p(self.engagement_weight_min))

if __name__ == '__main__':
	redis_db = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
	PySQLPool.getNewPool().maxActiveConnections = 1
	mysql_db = PySQLPool.getNewConnection(username=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, db=MYSQL_DB)
	query = PySQLPool.getNewQuery(mysql_db, commitOnEnd=True)
	query.Query('SET NAMES utf8mb4;')
	query.Query('SET CHARACTER SET utf8mb4;')
	query.Query('SET character_set_connection=utf8mb4;')


	detector = EventDetector(redis_connection=redis_db, mysql_connection=mysql_db, bbox=BBOX, nets=NETS)
	end = datetime.datetime.strptime('2015-07-17 00:00:00', '%Y-%m-%d %H:%M:%S')
	start = datetime.datetime.strptime('2015-07-01 00:00:00', '%Y-%m-%d %H:%M:%S')
	detector.detect_offline(start, end)