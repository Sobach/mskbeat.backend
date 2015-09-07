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
import datetime

# NEW
import math
import itertools
from sklearn.neighbors import BallTree
from sklearn.cluster import DBSCAN
import networkx as nx
import json
import uuid
import copy

# CONSTANTS
from settings import *

# MATH
import numpy as np

import pprint

redis_db = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
PySQLPool.getNewPool().maxActiveConnections = 1
mysql_db = PySQLPool.getNewConnection(username=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, db=MYSQL_DB)
query = PySQLPool.getNewQuery(mysql_db, commitOnEnd=True)
query.Query('SET NAMES utf8mb4;')
query.Query('SET CHARACTER SET utf8mb4;')
query.Query('SET character_set_connection=utf8mb4;')


BBOX = [float(x) for x in TW_LOCATIONS.split(',')]
NETS = {'i':0, 't':1, 'v':2}
INV_NETS = {0:'i', 1:'t', 2:'v'}

class ThresholdUpdaterThread(threading.Thread):
	def run(self):
		while True:
			d = datetime.datetime.now()
			get_average_array(d.hour)
			self.store_array(self.threshold_matrix)
			wakeup = datetime.datetime.strptime(d.strftime('%Y-%m-%d ')+str(d.hour+1)+':00:00', '%Y-%m-%d %H:%M:%S')
			time.sleep((wakeup - datetime.datetime.now()).seconds)

	def get_average_array(self, hour, min_date=(datetime.datetime.now() - datetime.timedelta(days=14)), max_date=datetime.datetime.now(), grain=100, min_threshold=3):
		days = int((max_date - min_date).days) + 1
		q = 'SELECT lat, lng, tstamp, network FROM tweets WHERE HOUR(tstamp) = {} AND tstamp <= "{}" AND tstamp >= "{}" AND lng > {} AND lat > {} AND lng <  {} AND lat < {}'.format(hour, max_date, min_date, BBOX[0], BBOX[1], BBOX[2], BBOX[3])
		data = exec_mysql(q)[0]
		get_coords = np.vectorize(cube_coords, excluded=['density', 'bbox', 'min_date'], otypes=[np.uint8])
		density = np.zeros((3, days, grain, grain), dtype=np.uint16)
		get_coords(req_item=np.array(data), density=density, bbox=BBOX, min_date=min_date)
		thresholds = (np.mean(density, axis=1) + 2 * np.std(density, axis=1)).astype(np.uint16)
		thresholds[np.where(thresholds < min_threshold)] = min_threshold
		self.threshold_matrix = thresholds

	def store_array(self, array, grain=100):
		query = PySQLPool.getNewQuery(mysql_db, commitOnEnd=True)
		query.executeMany('''INSERT INTO threshold(net,lng,lat,expect,reality) VALUES (%s,%s,%s,%s,0) ON DUPLICATE KEY UPDATE expect=VALUES(expect), reality=0;''', 
			list((x[0], x[1], x[2], array[x[0], x[1], x[2]]) for x in itertools.product(range(3), range(grain), range(grain))))

class GotMsg(threading.Thread):
	#def __init__(self):
	#	super(GotMsg, self).__init__()

	def run(self):
		while True:
			data = json.loads(redis_db.blpop('upd_queue')[1])
			x,y,n = online_cube_coords(data, BBOX)
			exec_mysql('''UPDATE threshold SET reality = reality + 1 WHERE lng = {} AND lat = {} AND net = {};'''.format(x, y, n))
			current_level = exec_mysql('''SELECT expect, reality FROM threshold WHERE lng = {} AND lat = {} AND net = {};'''.format(x, y, n))[0][0]
			if current_level['reality'] > current_level['expect']:
				exec_mysql('''INSERT INTO event_grains(net,lng,lat,hour,deviation) VALUES ({},{},{},{},{}) ON DUPLICATE KEY UPDATE deviation=VALUES(deviation);'''.format(
					n, x, y, int(math.floor(time.time()/3600)), float(current_level['reality'])/current_level['expect']))

class ClusterEvents(threading.Thread):
	def __init__(self, thres_nnd=1.8, thres_deviation=3):
		super(ClusterEvents, self).__init__()
		self.thresholds = (thres_nnd, thres_deviation)

	def run(self):
		while True:
			self.get_grains()
			self.filter_grains()
			self.create_candidates(self.cluster_grains())
			self.previous_candidates = redis_db.get('previous_candidates')
			if not self.previous_candidates:
				self.previous_candidates = {}
			else:
				self.previous_candidates = json.loads(self.previous_candidates)
			self.match_candidates()
			self.garbage_collector(delay=1)
			redis_db.set('previous_candidates', json.dumps(self.matched_candidates))
			pprint.pprint(self.matched_candidates)
			break
			time.sleep(60)

	def get_grains(self):
		self.data = exec_mysql('SELECT * FROM event_grains;')[0]

	def filter_grains(self):
		tree_data = [(x['net'], x['lng'], x['lat'], x['hour']) for x in self.data]
		tree = BallTree(np.array(tree_data))
		distances = tree.query(tree_data, k=2, return_distance=True)[0][:,1]
		self.clean_grains = []
		for i in range(len(self.data)):
			if self.data[i]['deviation'] >= self.thresholds[1] or distances[i] <= self.thresholds[0]:
				self.clean_grains.append([self.data[i]['net'], self.data[i]['lat'], self.data[i]['lng'], self.data[i]['hour']])

	def cluster_grains(self):
		cluster = DBSCAN(eps=self.thresholds[0], min_samples=1)
		clusters = cluster.fit_predict(np.array(self.clean_grains))
		return(clusters)

	def create_candidates(self, clusters):
			data = {(x['net'], x['lat'], x['lng'], x['hour']):x['deviation'] for x in self.data}
			self.event_candidates = {}
			for element in set(clusters):
				coords = [tuple(self.clean_grains[i]) for i in range(len(self.clean_grains)) if clusters[i] == element]
				weight = sum([data[x] for x in coords])
				self.event_candidates[str(uuid.uuid4())] = {'coords':coords, 'weight':weight, 'created':int(time.time()), 'updated':int(time.time())}

	def match_candidates(self):
		G = nx.Graph()
		candidates = copy.deepcopy(self.event_candidates)
		candidates.update(self.previous_candidates)
		for element in candidates.keys():
			G.add_node(element)
		for p1, p2 in itertools.product(self.event_candidates.keys(), self.previous_candidates.keys()):
			sim = self.candidates_similarity(self.event_candidates[p1],self.previous_candidates[p2])
			if sim > 0:
				G.add_edge(p1, p2, weight=sim)
		self.matched_candidates = {}
		data = {(x['net'], x['lat'], x['lng'], x['hour']):x['deviation'] for x in self.data}
		for match in nx.connected_components(G):
			match = tuple(match)
			if len(match) == 1 and match[0] in self.event_candidates.keys():
				self.matched_candidates[match[0]] = self.event_candidates[match[0]]
			elif len(match) == 2 and G[match[0]][match[1]]['weight'] == 1:
				self.matched_candidates[match[0]] = {'coords':candidates[match[0]]['coords'],
													 'weight':candidates[match[0]]['weight'],
													 'created': min((candidates[match[0]]['created'], candidates[match[1]]['created'])),
													 'updated': min((candidates[match[0]]['updated'], candidates[match[1]]['updated']))}
			else:
				self.matched_candidates[match[0]] = {'coords':list(set([tuple(x) for i in range(len(match)) for x in self.event_candidates[match[i]]['coords']])),
													 'created': min((self.event_candidates[match[0]]['created'], self.event_candidates[match[1]]['created'])),
													 'updated': max((self.event_candidates[match[0]]['updated'], self.event_candidates[match[1]]['updated']))}
													
				self.matched_candidates[match[0]]['weight'] = sum([data[x] for x in self.matched_candidates[match[0]]['coords']])

	def garbage_collector(self, delay = 60*60*24):
		current_time = int(time.time())
		for k in self.matched_candidates.keys():
			if current_time - int(self.matched_candidates[k]['updated']) > delay:
				to_clean = self.matched_candidates.pop(k)
				for coord in to_clean['coords']:
					exec_mysql('DELETE FROM event_grains WHERE net = {} AND lat = {} AND lng = {} AND hour = {};'.format(*coord))
				q = 'INSERT INTO events(start, end, weight, coordinates) VALUES ("{}", "{}", {}, "{}");'.format(
					datetime.datetime.fromtimestamp(min([x[3] for x in to_clean['coords']])).strftime('%Y-%m-%d %H:%M:%S'),
					datetime.datetime.fromtimestamp(max([x[3] for x in to_clean['coords']])).strftime('%Y-%m-%d %H:%M:%S'),
					to_clean['weight'],
					json.dumps(to_clean['coords'])
				)
				exec_mysql(q)

	def candidates_similarity(self, candidate1, candidate2):
		s1 = set([tuple(x) for x in candidate1['coords']])
		s2 = set([tuple(x) for x in candidate2['coords']])
		return float(len(s1.intersection(s2)))/len(s1.union(s2))

class EventDescriber():
	def __init__(self, grain = 100, bbox = BBOX):
		self.grain = grain
		self.bbox = BBOX

	def get_messages(self, coords):
		self.messages = []
		for coord in coords:
			coords_transformed = online_cell_coords(coord, self.bbox, self.grain)
			q = 'SELECT * FROM tweets WHERE network = "{}" AND lat >= {} AND lat <= {} AND lng >= {} AND lng <= {} AND tstamp >= "{}" AND tstamp <= "{}";'.format(
				coords_transformed['network'], coords_transformed['lat'][0], coords_transformed['lat'][1], coords_transformed['lng'][0], coords_transformed['lng'][1], coords_transformed['tstamp'][0], coords_transformed['tstamp'][1])
			data = exec_mysql(q)[0]
			print data

def exec_mysql(cmd):
	query = PySQLPool.getNewQuery(mysql_db, commitOnEnd=True)
	result = query.Query(cmd)
	return query.record, result

def cube_coords(req_item, density, bbox, min_date, grain=100):
	x,y,n = online_cube_coords(req_item, bbox, grain)
	d = (req_item['tstamp'] - min_date).days
	density[n,d,x,y] += 1
	return 1

def online_cube_coords(req_item, bbox, grain=100):
	x = math.floor(grain*(req_item['lng'] - bbox[0])/(bbox[2] - bbox[0]))
	if x >= grain: x = grain-1
	y = math.floor(grain*(req_item['lat'] - bbox[1])/(bbox[3] - bbox[1]))
	if y >= grain: y = grain-1
	n = NETS[req_item['network']]
	return x,y,n

def online_cell_coords(req_item, bbox, grain=100):
	minx = req_item[2]*(bbox[2] - bbox[0])/grain + bbox[0]
	maxx = (req_item[2]+1)*(bbox[2] - bbox[0])/grain + bbox[0]
	miny = req_item[1]*(bbox[3] - bbox[1])/grain + bbox[1]
	maxy = (req_item[1]+1)*(bbox[3] - bbox[1])/grain + bbox[1]
	minh = datetime.datetime.fromtimestamp(req_item[3]*3600).strftime('%Y-%m-%d %H:%M:%S')
	maxh = datetime.datetime.fromtimestamp((req_item[3]+1)*3600).strftime('%Y-%m-%d %H:%M:%S')
	net = INV_NETS[req_item[0]]
	return {'lng': (minx, maxx), 'lat': (miny, maxy), 'network': net, 'tstamp': (minh, maxh)}

#maxt = datetime.datetime.strptime('2015-07-17 00:00:00', '%Y-%m-%d %H:%M:%S')
#mint = datetime.datetime.strptime('2015-07-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#t = get_average_array(15, mint, maxt)
#store_array(t)
#ClusterEvents().start()

#(37.831226, 55.918149)
# 1434930120
# 23915502

#d = online_cube_coords({'text':'make plans #morning #moscow #summer #june #city  @ Набережная Воробьевых Гор', 'id':'10005607_342', 'lat':55.7267, 'lng':37.542, 'tstamp':'2015-06-21 23:42:58', 'network':'v'}, [float(x) for x in TW_LOCATIONS.split(',')])
#print d
print online_cell_coords([2, 46, 38, 398588], [float(x) for x in TW_LOCATIONS.split(',')])
e = EventDescriber()
e.get_messages([[2, 46, 38, 398588]])