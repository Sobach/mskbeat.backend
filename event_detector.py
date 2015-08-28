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

class ThresholdUpdaterThread(threading.Thread):
	def run(self):
		while True:
			d = datetime.datetime.now()
			threshold_matrix = get_average_array(d.hour)
			store_array(threshold_matrix)
			wakeup = datetime.datetime.strptime(d.strftime('%Y-%m-%d ')+str(d.hour+1)+':00:00', '%Y-%m-%d %H:%M:%S')
			time.sleep((wakeup - datetime.datetime.now()).seconds)

class GotMsg(threading.Thread):
	def __init__(self):
		super(GotMsg, self).__init__()
		self.bbox = [float(x) for x in TW_LOCATIONS.split(',')]

	def run(self):
		while True:
			data = json.loads(redis_db.blpop('upd_queue')[1])
			x,y,n = online_cube_coords(data, self.bbox)
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
			data = exec_mysql('SELECT * FROM event_grains;')[0]
			tree_data = [(x['net'], x['lng'], x['lat'], x['hour']) for x in data]
			tree = BallTree(np.array(tree_data))
			distances = tree.query(tree_data, k=2, return_distance=True)[0][:,1]
			filtered_tree = []
			for i in range(len(data)):
				if data[i]['deviation'] >= self.thresholds[1] or distances[i] <= self.thresholds[0]:
					filtered_tree.append([data[i]['net'], data[i]['lat'], data[i]['lng'], data[i]['hour']])
					#filtered_tree[-1]['nnd'] = distances[i]
			cluster = DBSCAN(eps=self.thresholds[0], min_samples=1)
			clusters = cluster.fit_predict(np.array(filtered_tree))
			data = {(x['net'], x['lat'], x['lng'], x['hour']):x['deviation'] for x in data}
			event_candidates = []
			for element in set(clusters):
				coords = [tuple(filtered_tree[i]) for i in range(len(filtered_tree)) if clusters[i] == element]
				weight = sum([data[x] for x in coords])
				event_candidates.append({'coords':coords, 'weight':weight})
			pprint.pprint(event_candidates)
			break
			time.sleep(60)

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
	x = round(grain*(req_item['lng'] - bbox[0])/(bbox[2] - bbox[0]), 0)
	if x >= grain: x = grain-1
	y = round(grain*(req_item['lat'] - bbox[1])/(bbox[3] - bbox[1]), 0)
	if y >= grain: y = grain-1
	if req_item['network'] == 'i': n = 0
	elif req_item['network'] == 't': n = 1
	else: n = 2
	return x,y,n

def get_average_array(hour, min_date=(datetime.datetime.now() - datetime.timedelta(days=14)), max_date=datetime.datetime.now(), grain=100, min_threshold=3):
	days = int((max_date - min_date).days) + 1
	bbox = [float(x) for x in TW_LOCATIONS.split(',')]
	q = 'SELECT lat, lng, tstamp, network FROM tweets WHERE HOUR(tstamp) = {} AND tstamp <= "{}" AND tstamp >= "{}" AND lng > {} AND lat > {} AND lng <  {} AND lat < {}'.format(hour, max_date, min_date, bbox[0], bbox[1], bbox[2], bbox[3])
	data = exec_mysql(q)[0]
	get_coords = np.vectorize(cube_coords, excluded=['density', 'bbox', 'min_date'], otypes=[np.uint8])
	density = np.zeros((3, days, grain, grain), dtype=np.uint16)
	get_coords(req_item=np.array(data), density=density, bbox=bbox, min_date=min_date)
	thresholds = (np.mean(density, axis=1) + 2 * np.std(density, axis=1)).astype(np.uint16)
	thresholds[np.where(thresholds < min_threshold)] = min_threshold
	return thresholds

def store_array(array, grain=100):
	query = PySQLPool.getNewQuery(mysql_db, commitOnEnd=True)
	query.executeMany('''INSERT INTO threshold(net,lng,lat,expect,reality) VALUES (%s,%s,%s,%s,0) ON DUPLICATE KEY UPDATE expect=VALUES(expect), reality=0;''', 
		list((x[0], x[1], x[2], array[x[0], x[1], x[2]]) for x in itertools.product(range(3), range(grain), range(grain))))

#maxt = datetime.datetime.strptime('2015-07-17 00:00:00', '%Y-%m-%d %H:%M:%S')
#mint = datetime.datetime.strptime('2015-07-01 00:00:00', '%Y-%m-%d %H:%M:%S')
#t = get_average_array(15, mint, maxt)
#store_array(t)
ClusterEvents().start()