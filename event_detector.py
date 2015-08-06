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

# CONSTANTS
from settings import *

# MATH
import numpy as np

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
	def run(self):
		bbox = [float(x) for x in TW_LOCATIONS.split(',')]
		while True:
			data = json.loads(redis_db.blpop('upd_queue')[1])
			x,y,n = online_cube_coords(data, bbox)
			q = 'UPDATE threshold SET reality = reality + 1 WHERE lng = {} AND lat = {} AND net = {};'.format(x, y, n)
			exec_mysql(q)
			current_level = exec_mysql('SELECT expect, reality FROM threshold WHERE lng = {} AND lat = {} AND net = {};'.format(x, y, n))[0][0]
			if current_level['reality'] > current_level['expect']:
				redis_db.rpush('event_grain', json.dumps({'lng':x, 'lat':y, 'net':n, 'hour':int(math.floor(time.time()/3600))}))

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

def get_average_array(hour, min_date=(datetime.datetime.now() - datetime.timedelta(days=14)), max_date=datetime.datetime.now(), grain=100):
	days = int((max_date - min_date).days) + 1
	bbox = [float(x) for x in TW_LOCATIONS.split(',')]
	q = 'SELECT lat, lng, tstamp, network FROM tweets WHERE HOUR(tstamp) = {} AND tstamp <= "{}" AND tstamp >= "{}" AND lng > {} AND lat > {} AND lng <  {} AND lat < {}'.format(hour, max_date, min_date, bbox[0], bbox[1], bbox[2], bbox[3])
	data = exec_mysql(q)[0]
	get_coords = np.vectorize(cube_coords, excluded=['density', 'bbox', 'min_date'], otypes=[np.uint8])
	density = np.zeros((3, days, grain, grain), dtype=np.uint16)
	get_coords(req_item=np.array(data), density=density, bbox=bbox, min_date=min_date)
	thresholds = (np.mean(density, axis=1) + 2 * np.std(density, axis=1)).astype(np.uint16)
	return thresholds

def store_array(array):
	query = PySQLPool.getNewQuery(mysql_db, commitOnEnd=True)
	for n in range(3):
		for i in range(100):
			for j in range(100):
				query.Query('INSERT INTO threshold(net,lng,lat,expect,reality) VALUES ({},{},{},{},0) ON DUPLICATE KEY UPDATE expect={}, reality=0;'.format(
					n, i, j, array[n,i,j], array[n,i,j], n, i, j))

t = get_average_array(15)
print np.sum(t)
store_array(t)