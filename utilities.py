#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK PULSE backend
# Number of separate utilities without common import

def drange(start, stop, step):
	r = start
	while r <= stop+step:
		yield r
		r += step

def get_circles_centers(bbox, radius=5000, polygons=None):
	from math import radians, cos
	from itertools import product
	lat = radians(max([abs(bbox[1]), abs(bbox[3])]))
	# Calculate the length of a degree of latitude and longitude in meters
	latlen = 111132.92 - (559.82 * cos(2 * lat)) + (1.175 * cos(4 * lat)) + (-0.0023 * cos(6 * lat))
	longlen = (111412.84 * cos(lat)) - (93.5 * cos(3 * lat)) + (0.118 * cos(5 * lat))
	radius_x = radius/longlen
	radius_y = radius/latlen
	x_marks = [x for x in drange(bbox[0], bbox[2], radius_x)]
	y_marks = [y for y in drange(bbox[1], bbox[3], radius_y)]
	centers = [x for x in product(x_marks[0::2], y_marks[0::2])] + [x for x in product(x_marks[1::2], y_marks[1::2])]
	if polygons:
		cleaned_centers = []
		from shapely.geometry import Point
		from shapely.affinity import scale
		for center in centers:
			circle = scale(Point(center[0], center[1]).buffer(1), xfact=radius_x, yfact=radius_y)
			if polygons.intersects(circle):
				cleaned_centers.append(center)
		centers = cleaned_centers
	return centers

def get_locations(bfile = None, bbox = None, filter_centers = True):
	if not bfile and not bbox:
		raise BaseException('Neither bounds geojson file, nor bounding box are specified.')
	if bfile:
		from json import load
		from shapely.geometry import shape
		with open(bfile, 'rb') as f:
			BOUNDS = shape(load(f))
		BBOX = list(BOUNDS.bounds)
	else:
		from shapely.geometry import Polygon
		BBOX = bbox
		BOUNDS = Polygon([(BBOX[0], BBOX[1]), (BBOX[0], BBOX[3]), (BBOX[2], BBOX[3]), (BBOX[2], BBOX[1]), (BBOX[0], BBOX[1])])
	if filter_centers:
		polys = BOUNDS
	else:
		polys = None
	TW_LOCATIONS = ','.join([str(x) for x in BBOX])
	VK_LOCATIONS = get_circles_centers(BBOX, radius=4000, polygons=polys)
	IG_LOCATIONS = get_circles_centers(BBOX, radius=5000, polygons=polys)
	return BOUNDS, BBOX, TW_LOCATIONS, VK_LOCATIONS, IG_LOCATIONS

def exec_mysql(cmd, connection):
	from PySQLPool import getNewQuery
	query = getNewQuery(connection, commitOnEnd=True)
	result = query.Query(cmd)
	return query.record, result

def get_mysql_con():
	from PySQLPool import getNewPool, getNewConnection, getNewQuery
	from settings import MYSQL_USER, MYSQL_PASSWORD, MYSQL_HOST, MYSQL_DB
	getNewPool().maxActiveConnections = 1
	mysql_db = getNewConnection(username=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, db=MYSQL_DB)
	query = getNewQuery(mysql_db, commitOnEnd=True)
	query.Query('SET NAMES utf8mb4;')
	query.Query('SET CHARACTER SET utf8mb4;')
	query.Query('SET character_set_connection=utf8mb4;')
	return mysql_db

def create_mysql_tables():
	con = get_mysql_con()
	tabs = [
		"""CREATE TABLE `tweets` (`id` varchar(40) NOT NULL DEFAULT '', `text` text, `lat` float DEFAULT NULL, `lng` float DEFAULT NULL, `tstamp` datetime DEFAULT NULL, `user` bigint(40) DEFAULT NULL, `network` tinyint(4) DEFAULT NULL, `iscopy` tinyint(1) DEFAULT NULL, PRIMARY KEY (`id`), KEY `net` (`network`), KEY `latitude` (`lat`), KEY `longitude` (`lng`), KEY `time` (`tstamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""",
		"""CREATE TABLE `media` (`id` int(11) unsigned NOT NULL AUTO_INCREMENT, `tweet_id` varchar(40) DEFAULT NULL, `url` varchar(250) DEFAULT NULL, PRIMARY KEY (`id`), KEY `tweet_id` (`tweet_id`)) ENGINE=InnoDB AUTO_INCREMENT=2128971 DEFAULT CHARSET=utf8mb4;""",
		"""CREATE TABLE `events` (`id` varchar(20) NOT NULL DEFAULT '', `start` datetime DEFAULT NULL, `end` datetime DEFAULT NULL, `vocabulary` text, `core` text, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""",
		"""CREATE TABLE `event_msgs` (`id` int(11) NOT NULL AUTO_INCREMENT, `msg_id` varchar(40) DEFAULT NULL, `event_id` varchar(20) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;""",
	]
	for tab in tabs:
		exec_mysql(tab, con)