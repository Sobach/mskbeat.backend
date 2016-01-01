#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK PULSE backend
# Number of separate utilities without common input

def drange(start, stop, step):
	r = start
	while r <= stop+step:
		yield r
		r += step

def get_circles_centers(bbox, radius=5000):
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
	return [x for x in product(x_marks[0::2], y_marks[0::2])] + [x for x in product(x_marks[1::2], y_marks[1::2])]

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