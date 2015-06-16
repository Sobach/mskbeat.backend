#!/usr/bin/python
# -*- coding: utf-8 -*-
# Collecting missed dates for tweets

# DATABASES
import PySQLPool
import MySQLdb

# NETWORKING
from TwitterAPI import TwitterAPI

import datetime
from time import sleep
# CONSTANTS
from settings import *

tw_api = TwitterAPI(TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN_KEY, TW_ACCESS_TOKEN_SECRET)
PySQLPool.getNewPool().maxActiveConnections = 1
mysql_db = PySQLPool.getNewConnection(username=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, db=MYSQL_DB)
query = PySQLPool.getNewQuery(mysql_db, commitOnEnd=True)
query.Query('SET NAMES utf8mb4;')
query.Query('SET CHARACTER SET utf8mb4;')
query.Query('SET character_set_connection=utf8mb4;')

while True:
	no_answer = True
	query.Query('SELECT * FROM tweets WHERE ISNULL(tstamp) LIMIT 50;')
	ids = ','.join([str(x['id']) for x in query.record])
	r = tw_api.request('statuses/lookup', {'id':ids})
	for item in r:
		print item['id'], item['created_at']
		query.Query('UPDATE tweets SET tstamp="{}" WHERE id={}'.format(datetime.datetime.strptime(item['created_at'][4:], '%b %d %H:%M:%S +0000 %Y'), item['id']))
		no_answer = False
	if no_answer:
		break
	sleep(15)