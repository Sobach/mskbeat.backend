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
	query.Query('SELECT * FROM tweets WHERE ISNULL(tstamp) LIMIT 50;')
	tweet_ids = query.record
	if len(tweets_ids) == 0:
		break
	ids = ','.join([str(x['id']) for x in tweet_ids])
	r = tw_api.request('statuses/lookup', {'id':ids})
	for item in r:
		print item['id'], item['created_at']
		query.Query('UPDATE tweets SET tstamp="{}" WHERE id={}'.format(datetime.datetime.strptime(item['created_at'][4:], '%b %d %H:%M:%S +0000 %Y'), item['id']))
		tweet_ids.pop(tweet_ids.index(item['id']))
	for tid in tweets_ids:
		print tid, 'Not found'
		query.Query('UPDATE tweets SET tstamp="2000-01-01 00:00:00" WHERE id={}'.format(tid))
	sleep(15)