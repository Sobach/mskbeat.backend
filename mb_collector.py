#!/usr/bin/python
# -*- coding: utf-8 -*-
# Twitter geo tweets collector

# DATABASES
import redis
import PySQLPool
import MySQLdb

# NETWORKING
import requests
from TwitterAPI import TwitterAPI, TwitterRequestError, TwitterConnectionError

# OTHER
import threading
import json
from time import sleep
import datetime

# CONSTANTS
from settings import *

tw_api = TwitterAPI(TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN_KEY, TW_ACCESS_TOKEN_SECRET)
redis_db = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
PySQLPool.getNewPool().maxActiveConnections = 1
mysql_db = PySQLPool.getNewConnection(username=MYSQL_USER, password=MYSQL_PASSWORD, host=MYSQL_HOST, db=MYSQL_DB)
query = PySQLPool.getNewQuery(mysql_db, commitOnEnd=True)
query.Query('SET NAMES utf8mb4;')
query.Query('SET CHARACTER SET utf8mb4;')
query.Query('SET character_set_connection=utf8mb4;')

def get_media(entities, tw_id):
	media = []
	if 'media' in entities:
		for item in entities['media']:
			q = 'INSERT INTO media(tweet_id, url) VALUES ({}, "{}");'.format(
				tw_id, item['media_url_https'])
			exec_mysql(q)
	if 'urls' in entities:
		for url in entities['urls']:
			if url['expanded_url'].startswith('https://instagram.com/'):
				redis_db.rpush('ig_queue', json.dumps([tw_id, url['expanded_url']]))
	return media

def exec_mysql(cmd):
	query = PySQLPool.getNewQuery(mysql_db, commitOnEnd=True)
	query.Query(cmd)
	return query.record

class TwitterStreamThread(threading.Thread):
	def run(self):
		while True:
			try:
				stream = tw_api.request('statuses/filter', {'locations':'37.364307,55.558649,37.831226,55.918149'})
				for item in stream:
					if 'coordinates' in item and item['coordinates']:
						print datetime.datetime.now().isoformat(), item['coordinates']['coordinates'], item['text']
						q = 'INSERT IGNORE INTO tweets(id, text, lat, lng, tstamp, user) VALUES ({}, "{}", {}, {}, "{}", {});'.format(
							item['id_str'], 
							MySQLdb.escape_string(item['text'].encode('utf-8', 'replace')),
							item['coordinates']['coordinates'][1],
							item['coordinates']['coordinates'][0],
							datetime.datetime.strptime(item['created_at'][4:], '%b %d %H:%M:%S +0000 %Y'),
							item['user']['id_str']
							)
						exec_mysql(q)
						get_media(item['entities'], item['id_str'])
					elif 'disconnect' in item:
						event = item['disconnect']
						if event['code'] in [2,5,6,7]:
							raise Exception(event['reason'])
						else:
							break
			except TwitterRequestError as e:
				if e.status_code < 500:
					raise
				else:
					pass
			except TwitterConnectionError:
				pass

class InstagramThread(threading.Thread):
	def run(self):
		while True:
			data = json.loads(redis_db.blpop('ig_queue')[1])
			url = 'https://api.instagram.com/v1/media/shortcode/{}?access_token={}'.format(data[1].split('/')[4], IG_ACCESS_TOKEN)
			try:
				photo_data = requests.get(url).json()
				link = photo_data['data']['images']['standard_resolution']['url']
				print datetime.datetime.now().isoformat(), ' INSTAGRAM: ', link
			except:
				pass
			else:
				q = 'INSERT INTO media(tweet_id, url) VALUES ({}, "{}");'.format(data[0], link)
				exec_mysql(q)
			sleep(.5)

TwitterStreamThread().start()
InstagramThread().start()