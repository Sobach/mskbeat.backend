#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# DATABASES
import redis
import PySQLPool
import MySQLdb

# NETWORKING
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
from requests.packages.urllib3.exceptions import ReadTimeoutError, ProtocolError
import requests
import socket
import ssl
from TwitterAPI import TwitterAPI, TwitterRequestError, TwitterConnectionError

# INCLUDED IN PYTHON BY DEFAULT
import threading
import json
import time
import datetime

# OTHER
import npyscreen  # UI

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

class TwitterStreamThread(threading.Thread):
	def run(self):
		while True:
			try:
				stream = tw_api.request('statuses/filter', {'locations':TW_LOCATIONS})
				for item in stream:
					if 'coordinates' in item and item['coordinates']:
						q = 'INSERT IGNORE INTO tweets(id, text, lat, lng, tstamp, user, network, iscopy) VALUES ("{}", "{}", {}, {}, "{}", {}, "t", {});'.format(
							item['id_str'], 
							MySQLdb.escape_string(item['text'].encode('utf-8', 'replace')),
							item['coordinates']['coordinates'][1],
							item['coordinates']['coordinates'][0],
							datetime.datetime.strptime(item['created_at'][4:], '%b %d %H:%M:%S +0000 %Y'),
							item['user']['id_str'],
							int('Instagram' in item['source'])
							)
						exec_mysql(q)
						redis_db.set('tw_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))
						get_twitter_media(item['entities'], item['id_str'])
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

class InstagramHelperThread(threading.Thread):
	def run(self):
		while True:
			data = json.loads(redis_db.blpop('ig_queue')[1])
			try:
				url = 'https://api.instagram.com/v1/media/shortcode/{}?access_token={}'.format(data[1].split('/')[4], IG_ACCESS_TOKEN_1)
				photo_data = requests.get(url, stream=False, timeout=10)
			except (IndexError, ConnectionError, ProtocolError, ReadTimeout, ReadTimeoutError, SSLError, ssl.SSLError, socket.error) as e:
				pass
			else:
				if photo_data.ok:
					link = photo_data.json()['data']['images']['standard_resolution']['url']
					q = 'INSERT INTO media(tweet_id, url) VALUES ("{}", "{}");'.format(data[0], link)
					exec_mysql(q)
					redis_db.set('media_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))
			time.sleep(1)

class InstagramStreamThread(threading.Thread):
	def run(self):
		last_time = [int(time.time()) - 60]*len(IG_LOCATIONS)
		while True:
			medialist = []
			for i in range(len(IG_LOCATIONS)):
				url = 'https://api.instagram.com/v1/media/search?lat={}&lng={}&min_timestamp={}&distance=5000&access_token={}'.format(
					IG_LOCATIONS[i][1], IG_LOCATIONS[i][0], last_time[i], IG_ACCESS_TOKEN_2)
				try:
					resp = requests.get(url, stream=False, timeout=10)
				except (ConnectionError, ProtocolError, ReadTimeout, ReadTimeoutError, SSLError, ssl.SSLError, socket.error) as e:
					pass
				else:
					if resp.ok:
						last_time[i] = int(time.time())
						medialist = get_ig_data(resp.json(), medialist)
				time.sleep(3)

class VKCheckinsThread(threading.Thread):
	def run(self):
		from_time = int(time.time())-60
		f_time = int(time.time())-60
		while True:
			url = 'https://api.vk.com/method/execute.beatFunc'
			params = {'from_time':from_time, 'lat':VK_LOCATIONS['lat'], 'lng':VK_LOCATIONS['lng'], 'access_token':VK_ACCESS_TOKEN, 'v':5.34}
			f_time = int(time.time())
			try:
				resp = requests.post(url, data=params, stream=False, timeout=10)
			except (ConnectionError, ProtocolError, ReadTimeout, ReadTimeoutError, SSLError, ssl.SSLError, socket.error) as e:
				pass
			else:
				if resp.ok:
					data = resp.json()
					redis_db.set('vk_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))
					get_vk_data(data)
					from_time = f_time
				time.sleep(2)

class MskBeatUI(npyscreen.FormBaseNew):
	def create(self):
		self.timer = self.add(npyscreen.Textfield, value=' ' * 31 + datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'), editable=False,)
		self.add(npyscreen.Textfield, value= "")
		self.tw_stat = self.add(npyscreen.TitleText, name = "Twitter:", value="0", editable=False,)
		self.vk_stat = self.add(npyscreen.TitleText, name = "VKontakte:", value="0", editable=False,)
		self.ig_stat = self.add(npyscreen.TitleText, name = "Instagram:", value="0", editable=False,)
		self.media_stat = self.add(npyscreen.TitleText, name = "Media links:", value="0", editable=False,)

	def while_editing(self,arg):
		while True:
			self.timer.value = ' ' * 31 + datetime.datetime.now().strftime('%H:%M:%S %d %b %Y')
			self.timer.display()
			self.tw_stat.value = '{:10,d}'.format(exec_mysql('SELECT COUNT(*) FROM tweets WHERE network="t";')[0][0].values()[0]) + '     ' + redis_db.get('tw_last')
			self.tw_stat.display()
			self.vk_stat.value = '{:10,d}'.format(exec_mysql('SELECT COUNT(*) FROM tweets WHERE network="v";')[0][0].values()[0]) + '     ' + redis_db.get('vk_last')
			self.vk_stat.display()
			self.ig_stat.value = '{:10,d}'.format(exec_mysql('SELECT COUNT(*) FROM tweets WHERE network="i";')[0][0].values()[0]) + '     ' + redis_db.get('ig_last')
			self.ig_stat.display()
			self.media_stat.value = '{:10,d}'.format(exec_mysql('SELECT COUNT(*) FROM media;')[0][0].values()[0]) + '     ' + redis_db.get('media_last')
			self.media_stat.display()
			time.sleep(1)

class MskBeatApp(npyscreen.NPSAppManaged):
	def onStart(self):
		self.addForm('MAIN', MskBeatUI, name="MSK.PULSE BackEnd v.1.0.0 <J.A.R.V.I.S.>",)

def get_twitter_media(entities, tw_id):
	media = []
	if 'media' in entities:
		for item in entities['media']:
			q = 'INSERT INTO media(tweet_id, url) VALUES ("{}", "{}");'.format(
				tw_id, item['media_url_https'])
			exec_mysql(q)
			redis_db.set('media_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))
	if 'urls' in entities:
		for url in entities['urls']:
			if url['expanded_url'].startswith('https://instagram.com/'):
				redis_db.rpush('ig_queue', json.dumps([tw_id, url['expanded_url']]))
	return media

def exec_mysql(cmd):
	query = PySQLPool.getNewQuery(mysql_db, commitOnEnd=True)
	result = query.Query(cmd)
	return query.record, result

def get_vk_data(data):
	try:
		wall_posts = {'{}_{}'.format(x['from_id'], x['id']): x for x in data['response']['wall']}
	except:
		pass
	for item in data['response']['places']['items']:
		lat = None
		lng = None
		if item['latitude'] > 0 and item['longitude'] > 0:
			lat = item['latitude']
			lng = item['longitude']
			iscopy = 0
		elif 'geo' in wall_posts[item['id']]:
			coordinates = wall_posts[item['id']]['geo']['coordinates'].split(' ')
			lat = float(coordinates[0])
			lng = float(coordinates[1])
			iscopy = 1
		else:
			continue
		if 'text' in item:
			text = item['text']
		else:
			text = ''
		if lat and lng:
			q = 'INSERT IGNORE INTO tweets(id, text, lat, lng, tstamp, user, network, iscopy) VALUES ("{}", "{}", {}, {}, "{}", {}, "v", {});'.format(
				item['id'], 
				MySQLdb.escape_string(text.encode('utf-8', 'replace')),
				lat,
				lng,
				datetime.datetime.fromtimestamp(int(item['date'])),
				item['user_id'],
				iscopy
				)
			exec_mysql(q)
			if 'attachments' in wall_posts[item['id']] and 'photo' in wall_posts[item['id']]['attachments'][0] and 'photo_807' in wall_posts[item['id']]['attachments'][0]['photo']:
				q = 'INSERT INTO media(tweet_id, url) VALUES ("{}", "{}");'.format(
				item['id'], wall_posts[item['id']]['attachments'][0]['photo']['photo_807'])
			exec_mysql(q)
			redis_db.set('media_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))

def get_ig_data(data, medialist):
	for item in data['data']:
		if item['id'] in medialist:
			continue
		medialist.append(item['id'])
		try:
			text = item['caption']['text']
		except:
			text = ''
		try:
			lat = item['location']['latitude']
			lng = item['location']['longitude'] 
			user = item['user']['id']
			media_url = item['images']['standard_resolution']['url']
		except:
			pass
		else:
			q = 'INSERT IGNORE INTO tweets(id, text, lat, lng, tstamp, user, network, iscopy) VALUES ("{}", "{}", {}, {}, "{}", {}, "i", 0);'.format(
				item['id'], 
				MySQLdb.escape_string(text.encode('utf-8', 'replace')),
				lat, lng,
				datetime.datetime.fromtimestamp(int(item['created_time'])),
				user)
			exec_mysql(q)
			q = 'INSERT IGNORE INTO media(tweet_id, url) VALUES ("{}", "{}");'.format(
				item['id'], media_url)
			exec_mysql(q)
	redis_db.set('ig_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))
	redis_db.set('media_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))
	return medialist

def prepare_ui():
	redis_db.set('tw_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))
	redis_db.set('vk_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))
	redis_db.set('ig_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))
	redis_db.set('media_last', datetime.datetime.now().strftime('%H:%M:%S %d %b %Y'))

def main():
	prepare_ui()
	App = MskBeatApp()
	TwitterStreamThread().start()
	InstagramHelperThread().start()
	InstagramStreamThread().start()
	VKCheckinsThread().start()
	App.run()

if __name__ == '__main__':
	main()