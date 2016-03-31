#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# SYSTEM
from threading import Thread
from json import dumps as jdumps, loads as jloads
from time import sleep, time, mktime
from datetime import datetime
from pickle import dumps as pdumps
from logging import basicConfig, warning, error, WARNING

# DATABASE
from redis import StrictRedis
from MySQLdb import escape_string

# NETWORK
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
from requests.packages.urllib3.exceptions import ReadTimeoutError, ProtocolError
from OpenSSL.SSL import SysCallError
from requests import get, post
from socket import error as soc_error
from ssl import SSLError as ssl_SSLError
from TwitterAPI import TwitterAPI, TwitterRequestError, TwitterConnectionError

# MATH
from shapely.geometry import Point

# SELF IMPORT
from settings import REDIS_HOST, REDIS_PORT, REDIS_DB, TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN_KEY, TW_ACCESS_TOKEN_SECRET, IG_ACCESS_TOKEN, VK_ACCESS_TOKEN, BOUNDS, TW_LOCATIONS, VK_LOCATIONS, IG_LOCATIONS, TIME_SLIDING_WINDOW
from utilities import get_mysql_con, exec_mysql

class TwitterStreamThread(Thread):
	"""
	Twitter collector class. Inherits from threading.Thread, so can be used in parallel with other collector classes.
	Requires TW_LOCATIONS, TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN_KEY, and TW_ACCESS_TOKEN_SECRET constants to get access token.
	If filter_bounds is True, requires BOUNDS constant.
	Object dumps messages to MySQL, saves it to Redis with expiration in TIME_SLIDING_WINDOW seconds, and updates statistics:tw_last key in Redis.
	Additionaly, if message was shared from Instagram, it's url is being pushed to queue:instagram key in Redis.
	"""

	def __init__(self, mysql_con, redis_con, filter_bounds = True):
		"""
		Initialization.

		Args:
			mysql_con (PySQLPoolConnection): MySQL connection Object
			redis_con (StrictRedis): RedisDB connection Object
			filter_bounds (bool): whether to drop surplus messages outside the bounds
		"""
		Thread.__init__(self)
		self.mysql = mysql_con
		self.redis = redis_con
		self.tw_api = TwitterAPI(TW_CONSUMER_KEY, TW_CONSUMER_SECRET, TW_ACCESS_TOKEN_KEY, TW_ACCESS_TOKEN_SECRET)
		if filter_bounds:
			self.city_polygon = BOUNDS

	def run(self):
		"""
		Infinity loop for running twitter collector.
		Used recomendations for error handling to make it loop for ever.
		"""
		while True:
			try:
				stream = self.tw_api.request('statuses/filter', {'locations':TW_LOCATIONS}).get_iterator()
				for item in stream:
					if 'coordinates' in item and item['coordinates']:
						if self.city_polygon and self.city_polygon.disjoint(Point(item['coordinates']['coordinates'][0],item['coordinates']['coordinates'][1])):
							continue
						q = 'INSERT IGNORE INTO tweets(id, text, lat, lng, tstamp, user, network, iscopy) VALUES ("{}", "{}", {}, {}, "{}", {}, 1, {});'.format(
							item['id_str'], 
							escape_string(item['text'].encode('utf-8', 'replace')),
							item['coordinates']['coordinates'][1],
							item['coordinates']['coordinates'][0],
							datetime.strptime(item['created_at'][4:], '%b %d %H:%M:%S +0000 %Y'),
							item['user']['id_str'],
							int('Instagram' in item['source'])
							)
						exec_mysql(q, self.mysql)
						if 'Instagram' not in item['source']:
							warning(u"Twitter data source: {}".format(item['source']))
						message = {
							'id':item['id_str'], 
							'lat':item['coordinates']['coordinates'][1], 
							'lng':	item['coordinates']['coordinates'][0], 
							'tstamp': int(mktime(datetime.strptime(item['created_at'][4:], '%b %d %H:%M:%S +0000 %Y').timetuple())), 
							'network': 1
						}
						self.redis.hmset("message:{}".format(message['id']), message)
						self.redis.expire("message:{}".format(message['id']), int(TIME_SLIDING_WINDOW))
						self.redis.set('statistics:tw_last', datetime.now().strftime('%H:%M:%S %d %b %Y'))
						self.get_twitter_media(item['entities'], item['id_str'])
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
					error(e)
			except TwitterConnectionError as e:
				error(e)

	def get_twitter_media(self, entities, tw_id):
		"""
		Method is used to extract media data from tweets. Called from infinity loop. 
		In case, if url contains Instagram photo (not shown in 'media'), pushes data to queue:instagram Redis key.
		"""
		if 'media' in entities:
			for item in entities['media']:
				q = 'INSERT INTO media(tweet_id, url) VALUES ("{}", "{}");'.format(tw_id, item['media_url_https'])
				exec_mysql(q, self.mysql)
		if 'urls' in entities:
			for url in entities['urls']:
				if 'instagram.com' in url['expanded_url']:
					self.redis.rpush('queue:instagram', jdumps([tw_id, url['expanded_url']]))

class InstagramHelperThread(Thread):
	"""
	Instagram media collector assistant. Inherits from threading.Thread, so can be used in parallel with other collector classes.
	Requires IG_ACCESS_TOKEN constants to get access token.
	Object gets instagram links from queue:instagram Redis key and gets all the data from the link.
	Saves data to media MySQL table.
	"""

	def __init__(self, mysql_con, redis_con):
		"""
		Initialization.

		Args:
			mysql_con (PySQLPoolConnection): MySQL connection Object
			redis_con (StrictRedis): RedisDB connection Object
		"""
		Thread.__init__(self)
		self.mysql = mysql_con
		self.redis = redis_con

	def run(self):
		"""
		Infinity loop: wait for any new item in queue:instagram Redis list, pop it and process.
		Wait for 2 seconds, go to the begining.
		"""
		while True:
			data = jloads(self.redis.blpop('queue:instagram')[1])
			try:
				url = 'https://api.instagram.com/v1/media/shortcode/{}?access_token={}'.format(data[1].split('/')[4], IG_ACCESS_TOKEN)
				photo_data = get(url, stream=False, timeout=10)
			except (IndexError, ConnectionError, ProtocolError, ReadTimeout, ReadTimeoutError, SSLError, ssl_SSLError, soc_error, SysCallError) as e:
				error(e)
			else:
				if photo_data.ok:
					link = photo_data.json()['data']['images']['standard_resolution']['url']
					q = 'INSERT INTO media(tweet_id, url) VALUES ("{}", "{}");'.format(data[0], link)
					exec_mysql(q, self.mysql)
			sleep(2)

class InstagramStreamThread(Thread):
	"""
	Instagram collector class. Deals with Instagram API. Inherits from threading.Thread, so can be used in parallel with other collector classes.
	Requires IG_LOCATIONS, IG_ACCESS_TOKEN constants.	If filter_bounds is True, requires BOUNDS constant.
	Object dumps messages to MySQL, saves it to Redis with expiration in TIME_SLIDING_WINDOW seconds, and updates statistics:ig_last key in Redis.
	"""

	def __init__(self, mysql_con, redis_con, filter_bounds = True):
		"""
		Initialization.

		Args:
			mysql_con (PySQLPoolConnection): MySQL connection Object
			redis_con (StrictRedis): RedisDB connection Object
			filter_bounds (bool): whether to drop surplus messages outside the bounds
		"""
		Thread.__init__(self)
		self.mysql = mysql_con
		self.redis = redis_con
		self.last_time = [int(time()) - 60]*len(IG_LOCATIONS)
		if filter_bounds:
			self.city_polygon = BOUNDS

	def run(self):
		"""
		Infinity loop for running Instagram collector.
		Alternately checks every location from IG_LOCATIONS, and saves last check timestamp.
		"""
		while True:
			medialist = []
			for i in range(len(IG_LOCATIONS)):
				api_time = int(time())
				url = 'https://api.instagram.com/v1/media/search?lat={}&lng={}&min_timestamp={}&distance=5000&access_token={}'.format(
					IG_LOCATIONS[i][1], IG_LOCATIONS[i][0], self.last_time[i], IG_ACCESS_TOKEN)
				try:
					resp = get(url, stream=False, timeout=10)
				except (ConnectionError, ProtocolError, ReadTimeout, ReadTimeoutError, SSLError, ssl_SSLError, soc_error, SysCallError) as e:
					error(e)
				else:
					if resp.ok:
						redis_db.set('statistics:ig_last', datetime.now().strftime('%H:%M:%S %d %b %Y'))
						self.last_time[i] = api_time
						medialist = self.get_ig_data(resp.json(), medialist)
				sleep(2)

	def get_ig_data(self, data, medialist):
		"""
		Method for parsing data, collected from Instagram search API endpoint.
		Looks for coordinates, text, and other attributes for every message.
		Dumps message to MySQL and Redis with expiration, updates statistics.
		List medialist is used to minimize overlaping for multiple locations.
		"""
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
				if self.city_polygon and self.city_polygon.disjoint(Point(lng,lat)):
					continue
				q = '''INSERT IGNORE INTO tweets(id, text, lat, lng, tstamp, user, network, iscopy) VALUES ("{}", "{}", {}, {}, "{}", {}, 2, 0);'''.format(
					item['id'], 
					escape_string(text.encode('utf-8', 'replace')),
					lat, lng,
					datetime.fromtimestamp(int(item['created_time'])),
					user)
				exec_mysql(q, self.mysql)
				message = {
					'id':item['id'], 
					'lat':lat, 
					'lng':lng, 
					'tstamp': 	int(mktime(datetime.fromtimestamp(int(item['created_time'])).timetuple())), 
					'network':2
				}
				self.redis.hmset("message:{}".format(message['id']), message)
				self.redis.expire("message:{}".format(message['id']), int(TIME_SLIDING_WINDOW))
				q = 'INSERT IGNORE INTO media(tweet_id, url) VALUES ("{}", "{}");'.format(
					item['id'], media_url)
				exec_mysql(q, self.mysql)
		return medialist

class VKontakteStreamThread(Thread):
	"""
	VKontakte collector class. Deals with VK API. Inherits from threading.Thread, so can be used in parallel with other collector classes.
	Requires VK_LOCATIONS, VK_ACCESS_TOKEN constants.	If filter_bounds is True, requires BOUNDS constant.
	Object dumps messages to MySQL, saves it to Redis with expiration in TIME_SLIDING_WINDOW seconds, and updates statistics:ig_last key in Redis.
	"""

	def __init__(self, mysql_con, redis_con, filter_bounds = True):
		"""
		Initialization.

		Args:
			mysql_con (PySQLPoolConnection): MySQL connection Object
			redis_con (StrictRedis): RedisDB connection Object
			filter_bounds (bool): whether to drop surplus messages outside the bounds
		"""
		Thread.__init__(self)
		self.mysql = mysql_con
		self.redis = redis_con
		self.last_time = [int(time())-60]*len(VK_LOCATIONS)
		if filter_bounds:
			self.city_polygon = BOUNDS

	def run(self):
		"""
		Infinity loop for running VKontakte collector.
		Alternately checks every location from VK_LOCATIONS, and saves last check timestamp.
		"""
		while True:
			medialist = []
			for i in range(len(VK_LOCATIONS)):
				api_time = int(time())
				url = 'https://api.vk.com/method/execute.beatFunc'
				params = {'from_time':self.last_time[i]-1, 'lat':VK_LOCATIONS[i][1], 'lng':VK_LOCATIONS[i][0], 'access_token':VK_ACCESS_TOKEN, 'v':5.42}
				try:
					resp = post(url, data=params, stream=False, timeout=15)
				except (ConnectionError, ProtocolError, ReadTimeout, ReadTimeoutError, SSLError, ssl_SSLError, soc_error, SysCallError) as e:
					pass
				else:
					if resp.ok:
						data = resp.json()
						if 'response' in data.keys():
							self.redis.set('statistics:vk_last', datetime.now().strftime('%H:%M:%S %d %b %Y'))
							medialist = self.get_vk_data(data, medialist)
							self.last_time[i] = api_time
					sleep(2)

	def get_vk_data(self, data, medialist):
		"""
		Method for parsing data, collected from VK API  custom function endpoint.
		Looks for coordinates, text, and other attributes for every message.
		Dumps message to MySQL and Redis with expiration, updates statistics.
		List medialist is used to minimize overlaping for multiple locations.
		Stored function:
		---
		var places = API.places.getCheckins({
			"latitude":Args.lat,
			"longitude":Args.lng,
			"count":100,
			"timestamp":Args.from_time
		});

		if(places["items"].length == 0){
			return {"wall":[], "places":places};
		}
		var wallIds = [];
		var i = 0;
		while(i < places["items"].length){
			wallIds = wallIds + [places["items"][i]["id"]];
			i = i + 1;
		}
		var walls = API.wall.getById({"posts": wallIds});
		return {"wall":walls, "places":places};
		---
		"""
		try:
			wall_posts = {'{}_{}'.format(x['from_id'], x['id']): x for x in data['response']['wall']}
		except:
			pass
		if 'response' not in data or 'places' not in data['response'] or 'items' not in data['response']['places']:
			return medialist
		for item in data['response']['places']['items']:
			if item['id'] in medialist or item['id'] not in wall_posts.keys():
				continue
			medialist.append(item['id'])
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
				if self.city_polygon and self.city_polygon.disjoint(Point(lng,lat)):
					continue
				q = 'INSERT IGNORE INTO tweets(id, text, lat, lng, tstamp, user, network, iscopy) VALUES ("{}", "{}", {}, {}, "{}", {}, 3, {});'.format(
					item['id'], 
					escape_string(text.encode('utf-8', 'replace')),
					lat,
					lng,
					datetime.fromtimestamp(int(item['date'])),
					item['user_id'],
					iscopy
					)
				exec_mysql(q, self.mysql)
				message = {
					'id':item['id'],
					'lat':lat,
					'lng':lng,
					'tstamp': int(mktime(datetime.fromtimestamp(int(item['date'])).timetuple())),
					'network':3, 
					}
				self.redis.hmset("message:{}".format(message['id']), message)
				self.redis.expire("message:{}".format(message['id']), int(TIME_SLIDING_WINDOW))
				if 'attachments' in wall_posts[item['id']] and 'photo' in wall_posts[item['id']]['attachments'][0] and 'photo_807' in wall_posts[item['id']]['attachments'][0]['photo']:
					q = 'INSERT INTO media(tweet_id, url) VALUES ("{}", "{}");'.format(
					item['id'], wall_posts[item['id']]['attachments'][0]['photo']['photo_807'])
				exec_mysql(q, self.mysql)
		return medialist

if __name__ == '__main__':
	redis_db = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
	mysql_db = get_mysql_con()

	basicConfig(filename='collector.log', level=WARNING, format=u'[%(asctime)s] LINE: #%(lineno)d | THREAD: %(threadName)s | %(levelname)-8s | %(message)s')
	
	t = TwitterStreamThread(mysql_db, redis_db)
	ih = InstagramHelperThread(mysql_db, redis_db)
	i = InstagramStreamThread(mysql_db, redis_db)
	v = VKontakteStreamThread(mysql_db, redis_db)
	v.start()
	t.start()
	ih.start()
	i.start()
