# NETWORKING
from requests.exceptions import ConnectionError, ReadTimeout, SSLError
from requests.packages.urllib3.exceptions import ReadTimeoutError, ProtocolError
import requests
import socket
import ssl


import datetime

# CONSTANTS
from settings import *

import time

import pprint

# id, text, lat, lng, tstamp, user, network, iscopy
def get_ig_data(data):
	for item in data['data']:
		try:
			text = item['caption']['text']
		except:
			text = ''
		try:
			id = item['id']
			lat = item['location']['latitude']
			lng = item['location']['longitude']
			tstamp = datetime.datetime.fromtimestamp(int(item['created_time']))
			user = item['user']['id']
			media_url = item['images']['standard_resolution']['url']
		except:
			pass
		else:
			print id, lat, lng, tstamp, user, 'i', 0, media_url, text

last_time = [int(time.time()) - 60*10]*len(IG_LOCATIONS)
for i in range(len(IG_LOCATIONS)):
	url = 'https://api.instagram.com/v1/media/search?lat={}&lng={}&min_timestamp={}&distance=5000&access_token={}'.format(
		IG_LOCATIONS[i][1], IG_LOCATIONS[i][0], last_time[i], IG_ACCESS_TOKEN_2)
	print url
	try:
		resp = requests.get(url)
	except (ConnectionError, ProtocolError, ReadTimeout, ReadTimeoutError, SSLError, ssl.SSLError, socket.error) as e:
		pass
	else:
		last_time[i] = int(time.time())
		get_ig_data(resp.json())
	time.sleep(3)