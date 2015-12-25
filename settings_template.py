#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
MSK BEAT SETTINGS
- databases credentials
- SM credentials
- city locations
- sliding window size
"""

# MySQL
MYSQL_USER = ''
MYSQL_PASSWORD = ''
MYSQL_HOST = ''
MYSQL_DB = ''

# Redis
REDIS_HOST = ''
REDIS_PORT = 0
REDIS_DB = 0

# Twitter credentials
TW_CONSUMER_KEY = ''
TW_CONSUMER_SECRET = ''
TW_ACCESS_TOKEN_KEY = ''
TW_ACCESS_TOKEN_SECRET = ''

# Instagram credentials
IG_ACCESS_TOKEN_1 = ''
IG_ACCESS_TOKEN_2 = ''

# VKontakte credentials
VK_ACCESS_TOKEN = ''

# Locations
"""
TBD: Make one "locations" square and, produce all others using it. BBOX should be the main and the only
"""
TW_LOCATIONS = 'lng1,lat1,lng2,lat2'
VK_LOCATIONS = {'lat':0, 'lng':0}
IG_LOCATIONS = [(0, 0),(0, 0)] # (lng, lat) pairs as Instagram has small radius around every point
BBOX = [float(x) for x in TW_LOCATIONS.split(',')]

# Used networks
"""
TBD: Remove letter codes from Collector, database, and settings. While these letters are not used in Detector
"""
NETS = {'i':0, 't':1, 'v':2}
INV_NETS = {NETS[k]:k for k in NETS.keys()}

# Sliding window size in seconds: default = 1 hour
TIME_SLIDING_WINDOW = 3600