#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
MSK BEAT SETTINGS
- databases credentials
- SM credentials
- city location
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
# Main bounding box: 4 floats (longitude1, latitude1, longitude2, latitude2)
BBOX = []

# Twitter bbox: string representation
TW_LOCATIONS = ','.join(str(BBOX))

# VKontakte: currently one central point
VK_LOCATIONS = {'lng':BBOX[0] + (BBOX[2]-BBOX[0])/2, 'lat':BBOX[1] + (BBOX[3]-BBOX[1])/2}

# Instagram bbox: list of tuples (lng, lat), number of points, each covers circle with 5km radius
from utilities import get_circles_centers
IG_LOCATIONS = get_circles_centers(BBOX, radius=5000)

# Used networks
"""
TBD: Remove letter codes from Collector, database, and settings. While these letters are not used in Detector
"""
NETS = {'i':0, 't':1, 'v':2}
INV_NETS = {NETS[k]:k for k in NETS.keys()}

# Sliding window size in seconds: default = 1 hour
TIME_SLIDING_WINDOW = 3600