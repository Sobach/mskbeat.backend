#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
MSK PULSE SETTINGS
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

from utilities import get_circles_centers
TW_LOCATIONS = ','.join([str(x) for x in BBOX])
VK_LOCATIONS = get_circles_centers(BBOX, radius=4000)
IG_LOCATIONS = get_circles_centers(BBOX, radius=5000)

# Sliding window size in seconds: default = 1 hour
TIME_SLIDING_WINDOW = 3600