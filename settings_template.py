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
# specify geojson file
BOUNDS_FILE = 'msk.geojson'

from utilities import get_locations
BOUNDS, BBOX, TW_LOCATIONS, VK_LOCATIONS, IG_LOCATIONS = get_locations(bfile = BOUNDS_FILE)

# or bbox: simplified version
# BBOX = [lng min, lat min, lng max, lat max]
# BOUNDS, BBOX, TW_LOCATIONS, VK_LOCATIONS, IG_LOCATIONS = get_locations(bbox = BBOX)

# Sliding window size in seconds: default = 1 hour
TIME_SLIDING_WINDOW = 3600