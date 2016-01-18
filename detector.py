#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# SYSTEM
from datetime import datetime, timedelta
from itertools import groupby
from pickle import loads as ploads

# DATABASE
from redis import StrictRedis

# MATH
from numpy import array, mean, std, absolute, seterr
from networkx import Graph, connected_components
from sklearn.neighbors import KDTree
from sklearn.cluster import DBSCAN

# SYSTEM MATH
from math import radians, cos, sin, asin, sqrt

# SELF IMPORT
from settings import *
from utilities import get_mysql_con, exec_mysql
from event import Event

seterr(divide='ignore', invalid='ignore')

class EventDetector():
	"""
	Event detector Object: used to discover events both online and offline.
	When self.run() method starts, initializes infinity loop, where every time
	takes messages from redis db, filters and clusters them, and merges with
	clusters from previous steps.
	"""

	def __init__(self, mysql_con, redis_con, bbox, fast_forward_ratio=1.0):
		"""
		Initialization.

		Args:
			redis_con (StrictRedis): RedisDB connection Object
			mysql_con (PySQLPoolConnection): MySQL connection Object
			bbox (List[float]): min longitude, min latitude, max longitude, max latitude
			fast_forward_ratio (float): parameter for emulation - the greater param - the faster emulation and
		"""
		self.bbox = bbox
		self.mysql = mysql_con
		self.redis = redis_con
		self.calcualte_eps_dbscan()
		self.interrupter = False
		self.ffr = fast_forward_ratio
		self.events = {}

	def run(self):
		"""
		Main object loop. Looks for actual messages, DBSCANs them, and merges with previously computed events.
		Interrupts if self.interrupter is set to True.
		"""
		while True:
			start_time = datetime.now()
			self.build_current_trees()
			if self.current_datapoints:
				self.build_reference_trees(take_origins = True)
				points = self.get_current_outliers()
				slice_clusters = self.dbscan_tweets(points)
				self.get_previous_events()
				self.merge_slices_to_events(slice_clusters)
				self.dump_current_events()
				secs = (datetime.now() - start_time).total_seconds()
				print '{} seconds,\t{} events,\t{} messages'.format(secs, len(self.events.values()), len(self.current_datapoints.values()))
				if self.interrupter:
					for event in self.events.values():
						event.backup()
					break

	def calcualte_eps_dbscan(self, max_dist = 0.2):
		"""
		Calculate maximum "distance" in coordinates for DBSCAN between points,
		given bounding box and maximum distance in km.
		Result:
			self.eps (float)
		"""
		lon1, lat1, lon2, lat2 = map(radians, self.bbox)
		dlon = lon2 - lon1 
		dlat = lat2 - lat1
		a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
		c = 2 * asin(sqrt(a)) 
		km = c * 6371
		dist = sqrt((self.bbox[0] - self.bbox[2])**2 + (self.bbox[1] - self.bbox[3])**2)
		self.eps = dist * max_dist / km

	def build_reference_trees(self, days = 14, take_origins = False, min_points = 10):
		"""
		Create kNN-trees (KDTrees) for previous period - 1 day = 1 tree. 
		These trees are used as reference, when comparing with current kNN-distance.
		Trees are created for each network separately.
		Args:
			days (int): how many days should be used for reference (by default 2 weeks)
			take_origins (bool): whether to use actual dynamic data (default), or training dataset
			min_points (int): minimum number of points for every tree. if there is not enough data, points (0,0) are used

		Result:
			self.trees (List[KDTree])
		"""
		time = max([item['tstamp'] for sublist in self.current_datapoints.values() for item in sublist])
		data = self.get_reference_data(time, days, take_origins)
		networks = [1,2,3]
		preproc = {net:{} for net in networks}
		for item in data:
			try: 
				preproc[item['network']][item['DATE(tstamp)']].append([item['lng'], item['lat']])
			except KeyError: 
				preproc[item['network']][item['DATE(tstamp)']] = [[item['lng'], item['lat']]]
		self.reference_trees = {net:[] for net in networks}
		for net in networks:
			if not preproc[net]:
				self.reference_trees[net] = [KDTree(array([(0,0)]*min_points))]*days
				continue
			for element in preproc[net].values():
				if len(element) < min_points:
					element += [(0,0)]*(min_points - len(element))
				self.reference_trees[net].append(KDTree(array(element)))

	def get_reference_data(self, time, days = 14, take_origins = False):
		"""
		Load historical data from MySQL.
		If take_origins, use constant tweets from tweets_origins table,
		otherwise - use dynamic data from tweets table.
		Returns MySQL dict
		Args:
			time (datetime): timestamp for data of interest. 90% of data is taken from the past, and 10% - from the future.
			days (int): how many days should be used for reference (by default 2 weeks)
			take_origins (bool): whether to use actual dynamic data (default), or training dataset
		"""
		lower_bound = time - timedelta(seconds = TIME_SLIDING_WINDOW * 0.9)
		upper_bound = time + timedelta(seconds = TIME_SLIDING_WINDOW * 0.1)
		if take_origins:
			database = 'tweets_origins'
			d = exec_mysql('SELECT tstamp FROM tweets_origins ORDER BY tstamp DESC LIMIT 1;', self.mysql)
			max_date = d[0][0]['tstamp']
		else:
			database = 'tweets'
			max_date = time
		min_date = (max_date - timedelta(days = days)).replace(hour = lower_bound.hour, minute = lower_bound.minute)
		if lower_bound.time() < upper_bound.time():
			q = '''SELECT DATE(tstamp), lat, lng, network FROM {} WHERE tstamp >= '{}' AND tstamp <= '{}' AND TIME(tstamp) >= '{}' AND TIME(tstamp) <= '{}';'''.format(database, min_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%Y-%m-%d %H:%M:%S'), min_date.strftime('%H:%M:%S'), max_date.strftime('%H:%M:%S'))
			data, i = exec_mysql(q, self.mysql)
		else:
			q = '''SELECT DATE(tstamp), lat, lng, network FROM {} WHERE tstamp >= '{}' AND tstamp <= '{}' AND TIME(tstamp) >= '{}' AND TIME(tstamp) <= '23:59:59';'''.format(database, min_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%Y-%m-%d %H:%M:%S'), min_date.strftime('%H:%M:%S'))
			data = exec_mysql(q, self.mysql)[0]
			q = '''SELECT DATE_ADD(DATE(tstamp),INTERVAL -1 DAY) AS 'DATE(tstamp)', lat, lng, network FROM {} WHERE tstamp >= '{}' AND tstamp <= '{}' AND TIME(tstamp) >= '00:00:00' AND TIME(tstamp) <= '{}';'''.format(database, min_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%Y-%m-%d %H:%M:%S'), max_date.strftime('%H:%M:%S'))
			data += exec_mysql(q, self.mysql)[0]
		return data

	def build_current_trees(self):
		"""
		Building current KDTree from data, stored in Redis database.
		Every tweet there has expiration time: TIME_SLIDING_WINDOW/fast_forward_ratio
		So at every time only currently active tweets are selected.
		"""
		data = {}
		for key in self.redis.keys("message:*"):
			try:
				message = ploads(self.redis.get(key))
			except TypeError:
				pass
			else:
				if message['id'] == 0 and message['text'] == 'TheEnd':
					self.interrupter = True
				try:
					data[message['network']].append(message)
				except KeyError:
					data[message['network']] = [message]
		self.current_datapoints = data
		if data:
			self.current_trees = {}
			for net in data.keys():
				self.current_trees[net] = KDTree(array([[x['lng'], x['lat']] for x in data[net]]))

	def get_current_outliers(self, neighbour_points = 5):
		"""
		Computational part:
		- calculate mean and standart deviation for kNN distance for current points using referenced KDTrees
		- compare referenced values with current tree, find outliers (3 standart deviations from mean)
		Result: returns points without noise (outliers only)

		Args:
			neighbour_points (int)
		"""
		points = []
		if self.current_datapoints:
			for net in self.current_datapoints.keys():
				if len(self.current_datapoints[net]) < neighbour_points:
					continue
				cur_knn_data = mean(self.current_trees[net].query(array([[x['lng'], x['lat']] for x in self.current_datapoints[net]]), k=5, return_distance=True)[0], axis=1)
				ref_knn_data = mean(array([x.query(array([[x['lng'], x['lat']] for x in self.current_datapoints[net]]), k=5, return_distance=True)[0] for x in self.reference_trees[net]]), axis=2)
				thr_knn_mean = mean(ref_knn_data, axis=0)
				thr_knn_std = std(ref_knn_data, axis=0)
				thr_knn_data =  thr_knn_mean - thr_knn_std  * 3
				new_points = list(array(self.current_datapoints[net])[cur_knn_data < thr_knn_data])
				weights = (absolute(cur_knn_data - thr_knn_mean)/thr_knn_std)[cur_knn_data < thr_knn_data]
				for i in range(len(weights)):
					new_points[i]['weight'] = weights[i]
				points += new_points
		return points

	def dbscan_tweets(self, points):
		"""
		Method clusters points-outliers into slice-clusters using DBSCAN.
		Returns dict of slice-clusters - base for event-candidates.
		"""
		if points:
			clustering = DBSCAN(eps=self.eps, min_samples=5)
			labels = clustering.fit_predict(array([[x['lng'], x['lat']] for x in points]))
			ret_tab = [dict(points[i].items() + {'cluster':labels[i], 'is_core': i in clustering.core_sample_indices_}.items()) for i in range(len(labels)) if labels[i] > -1]
			ret_tab = { x[0]:tuple(x[1]) for x in groupby(sorted(ret_tab, key=lambda x:x['cluster']), lambda x: x['cluster']) }
			return ret_tab
		else:
			return {}

	def get_previous_events(self):
		"""
		Loading previously saved events from Redis database - 
		to have data to merge with currently created slice-clusters
		"""
		self.events = {}
		for key in self.redis.keys("event:*"):
			event = Event(self.mysql, self.redis)
			event.loads(key[6:])
			self.events[event.id] = event

	def merge_slices_to_events(self, current_slices):
		"""
		Looking for comparation between slices and events.
		Updating events, if needed; creating new events.
		Deleting garbage events (that has been merged).
		"""
		slices_ids = set(current_slices.keys())
		events_ids = set(self.events.keys())
		edges = []
		for slice_id, event_slice in current_slices.items():
			slice_ids = {x['id'] for x in event_slice}
			for event in self.events.values():
				if event.is_successor(slice_ids):
					edges.append((slice_id, event.id))
		G = Graph()
		G.add_nodes_from(slices_ids.union(events_ids))
		G.add_edges_from(edges)
		events_to_delete = []
		for cluster in [x for x in connected_components(G) if x.intersection(slices_ids)]:
			unify_slices = cluster.intersection(slices_ids)
			unify_events = list(cluster.intersection(events_ids))
			meta_slice = [x for x in (msg for cl in [current_slices[i] for i in unify_slices] for msg in cl)]
			chain([current_slices[x] for x in unify_slices])
			if not unify_events:
				new_event = Event(self.mysql, self.redis, meta_slice)
				self.events[new_event.id] = new_event
			else:
				if len(unify_events) > 1:
					for ancestor in unify_events[1:]:
						self.events[unify_events[0]].merge(self.events[ancestor])
						events_to_delete.append(ancestor)
				self.events[unify_events[0]].add_slice(meta_slice)
		for event in events_to_delete:
			del self.events[event]
			self.redis.delete("event:{}".format(event))

	def dump_current_events(self):
		"""
		Saves events to Redis after adding new slices and removing expired events.
		In parallel looks through self.current_events dict: searches for events without updates
		for TIME_SLIDING_WINDOW/fast_forward_ratio time.

		"""
		for event in self.events.values():
			if (datetime.now() - event.updated).total_seconds() > TIME_SLIDING_WINDOW/self.ffr:
				event.backup()
			else:
				event.dumps()

if __name__ == '__main__':
	redis_db = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
	mysql_db = get_mysql_con()
	detector = EventDetector(mysql_db, redis_db, BBOX, fast_forward_ratio=60)
	detector.run()