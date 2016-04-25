#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# SYSTEM
from datetime import datetime, timedelta
from itertools import groupby, chain
from time import sleep

# DATABASE
from redis import StrictRedis

# MATH
from numpy import array, mean, std, absolute, seterr, float32, concatenate, zeros
from networkx import Graph, connected_components
from sklearn.neighbors import KDTree
from sklearn.cluster import DBSCAN

# SYSTEM MATH
from math import radians, cos, sin, asin, sqrt

# HEAVY NLTK
from pymorphy2 import MorphAnalyzer
from nltk.tokenize import TreebankWordTokenizer

# SELF IMPORT
from settings import REDIS_HOST, REDIS_PORT, REDIS_DB, BBOX, TIME_SLIDING_WINDOW
from utilities import get_mysql_con, exec_mysql, build_event_classifier
from event import Event

seterr(all='ignore')

class EventDetector():
	"""
	Event detector Object: used to discover events both online and offline.
	When self.run() method starts, initializes infinity loop, where every time
	takes messages from redis db, filters and clusters them, and merges with
	clusters from previous steps.
	"""

	def __init__(self, mysql_con, redis_con, bbox, fast_forward_ratio=1.0, max_dist = 0.2, ref_period = 14, use_real_reference = True):
		"""
		Initialization.

		Args:
			redis_con (StrictRedis): RedisDB connection Object
			mysql_con (PySQLPoolConnection): MySQL connection Object
			bbox (List[float]): min longitude, min latitude, max longitude, max latitude
			fast_forward_ratio (float): parameter for emulation - the greater param - the faster emulation
			max_dist (float): maximum distance for messages, considered to be a part of the same event (in km)
			ref_period (int): reference period in days: how many days are used to build reference trees
		"""
		self.bbox = bbox
		self.mysql = mysql_con
		self.redis = redis_con
		self.morph = MorphAnalyzer()
		self.tokenizer = TreebankWordTokenizer()

		self.interrupter = False
		self.ref_days = ref_period
		self.use_real_reference = use_real_reference
		self.ffr = fast_forward_ratio
		if self.ffr > 1:
			self.pause = 0
		else:
			self.pause = 60

		self.calcualte_eps_dbscan(max_dist)
		self.daily_maintenance()
		self.get_dumped_events()

	def run(self):
		"""
		Main object loop. Looks for actual messages, DBSCANs them, and merges with previously computed events.
		Interrupts when self.interrupter attribute is set to True.
		"""
		while True:
			self.loop_start = datetime.now()
			self.build_current_trees()
			if self.current_datapoints:
				self.build_reference_trees()
				self.current_datapoints_threshold_filter()
				self.current_datapoints_outliers_filter()
				slice_clusters = self.current_datapoints_dbscan()
				self.merge_slices_to_events(slice_clusters)
				self.dump_current_events()

				# interrupter for emulator
				if self.interrupter:
					for event in self.events.values():
						event.backup()
					break

				# daily update tasks
				if self.last_maintenance.day != datetime.now().day:
					self.daily_maintenance()

				# cpu usage slower: one iteration per 1 minute, when working in real time
				pause = self.pause - (datetime.now() - self.loop_start).total_seconds()
				if pause > 0:
					sleep(pause)

	def daily_maintenance(self):
		# Updating classifier
		self.classifier = build_event_classifier(classifier_type="adaboost", balanced=True)

		# Creating new reference data table
		exec_mysql('TRUNCATE ref_data;', self.mysql)

		if self.use_real_reference:
			exec_mysql('''INSERT INTO ref_data SELECT lat, lng, network, DATE(tstamp) as tstamp, TIME_TO_SEC(TIME(tstamp)) AS `second` FROM tweets WHERE DATE(tstamp) BETWEEN '{}' AND '{}' ORDER BY `second` ASC;'''.format((datetime.now() - timedelta(days=self.ref_days)).strftime('%Y-%m-%d'), (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')), self.mysql)
		else:
			d = exec_mysql('SELECT tstamp FROM tweets_origins ORDER BY tstamp DESC LIMIT 1;', self.mysql)
			max_date = d[0][0]['tstamp'] - timedelta(days=1)
			exec_mysql('''INSERT INTO ref_data SELECT lat, lng, network, DATE(tstamp) as tstamp, TIME_TO_SEC(TIME(tstamp)) AS `second` FROM tweets_origins WHERE DATE(tstamp) BETWEEN '{}' AND '{}' ORDER BY `second` ASC;'''.format((max_date - timedelta(days=self.ref_days-1)).strftime('%Y-%m-%d'), max_date.strftime('%Y-%m-%d')), self.mysql)
		self.last_maintenance = datetime.now()

	def calcualte_eps_dbscan(self, max_dist):
		"""
		Calculate maximum "distance" in coordinates for DBSCAN between points,
		given bounding box and maximum distance in km. Produces self.eps attribute (float).

		Args:
			max_dist (float): maximum distance for messages, considered to be a part of the same event (in km)
		"""
		lon1, lat1, lon2, lat2 = map(radians, self.bbox)
		dlon = lon2 - lon1 
		dlat = lat2 - lat1
		a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
		c = 2 * asin(sqrt(a))
		km = c * 6371
		dist = sqrt((self.bbox[0] - self.bbox[2])**2 + (self.bbox[1] - self.bbox[3])**2)
		self.eps = dist * max_dist / km

	def build_reference_trees(self, min_points = 10):
		"""
		Create kNN-trees (KDTrees) for previous period - 1 day = 1 tree. 
		These trees are used as reference, when comparing with current kNN-distances.
		Trees are created for each network separately.
		Args:
			min_points (int): minimum number of points for every tree. if there is not enough data, points (0,0) are used

		Result:
			self.reference_trees (Dict[List[KDTree]]) - Dict keys: 1 (Twitter), 2 (Instagram), 3 (VKontakte)
		"""
		self.reference_data = self.get_reference_data(self.reference_time)
		networks = [1,2,3]
		preproc = {net:{} for net in networks}
		for item in self.reference_data:
			try: 
				preproc[item['network']][item['tstamp']].append([item['lng'], item['lat']])
			except KeyError:
				preproc[item['network']][item['tstamp']] = [[item['lng'], item['lat']]]
		self.reference_trees = {net:[] for net in networks}

		for net in networks:
			if not preproc[net]:
				self.reference_trees[net] = [KDTree(array([(0,0)]*min_points))]*14
				continue
			for element in preproc[net].values():
				if len(element) < min_points:
					element += [(0,0)]*(min_points - len(element))
				self.reference_trees[net].append(KDTree(array(element)))

	def get_reference_data(self, time, ):
		"""
		Load historical data from MySQL.
		Returns MySQL dict
		Args:
			time (datetime): timestamp for data of interest. 90% of data is taken from the past, and 10% - from the future
		"""
		lower_bound = time - timedelta(seconds = TIME_SLIDING_WINDOW * 0.9)
		upper_bound = time + timedelta(seconds = TIME_SLIDING_WINDOW * 0.1)
		lower_bound = lower_bound.time().second + lower_bound.time().minute * 60 + lower_bound.time().hour * 3600
		upper_bound = upper_bound.time().second + upper_bound.time().minute * 60 + upper_bound.time().hour * 3600

		if lower_bound < upper_bound:
			q = '''SELECT tstamp, lat, lng, network FROM ref_data WHERE `second` BETWEEN {} AND {};'''.format(lower_bound, upper_bound)
			data, i = exec_mysql(q, self.mysql)
		else:
			q = '''SELECT tstamp, lat, lng, network FROM ref_data WHERE `second` BETWEEN {} AND 86400;'''.format(lower_bound)
			data = exec_mysql(q, self.mysql)[0]
			q = '''SELECT DATE_ADD(tstamp,INTERVAL -1 DAY) AS tstamp, lat, lng, network FROM ref_data WHERE `second` BETWEEN 0 AND {};'''.format(upper_bound)
			data += exec_mysql(q, self.mysql)[0]
		return data

	def build_current_trees(self):
		"""
		Building current KDTree from data, stored in Redis database.
		Every tweet there has expiration time: TIME_SLIDING_WINDOW/fast_forward_ratio
		So at every time only currently active tweets are selected.
		Method updates (creates) self.current_datapoints (Dict[Dict]) - dict with three keys (1,2,3 - networks). 
		Every value of this dict consists of dict with 'array' and 'ids' keys. 
		'array' is a [lng, lat] numpy array of float32 values.
		'ids' is a strings numpy array.
		Method creates/updates self.current_trees Dict(KDTree) for each of networks
		Method updates self.reference_time (datetime.datetime) variable, and sets it to the maximum timestamp from the datapoints
		"""
		data = {}
		maxtime = []
		for key in self.redis.keys("message:*"):
			message = self.redis.hgetall(key)
			if not message:
				continue
			else:
				if message['id'] == '0':
					self.interrupter = True
				message['lat'] = float(message['lat'])
				message['lng'] = float(message['lng'])
				message['network'] = int(message['network'])
				maxtime.append(int(message['tstamp']))
				try:
					data[message['network']]['array'].append([message['lng'], message['lat']])
					data[message['network']]['ids'].append(message['id'])
				except KeyError:
					data[message['network']] = {'array':[[message['lng'], message['lat']]], 'ids':[message['id']]}
		self.current_datapoints = data
		self.current_trees = {}
		if data:
			for net in data.keys():
				self.current_datapoints[net]['array'] = array(self.current_datapoints[net]['array'], dtype=float32)
				self.current_datapoints[net]['ids'] = array(self.current_datapoints[net]['ids'])
				self.current_trees[net] = KDTree(self.current_datapoints[net]['array'])
		if maxtime:
			self.reference_time = datetime.fromtimestamp(max(maxtime))

	def current_datapoints_threshold_filter(self, neighbour_points = 5):
		"""
		Filter from current datapoints, those that do not have enough neighbour points in the 2*max_dist radius (in meters).
		Assumption: if there is less than neighbour_points around the data point, it can't be a part of event.
		Method doesn't take into account networks.
		This method is computationally cheaper, than self.current_datapoints_outliers_filter, so it is used as a prefilter.
		Method updates self.current_datapoints dict.

		Args:
			neighbour_points (int): minimal number of neighbours, every point should have.
		"""
		nets = self.current_datapoints.keys()
		ids = concatenate([self.current_datapoints[x]['ids'] for x in nets])
		coords = concatenate([self.current_datapoints[x]['array'] for x in nets])
		megatree = KDTree(coords)
		for net in nets:
			neighbours_number = megatree.query_radius(self.current_datapoints[net]['array'], r=self.eps*2, count_only=True)
			self.current_datapoints[net]['array'] = self.current_datapoints[net]['array'][neighbours_number >= neighbour_points]
			self.current_datapoints[net]['ids'] = self.current_datapoints[net]['ids'][neighbours_number >= neighbour_points]

	def current_datapoints_outliers_filter(self, neighbour_points = 5):
		"""
		Method looks for outliers between current datapoints. It estimates average distance to n neighbours. 
		If it is closer, than average for two weeks minus 3 standart deviations, the point goes to outliers. 
		Otherwise it is being filtered.
		Computational part:
		- calculate mean and standart deviation for kNN distance for current points using referenced KDTrees
		- compare referenced values with current tree, find outliers (3 standart deviations from mean)
		Method updates self.current_datapoints dict: filtered points, and 'weight' key for network dict.
		Weight is a number of standart deviations, that differ current point from average.

		Args:
			neighbour_points (int): minimal number of neighbours (of points in current dataset).
		"""
		if self.current_datapoints:
			for net in self.current_datapoints.keys():
				if len(self.current_datapoints[net]['array']) < neighbour_points:
					self.current_datapoints[net]['weights'] = zeros(len(self.current_datapoints[net]['ids']))
					continue
				cur_knn_data = self.current_trees[net].query_radius(self.current_datapoints[net]['array'], r=self.eps*2, count_only=True)
				ref_knn_data = array([x.query_radius(self.current_datapoints[net]['array'], r=self.eps*2, count_only=True) for x in self.reference_trees[net]])

				thr_knn_mean = mean(ref_knn_data, axis=0)
				thr_knn_std = std(ref_knn_data, axis=0)
				thr_knn_data =  thr_knn_mean + thr_knn_std  * 3
				self.current_datapoints[net]['array'] = self.current_datapoints[net]['array'][cur_knn_data > thr_knn_data]
				self.current_datapoints[net]['ids'] = self.current_datapoints[net]['ids'][cur_knn_data > thr_knn_data]
				self.current_datapoints[net]['weights'] = (absolute(cur_knn_data - thr_knn_mean)/thr_knn_std)[cur_knn_data > thr_knn_data]

	def current_datapoints_dbscan(self):
		"""
		Method clusters points-outliers (after current_datapoints_threshold_filter and current_datapoints_outliers_filter) into slice-clusters using DBSCAN.
		Returns dict of slice-clusters - base for event-candidates. Uses self.eps attribute to estimate cluster boundaries.
		"""
		nets = self.current_datapoints.keys()
		ids = concatenate([self.current_datapoints[x]['ids'] for x in nets])
		coords = concatenate([self.current_datapoints[x]['array'] for x in nets])
		weights = concatenate([self.current_datapoints[x]['weights'] for x in nets])
		if len(ids) > 0:
			clustering = DBSCAN(eps=self.eps, min_samples=5)
			labels = clustering.fit_predict(coords)
			core_ids = ids[clustering.core_sample_indices_]
			ids = ids[labels > -1]
			coords = coords[labels > -1]
			weights = weights[labels > -1]
			labels = labels[labels > -1]
			ret_tab = {}
			for i in range(len(labels)):
				try:
					ret_tab[labels[i]].append({'id':ids[i], 'lng':coords[i,0], 'lat':coords[i,1], 'weight':weights[i], 'is_core':ids[i] in core_ids})
				except KeyError:
					ret_tab[labels[i]] = [{'id':ids[i], 'lng':coords[i,0], 'lat':coords[i,1], 'weight':weights[i], 'is_core':ids[i] in core_ids}]
			return ret_tab
		else:
			return {}

	def get_dumped_events(self):
		"""
		Method loads previously saved events from Redis database - to have data to merge with currently created slice-clusters.
		Old events are being dumped to MySQL, and shouldn't be in Redis.
		Currently method is used only on initialisation. 
		"""
		self.events = {}
		for key in self.redis.keys("event:*"):
			event = Event(self.mysql, self.redis, self.tokenizer, self.morph, self.classifier)
			event.load(key[6:])
			self.events[event.id] = event

	def merge_slices_to_events(self, current_slices):
		"""
		Method merges DBSCAN-generated event slices with previously found events. 
		Bimodal network is used to find connections between events and slices,
		then slices are being merged with events, or transformed to new ones.
		Merged events are being deleted.

		Args:
			current_slices (Dict(List[Dict])): output of self.current_datapoints_dbscan method. Every item of dict is a slice cluster: list with dicts of messages from that cluster.
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
			meta_slice = [msg for i in unify_slices for msg in current_slices[i]]
			if not unify_events:
				new_event = Event(self.mysql, self.redis, self.tokenizer, self.morph, self.classifier, meta_slice)
				self.events[new_event.id] = new_event
			elif len(unify_events) == 1 and len(unify_slices) == 1 and set(self.events[unify_events[0]].messages.keys()) == {x['id'] for x in meta_slice}:
				continue
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
		Method saves events to Redis after adding new slices. Only events, that were updates, are being dumped.
		Only events with more than one author and more than 4 messages are being dumped to MySQL.
		In parallel looks through self.current_events dict: searches for events without updates	for TIME_SLIDING_WINDOW/fast_forward_ratio time and dumps them to MySQL.
		"""
		for eid in self.events.keys():
			if (datetime.now() - self.events[eid].updated).total_seconds() > TIME_SLIDING_WINDOW/self.ffr:
				if self.events[eid].authors > 1 or len(self.events[eid].messages.values()) >= 5:
					self.events[eid].backup()
				else:
					self.redis.delete("event:{}".format(eid))
				del self.events[eid]
			elif self.events[eid].updated > self.loop_start:
				self.events[eid].dump()

if __name__ == '__main__':
	redis_db = StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
	mysql_db = get_mysql_con()
	detector = EventDetector(mysql_db, redis_db, BBOX, fast_forward_ratio=1)
	detector.run()