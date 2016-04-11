#!/usr/bin/python
# -*- coding: utf-8 -*-
# MSK.PULSE backend

# SYSTEM
from datetime import datetime
from time import mktime
from re import compile, sub, match, UNICODE, IGNORECASE
from itertools import groupby, combinations
from pickle import loads as ploads, dumps as pdumps
from uuid import uuid4

# MATH
from numpy import mean, std
from scipy.stats import entropy
from shapely.geometry import MultiPoint

# DATABASE
from MySQLdb import escape_string
from redis import ResponseError

# NLTK
from nltk.tokenize import TreebankWordTokenizer
from gensim.corpora import Dictionary
from gensim.models import TfidfModel
from gensim.similarities import MatrixSimilarity
from pymorphy2 import MorphAnalyzer

# SERIALIZING
from msgpack import packb, unpackb

# SELF IMPORT
from utilities import exec_mysql, build_event_classifier

class Event():
	"""
	Event object - class for working with event candidates.
	Collects all data on event candidate, stores it between clustering slices; merges slices, if required.
	TBD: constructs and saves description, scores texts and media, scores and descripts event itself 
	(probability, that candidate is real, event buzz, event category).

	Attributes:
		self.created (datetime): creation timestamp
		self.updated (datetime): last update timestamp
		self.start (datetime): timestamp of the first message in the self.messages dict
		self.end (datetime): timestamp of the last message in the self.messages dict
		self.messages (Dict[dict]): raw tweets from database, enriched with weight, is_core params (on init), tokens (after add_stem_texts)
		self.media (Dict[dict]): raw media objects from database
		self.cores (Dict[list]): tokens, that form the most common vocabulary for the event; computed in create_core() method
		self.entropy (float): entropy for authorship: 0 for mono-authored cluster; computed in event_summary_stats() method
		self.ppa (float): average number of posts per one author; computed in event_summary_stats() method
		self.authors (int): number of unique authors for event
		self.most_active_author (float): share of messages, written by one (most active author)
		self.authors_share (float): number of authors divided by number of messages
		self.relevant_messages_share (float): share of messages with token_score above zero
		self.duration (int): total seconds from self.start to self.end
		self.classifier (Object): classifier for deciding, whether event is real
		self.validity (bool): Classifier verdict, whether event is real or not
		self.verification (bool): Handmade verification of event quality

	Methods:
		self.event_update: commands to calculate all data on event, based on messages and media
		self.is_successor: examines, if current event have common messages with specified event slice
		self.is_valid: method for classifier to determine, if event is actually event, and not a random messages contilation
		self.classifier_row: unififed method for creating classifier data-row
		self.merge: merge current event with another event, update stat Attributes
		self.add_slice: add messages and media to the event, recompute statistics
		self.loads / self.dumps: serialize/deserialize event to/from string representation
		self.load / self.dump: serialize/deserialize event and put/get it to Redis
		self.backup / self.restore: dump/restore event to/from MySQL long-term storage
		self.get_messages_data: get MySQL data for messages ids
		self.get_media_data: get MySQL data for media using existing messages ids
		self.event_summary_stats: calculate statistics and start/end time for event
		self.add_stem_texts: add tokens lists to self.messages
		self.create_core: create vocabulary of most important words for the event
		self.score_messages_by_text: method calculates token_score for messages. TF/IDF likelihood with core is used

	Message keys:
		cluster (int): legacy from DBSCAN - number of cluster (event ancestor)
		id (str): DB message id; unique
		is_core (bool): True, if tweet belongs to the core of ancestor cluster
		iscopy (int): 1, if message is shared from another network
		lat (float): latitude
		lng (float): longitude
		network (int): 1 for Instagram, 0 for Twitter, 2 for VKontakte
		text (str): raw text of the message
		tokens (Set[str]): collection of stemmed tokens from raw text; created in add_stem_texts()
		tstamp (datetime): 'created at' timestamp
		user (int): user id, absolutely unique for one network, but matches between networks are possible
		token_score (float): agreement estimation with average cluster text
		weight (float): standart deviations below average
	"""

	def __init__(self, mysql_con, redis_con, tokenizer = None, morph = None, classifier = None, points = []):
		"""
		Initialization.

		Args:
			mysql_con (PySQLPoolConnection): MySQL connection Object
			redis_con (StrictRedis): RedisDB connection Object
			tokenizer (NLTK.TreebankWordTokenizer): object to split tweets into words
			morph (pymorphy2.MorphAnalyzer): word analyzer - converts words tokens to normalized form. Requires a lot of memory, so it is not created for every event object. 
			classifier (Object): scikit trained classifier to detect real and fake events
			points (list[dict]): raw messages from event detector
		"""
		self.mysql = mysql_con
		self.redis = redis_con

		if morph:
			self.morph = morph
		else:
			self.morph = MorphAnalyzer()
		if tokenizer:
			self.tokenizer = tokenizer
		else:
			self.tokenizer = TreebankWordTokenizer()
		self.word = compile(r'^\w+$', flags = UNICODE | IGNORECASE)
		self.url_re = compile(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+')

		self.validity = None
		self.verification = None
		self.cores = {}
		self.classifier = classifier

		if points:
			self.id = str(uuid4())
			self.created = datetime.now()
			self.updated = datetime.now()

			self.messages = { x['id']:x for x in points }
			self.get_messages_data()
			self.media = {}
			self.get_media_data()
			self.event_update()

	def __del__(self):
		del self.created
		del self.updated
		del self.start
		del self.end
		del self.messages
		del self.media
		del self.cores
		del self.entropy
		del self.ppa
		del self.authors
		del self.most_active_author
		del self.authors_share
		del self.relevant_messages_share
		del self.duration
		del self.validity
		del self.verification

		del self.classifier
		del self.morph
		del self.tokenizer

		del self.word
		del self.url_re

	def __str__(self):
		txt = '<Event {}: {} msgs [{} -- {}]>'.format(self.id, len(self.messages), self.start.strftime("%Y-%m-%d %H:%M"), self.end.strftime("%H:%M"))
		return txt

	def __unicode__(self):
		txt = u'<Event {}: {} msgs [{} -- {}]>'.format(self.id, len(self.messages), self.start.strftime("%Y-%m-%d %H:%M"), self.end.strftime("%H:%M"))
		return txt

	def __repr__(self):
		txt = '<Event {}: {} msgs [{} -- {}]>'.format(self.id, len(self.messages), self.start.strftime("%Y-%m-%d %H:%M"), self.end.strftime("%H:%M"))
		return txt

	def event_update(self):
		"""
		Commands to calculate all data on event, based on messages and media.
		"""
		self.add_stem_texts()
		self.create_core(deviation_threshold = 1)
		self.create_core(deviation_threshold = 2)
		self.create_core(deviation_threshold = 3)
		self.score_messages_by_text()
		self.event_summary_stats()
		self.is_valid()

	def is_successor(self, slice_ids, threshold = 0):
		"""
		Method examines, if current event have common messages with specified event slice.

		Args:
			slice_ids (Set): set if message id's to compare with
			threshold (int): to recognize connection intersection between event messages and new slice should be more than threshold
		"""
		if len(set(self.messages.keys()).intersection(slice_ids)) > threshold:
			return True
		return False

	def is_valid(self):
		"""
		Method for Classifier to determine, if event is actually event, and not a random messages contilation.
		"""
		if self.validity:
			return True
		if self.classifier:
			self.validity = bool(self.classifier.predict([self.classifier_row()])[0])
		return self.validity

	def classifier_row(self):
		"""
		Unififed method for creating classifier data-row. Every var, used in prediction, is listed here, and only here.
		"""
		row = [
			len(self.messages.values()), 
			len(self.media.values()), 
			self.authors, 
			self.most_active_author, 
			self.authors_share, 
			self.entropy, 
			self.ppa, 
			self.relevant_messages_share, 
			self.duration
		]
		return row

	def merge(self, other_event):
		"""
		Method merges current event with another event, update stat Attributes.

		Args:
			other_event (Event): another event object - to merge with
		"""
		self.messages.update(other_event.messages)
		self.media.update(other_event.media)
		self.event_update()
		self.updated = datetime.now()
		self.created = min((self.created, other_event.created))

	def add_slice(self, new_slice):
		"""
		Method adds messages and media to the event, recompute statistics.

		Args:
			new_slice (List[dict]): initial list with messages to be added
		"""
		self.messages.update({ x['id']:x for x in new_slice })
		self.get_messages_data([x['id'] for x in new_slice])
		self.get_media_data([x['id'] for x in new_slice])
		self.event_update()
		self.updated = datetime.now()

	def backup(self, new_serializer=True):
		"""
		Method dumps event to MySQL long-term storage, used for non-evaluating events.

		Args:
			new_serializer (bool): whether to use serializer, based on msgpack package (new), or pickle (old one).
		"""
		if self.verification is None:
			ver = 'NULL'
		else:
			ver = int(self.verification)
		if self.validity is None:
			val = 'NULL'
		else:
			val = int(self.validity)
		if new_serializer:
			msg_string = self.pack()
		else:
			msg_string = self.dumps(compress=True)
		q = b'''INSERT INTO events(id, start, end, msgs, description, dumps, verification, validity) VALUES ("{}", "{}", "{}", {}, "{}", "{}", {}, {}) ON DUPLICATE KEY UPDATE `start`=VALUES(`start`), `end`=VALUES(`end`), `msgs`=VALUES(`msgs`), `description`=VALUES(`description`), `dumps`=VALUES(`dumps`), `verification`=VALUES(`verification`), `validity`=VALUES(`validity`);'''.format(self.id, self.start, self.end, len(self.messages.keys()), escape_string(', '.join([x.encode('utf-8') for x in self.cores[2]])), escape_string(msg_string), ver, val)
		exec_mysql(q, self.mysql)
		self.redis.delete("event:{}".format(self.id))

	def restore(self, event_id, new_serializer=True):
		"""
		Method restores event from MySQL table using event_id parameter.

		Args:
			event_id (str): unique event identifier
			new_serializer (bool): whether to use serializer, based on msgpack package (new), or pickle (old one).
		"""
		q = '''SELECT dumps FROM events WHERE id="{}"'''.format(event_id)
		event_data = exec_mysql(q, self.mysql)[0][0]['dumps']
		if new_serializer:
			self.unpack(event_data)
		else:
			self.loads(event_data)

	def load(self, event_id, redis_prefix='event', new_serializer=True):
		"""
		Method for deserializing and loading event from Redis database.

		Args:
			event_id (str): unique event isentifier
			redis_prefix (str): prefix used in Redis database
			new_serializer (bool): whether to use serializer, based on msgpack package (new), or pickle (old one).
		"""
		try:
			event_data = self.redis.hget('{}:{}'.format(redis_prefix, event_id), 'dumps')
		except ResponseError:
			event_data = self.redis.get('{}:{}'.format(redis_prefix, event_id))
		if new_serializer:
			self.unpack(event_data)
		else:
			self.loads(event_data)

	def dump(self, redis_prefix='event', new_serializer=True):
		"""
		Method for serializing and dumping event to Redis database.

		Args:
			redis_prefix (str): prefix to use, when storing new key in Redis database
			new_serializer (bool): whether to use serializer, based on msgpack package (new), or pickle (old one).
		"""
		if self.verification is None:
			ver = 'NULL'
		else:
			ver = int(self.verification)
		if self.validity is None:
			val = 'NULL'
		else:
			val = int(self.validity)
		if new_serializer:
			msg_string = self.pack()
		else:
			msg_string = self.dumps()
		event = {'start':self.start.strftime("%Y-%m-%d %H:%M:%S"), 'end':self.end.strftime("%Y-%m-%d %H:%M:%S"), 'msgs':len(self.messages.keys()), 'description':', '.join([x.encode('utf-8') for x in self.cores[2]]), 'dumps':msg_string, 'verification':ver, 'validity':val}
		self.redis.hmset("{}:{}".format(redis_prefix, self.id), event)

	def pack(self):
		"""
		Method for serializing event to string.
		"""
		todump = {
			'id':self.id,
			'created':int(mktime(self.created.timetuple())),
			'updated':int(mktime(self.updated.timetuple())),
			'verification':self.verification,
			'messages':[{'id':x['id'], 'is_core':x.get('is_core'), 'token_score':x.get('token_score'), 'weight':x.get('weight')} for x in self.messages.values()]
		}
		return packb(todump)

	def unpack(self, data):
		"""
		Method for deserializing event from string. msgpack lib is used (considered to be faster than pickle).

		Args:
			data (str): pickle dump of event-required parameters.
		"""
		data = unpackb(data)
		self.id = data['id']
		self.created = datetime.fromtimestamp(data['created'])
		self.updated = datetime.fromtimestamp(data['updated'])
		self.verification = data['verification']
		self.messages = {x['id']:x for x in data['messages']}
		self.get_messages_data()
		self.media = {}
		self.get_media_data()
		self.event_update()

	def loads(self, data):
		"""
		Method for deserializing event from string. To be removed.

		Args:
			data (str): pickle dump of event-required parameters.
		"""
		event_data = ploads(data)
		self.messages = event_data['messages']
		if 'compressed' not in event_data.keys() or not event_data['compressed']:
			self.media = event_data['media']
		else:
			for k, v in self.messages.items():
				v.update({'id':k})
			self.get_messages_data()
			self.media = {}
			self.get_media_data()
		self.id = event_data['id']
		self.created = event_data['created']
		self.updated = event_data['updated']
		if 'verification' in event_data.keys():
			self.verification = event_data['verification']
		if 'validation' in event_data.keys():
			self.validation = event_data['validation']
		if 'cores' in event_data.keys():
			self.cores = event_data['cores']
		self.event_update()

	def dumps(self, compress=False):
		"""
		Method for serializing event to string. To be removed.
		"""
		todump = {'id':self.id, 'created':self.created, 'updated':self.updated, 'verification':self.verification, 'validation':self.validity, 'cores':self.cores}
		if compress:
			todump['compressed'] = True
			todump['messages'] = {k:{'is_core':v['is_core'], 'token_score':v['token_score']} for k,v in self.messages.items()}
		else:
			todump['compressed'] = False
			todump['messages'] = self.messages
			todump['media'] = self.media
		return pdumps(todump)

	def get_messages_data(self, ids=None):
		"""
		Method loads MySQL data for messages ids and adds it to the self.messagea argument.

		Args:
			ids (List[str]): list of messages ids to load. If not provided, all ids from self.messages are used 
		"""
		if not ids:
			ids = [x['id'] for x in self.messages.values()]
		q = '''SELECT * FROM tweets WHERE id in ({});'''.format(','.join(['"'+str(x)+'"' for x in ids]))
		data = exec_mysql(q, self.mysql)[0]
		for item in data:
			self.messages[item['id']].update(item)

	def get_media_data(self, ids=None):
		"""
		Method loads MySQL data for media using existing messages ids and adds it to the self.media argument.

		Args:
			ids (List[str]): list of messages ids to load. If not provided, all ids from self.messages are used 
		"""
		if not ids:
			ids = [x['id'] for x in self.messages.values()]
		q = '''SELECT * FROM media WHERE tweet_id in ({});'''.format(','.join(['"'+str(x)+'"' for x in ids]))
		data = exec_mysql(q, self.mysql)[0]
		for item in data:
			self.media[item['id']] = item

	def event_summary_stats(self):
		"""
		Method calculates several statistics, updates self.start and self.end timestamps.
		"""
		authorsip_stats = [len(tuple(i[1])) for i in groupby(sorted(self.messages.values(), key=lambda x:x['user']), lambda z: z['user'])]
		self.authors = len(authorsip_stats)
		self.most_active_author = max(authorsip_stats)/float(len(self.messages.values()))
		self.authors_share = float(self.authors)/len(self.messages.values())
		self.entropy = entropy(authorsip_stats)
		self.ppa = mean(authorsip_stats)
		self.relevant_messages_share = float(len([x for x in self.messages.values() if x['token_score'] > 0]))/len(self.messages.values())
		self.start = min([x['tstamp'] for x in self.messages.values()])
		self.end = max([x['tstamp'] for x in self.messages.values()])
		self.duration = int((self.end - self.start).total_seconds())

	def add_stem_texts(self):
		"""
		Method adds tokens lists to self.messages.
		"""
		for i in self.messages.keys():
			if 'tokens' not in self.messages[i].keys():
				txt = self.messages[i].get('text', '')
				txt = sub(self.url_re, '', txt)
				self.messages[i]['tokens'] = {self.morph.parse(token.decode('utf-8'))[0].normal_form for token in self.tokenizer.tokenize(txt) if match(self.word, token.decode('utf-8'))}

	def create_core(self, deviation_threshold=2, min_token=3):
		"""
		Method creates core of imprtant words for event.

		Args:
			deviation_threshold (int): number of standart deviations, that differs core tokens from average tokens
			min_token (int): minimal length of token, to exclude prepositions/conjunctions
		"""
		texts_by_authors = [set().union(*[msg['tokens'] for msg in list(y[1])]) for y in groupby(sorted(self.messages.values(), key=lambda x:x['user']), lambda z:z['user'])]
		top_words = {}
		for doc in texts_by_authors:
			for token in doc:
				if len(token) >= min_token:
					try:
						top_words[token] += 1
					except KeyError:
						top_words[token] = 1
		th_vals = [x[1] for x in top_words.items()]
		threshold = mean(th_vals) + deviation_threshold * std(th_vals)
		self.cores[deviation_threshold] = [k for k,v in top_words.items() if v > threshold]

	def score_messages_by_text(self, deviation_threshold=2):
		"""
		Method calculates token_score parameter for self.messages.

		Args:
			deviation_threshold (int): number of standart deviations, that differs core tokens from average tokens
		"""
		texts = [x['tokens'] for x in self.messages.values()]
		if not sum([bool(x) for x in texts]) or len(set([frozenset(x) for x in texts])) == 1:
			for k in self.messages.keys():
				self.messages[k]['token_score'] = 0
			return
		dictionary = Dictionary(texts)
		corpus = [dictionary.doc2bow(text) for text in texts]
		tfidf = TfidfModel(corpus, id2word=dictionary)
		index = MatrixSimilarity(tfidf[corpus])
		scores = index[dictionary.doc2bow(self.cores[deviation_threshold])]
		for i in range(len(scores)):
			self.messages.values()[i]['token_score'] = float(scores[i])