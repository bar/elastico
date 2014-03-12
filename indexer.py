#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function # http://stackoverflow.com/questions/5980042/how-to-implement-the-verbose-or-v-option-into-a-python-script
from pyes import ES
from sqlalchemy import *
#from sqlalchemy.exc import *
from sqlalchemy.ext.declarative import declarative_base
from sqlsoup import SQLSoup, TableClassType, scoped_session, sessionmaker
from sys import exit#, stdin, stdout
from Queue import Queue
from threading import Thread, Lock
from getopt import getopt
from argparse import ArgumentParser, FileType
import threadWatcherClass
import itertools # iterate faster
import time

args = None


class Config:
	"""Configuration class."""

	def __init__(self):
		global args
		args = self.parseArgs()
		self.set()

	def parseArgs(self):
		parser = ArgumentParser(description = 'Index elements from MySQL databases to ElasticSearch servers.', fromfile_prefix_chars='@')
		# parser.add_argument('prefix', choices = prefixes, help = 'Elasticsearch index prefix.')
		parser.add_argument('-v', '--verbose', action = 'count')
		parser.add_argument('-d', '--development', action = 'store_true', help='Development environment.')
		parser.add_argument('-c', '--db-connections', dest = 'dbConnections', type = file, help = 'File where the MySQL database connections are defined.')
		parser.add_argument('-b', '--db-name', dest = 'dbName', help='MySQL database name.')
		parser.add_argument('-q', '--db-queue-size', dest = 'dbQueueSize', type = int, choices = range(1, 9), help = 'MySQL databases queue size.')
		parser.add_argument('-e', '--es-connections', dest = 'elasticConnections', type = file, help = 'File where the ElasticSearch server connections are defined.')
		parser.add_argument('-n', '--es-index', dest = 'esIndex', help='ElasticSearch index.')
		parser.add_argument('-y', '--es-type', dest = 'esType', help='ElasticSearch type.')
		parser.add_argument('-t', '--threads', type = int, choices = range(1, 17), help = 'Number of threads to deploy.')
		parser.add_argument('-r', '--read-buffer', type = int, dest = 'readBuffer', help = 'Size if the buffer used to read from the MySQL databases.')
		parser.add_argument('-w', '--write-buffer', type = int, dest = 'writeBuffer', help = 'Size of the buffer used to write to the ElasticSearch servers.')
		parser.add_argument('-l', '--limit', type = int, help = 'Limit the number of documents to index.')
		#parser.add_argument('-i', '--input', nargs='?', type = FileType('r'), default = stdin)
		#parser.add_argument('-o', '--output', nargs='?', type = FileType('w'), default = stdout)
		return parser.parse_args()

	def set(self):
		for k, v in self.load(args.development).items():
			setattr(self, k, v)
		# prefix
		# self.prefix = args.prefix

		# verbose
		if args.verbose is not None:
			self.verbose = args.verbose

		# elasticsearch connections
		if args.elasticConnections is not None:
			self.readElasticConnections()

		# elasticsearch index name
		if args.esIndex is not None:
			self.esIndex = args.esIndex
		# else:
		# 	self.esIndex = self.esIndex.format(args.prefix)

		# elasticsearch index type
		if args.esType is not None:
			self.esType = args.esType

		# database name
		if args.dbName is not None:
			self.dbName = args.dbName
		# else:
		# 	self.dbName = self.dbName.format(args.prefix)

		# database connections
		if args.dbConnections is not None:
			self.readDbConnections()
		else:
			for i, dbConnection in enumerate(self.dbConnections):
				self.dbConnections[i] = list(dbConnection)

		for dbConnection in self.dbConnections:
			dbConnection.insert(0, self.dbName)

		# databases queue size
		if args.dbQueueSize is not None:
			self.dbQueueSize = args.dbQueueSize

		# threads
		if args.threads is not None:
			self.threads = args.threads

		# read buffer size
		if args.readBuffer is not None:
			self.readBuffer = args.readBuffer

		# write buffer size
		if args.writeBuffer is not None:
			self.writeBuffer = args.writeBuffer

		# documents limit
		if args.limit is not None:
			self.limit = args.limit

	def load(self, development = False):
		if development:
			config = {
				'verbose': 3,
				'indexName': '{:s}_elements',
				'indexType': 'element',
				'dbName': '***REMOVED***_{:s}',
				'dbConnections': [
					('192.168.222.31', '***REMOVED***', '***REMOVED***'),
				],
				'dbQueueSize': 4,
				'threads': 16,
				'readBuffer': 100,
				'writeBuffer': 1000,
				'limit': 2000,
				'elasticConnections': [
					('http', '127.0.0.1', '9200')
				],
			}
		else:
			config = {
				'verbose': 0,
				'indexName': '{:s}_elements',
				'indexType': 'element',
				'dbName': '***REMOVED***_{:s}',
				'dbConnections': [
					('127.0.0.1', '***REMOVED***', '***REMOVED***'),
				],
				'dbQueueSize': 4,
				'threads': 16,
				'readBuffer': 100,
				'writeBuffer': 1000,
				'limit': None,
				'elasticConnections': [
					('http', '127.0.0.1', '9200')
				],
			}

		return config

	def readElasticConnections(self):
		elasticConnections = []
		for line in args.elasticConnections:
			try:
				dbConnection = (scheme, hostname, port) = line.split()
			except ValueError:
				continue
			elasticConnections.append(dbConnection)
		if elasticConnections is not []:
			self.elasticConnections = elasticConnections

	def readDbConnections(self):
		dbConnections = []
		for line in args.dbConnections:
			try:
				dbConnection = (hostname, username, password) = line.split()
			except ValueError:
				continue
			dbConnections.append(dbConnection)
		if dbConnections is not []:
			self.dbConnections = dbConnections

	def convert_arg_line_to_args(self, arg_line):
		for arg in arg_line.split():
			if not arg.strip():
				continue
			yield arg

class Soup(object):
	"""Intended for ORM usage."""

	def __init__(self, connection, engine = 'mysql'):
		name, hostname, username, password = connection
		self.url = '{:s}://{:s}:{:s}@{:s}/{:s}'.format(engine, username, password, hostname, name)

	def build(self, db_charset = 'utf8'):
		"""
		Construct the db object used to read the information from DB.

		Note: Use a session with autocommit=True is better when working with threads, and avoids calling Soup.session.commit()
		http://forum.griffith.cc/index.php?topic=1082.0
		"""
		# Connection to MySQL (read)
		#Soup = SQLSoup(self.url + '?charset=' + db_charset)

		##Session = scoped_session(sessionmaker(autoflush = True, expire_on_commit = True, autocommit = False))
		Session = scoped_session(sessionmaker(autoflush = False, expire_on_commit = False, autocommit = True))
		Soup = SQLSoup(self.url + '?charset=' + db_charset, session = Session)

		return self.bind(Soup)

	def bind(self, Soup):
		"""Intialize relationships.

		http://docs.sqlalchemy.org/en/latest/orm/relationships.html#adjacency-list-relationships
		"""

		return Soup


class Indexer(object):
	"""Indexer class"""

	def __init__(self, Soup, readLimit = None):
		self.count = 0
		self.empty = False

		# Producer
		self.produce(Soup, readLimit)

		# Lock
		self.lock = Lock()

		# Manage threads
		threadWatcherClass.threadWatcher()

	def produce(self, Soup, limit = None):
		"""Retrieve elements ids used as source by the consumer.

		http://stackoverflow.com/questions/7389759/memory-efficient-built-in-sqlalchemy-iterator-generator
		http://www.sqlalchemy.org/trac/wiki/UsageRecipes/WindowedRangeQuery

		http://stackoverflow.com/questions/1078383/sqlalchemy-difference-between-query-and-query-all-in-for-loops
		http://www.mail-archive.com/sqlalchemy@googlegroups.com/msg12443.html
		http://stackoverflow.com/questions/1145905/scanning-huge-tables-with-sqlalchemy-using-the-orm

		"""

		where = True

		total = Soup.session.query(func.count(Soup.elements.id)).filter(where).scalar()
		self.total = min(total, limit) if limit not in [None, 0] else total
		vprint('Total dishes: ' + str(self.total))

		query = Soup.elements.filter(where)
		if limit not in [None, 0]:
			query = query.limit(limit)
		self.buffer = query.values(Soup.elements.id)
		vprint('Preparing the food...')

	def consume(self, startTime, threadName, dbQueue, elasticServer, indexName, indexType, readBufferSize = 10, writeBufferSize = 1000):
		"""The consumer.

		Retrieves elements data an its associated models, using 'elements' as datasource.

		Args:
			threadName:
			dbQueue:
			elasticServer:
			indexName: String with index name for its creation or just its usage.
			indexType: String with inde type.
			readBufferSize:
			writeBufferSize: Integer with the number of docs to accumulate before inserting in elasticServer index.
		"""

		# sync threads
		tprint(threadName, 'Starting... ({:d} > {:d})'.format(readBufferSize, writeBufferSize))
		time.sleep(0.5)

		readBuffer = self.buffer
		total = self.total

		try:
			while not self.empty:
				ids = []

				try:
					self.lock.acquire()
					for _ in itertools.repeat(None, readBufferSize):
						ids.append(next(readBuffer)[0])
				except StopIteration:
					self.empty = True
					tprint(threadName, '## Empty buffer... bang! ##')
					if len(ids) == 0:
						break
					tprint(threadName, 'Dying...')
				finally:
					self.lock.release()

				Elements = []
				Soup = dbQueue.get(True)
				for mappedElement in Soup.elements.filter(Soup.elements.id.in_(ids)):
					Elements.append((mappedElement.id, self.buildElement(mappedElement)))
				#Soup.session.close()
				dbQueue.put(Soup)

				for Element in Elements:
					elasticServer.index(Element[1], indexName, indexType, Element[0], bulk = True)

				count = self.count = self.count + len(ids)

				if count % 10 == 0:
					tprint(threadName, '{:d}/{:d} ({:.2%}) {{{:f}}}'.format(count, total, float(count) / total, time.time() - startTime), 1)

				if count % 1000 == 0:
					elasticServer.refresh()


		except Exception, e:
			print(type(e))
			raise
		except (KeyboardInterrupt, SystemExit):
			exit(0)

		tprint(threadName, 'Ending... I\'m dead')

	def buildElement(self, Element):
		belongsTo = self.belongsTo
		hasMany = self.hasMany
		hasAndBelongsToMany = self.hasAndBelongsToMany

		return self.translateElement({
			'Element': Element,
		})

	def getEmptySchema(self, table):
		"""Get empty structure of a table.

		Args:
			table: Mapped object table.

		Returns:
			A dict with column names with None as values.
		"""
		try:
			schema = {}
			for c in table.c.keys():
				schema[c] = None

			return schema

		except NoSuchTableError, e:
			print('There is no such a table called {}').format(e.args[0])
			exit(1)

	def _mappedToModel(self, Mapped):
		"""Map an SqlSoup object to a CakePHP model.

		Args:
			Mapped: Mapped object to convert.

		Returns:
			Mapped values converted to dict.
		"""

		if not isinstance(type(Mapped), TableClassType):
			return None

		schema = self.getEmptySchema(Mapped._table)
		mappedObjectDict = Mapped.__dict__

		data = {}
		for k in schema:
			data[k] = mappedObjectDict[k]

		return data

	def mappedToModel(self, Mapped):
		"""Convert a Mapped object into a dict.

		If Mapped has other maps inside, also converts them too.

		Args:
			Mapped: Mapped object to convert.

		Returns:
			A list with dicts of converted mapped objects.

		"""
		if isinstance(Mapped, list):
			Model = []
			for v in Mapped:
				_v = self._mappedToModel(v)
				if _v != None:
					Model.append(_v)
			return Model
		else:
			return self._mappedToModel(Mapped)

	def translateElement(self, Mapped):
		"""Converts to dict all members of dict of mapped objects.

		Args:
			Mapped: Dict with mapped object as its values.

		Returns:
			Dict with mapped values converted.
		"""
		Model = {}
		for key in Mapped:
			Model[key] = self.mappedToModel(Mapped[key])

		return Model

	def getMappedObject(self, Mapped, relation):
		"""Gets a Mapped object based on an existing relation.

		Args:
			Mapped: Mapped object from where we get its relation.
			relation: String with the model to relate to.

		Returns:
			Mapped object if exists, None otherwise.
		"""
		return getattr(Mapped, relation)

	def belongsTo(self, Mapped, relation):
		"""Gets a Mapped object based on an existing belongsTo relation.

		Args:
			Mapped: Mapped object from where we get its relation.
			relation: String with the model to relate to.

		Returns:
			Mapped object if exists, None otherwise.
		"""
		return self.getMappedObject(Mapped, relation)

	def hasMany(self, Mapped, relation):
		"""Gets a Mapped object based on an existing hasMany relation.

		Args:
			Mapped: Mapped object from where we get its relation.
			relation: String with the model to relate to.

		Returns:
			Mapped object if exists, None otherwise.
		"""
		return self.getMappedObject(Mapped, relation)

	def hasAndBelongsToMany(self, Mapped, relation):
		"""Gets a Mapped object based on an existing hasAndBelongsToMany relation.

		Args:
			Mapped: Mapped object from where we get its relation.
			relation: String with the model to relate to.

		Returns:
			Mapped object if exists, None otherwise.
		"""

		return self.getMappedObject(Mapped, relation)


class ThreadClass(Thread):
	"""Wrap all thread logic and neccessary implementations.

	TODO: Could use Event() to signal thread termination, and move consumer logic here.
	"""

	def __init__(self, target, args=()):
		if not isinstance(args, tuple):
			args = (args,)
		Thread.__init__(self, target=target, args=args)


def tprint(name, text, tab = 0):
	vvprint('\t' * tab + '[{:s}] {:s}'.format(name, text))

def uniqify(collection):
	"""Utility for 'uniqify' items in collections.

	Args:
		collection: The collection to analize.

	Returns:
		A collection with unique items.
	"""

	seen = set()
	seen_add = seen.add
	return [ x for x in collection if x not in seen and not seen_add(x)]

if __name__ == '__main__':
	startTime = time.time()

	config = Config()

	# verbose helpers
	vprint = print if config.verbose >= 1 else lambda *a, **k: None
	vvprint = print if config.verbose >= 2 else lambda *a, **k: None
	vvvprint = print if config.verbose >= 3 else lambda *a, **k: None

	threadList = range(config.threads)
	threads = []
	dbQueue = Queue()

	dbConnections = config.dbConnections

	for _ in range(config.dbQueueSize):
		dbServer = dbConnections.pop(0)
		dbSoup = Soup(dbServer)
		dbQueue.put(dbSoup.build())
		dbConnections.append(dbServer)

	vprint('Index name: {:s}'.format(config.indexName))
	vprint('Index type: {:s}'.format(config.indexType))
	vprint('Threads: {:d}'.format(config.threads))
	vprint('DB Queue size: {:d}'.format(config.dbQueueSize))
	vprint('Read buffer: {:d}'.format(config.readBuffer))
	vprint('Write buffer: {:d}'.format(config.writeBuffer))

	Indexer = Indexer(Soup(dbConnections.pop(0)).build(), config.limit)

	# Connection to ElasticSearch (write)
	# retry_time = 10
	# timeout = 10
	elasticServer = ES(server = config.elasticConnections, bulk_size = config.writeBuffer)
	elasticServer.indices.create_index_if_missing(config.indexName)

	# Create new threads
	for i in threadList:
		thread = ThreadClass(Indexer.consume, (startTime, str(i+1), dbQueue, elasticServer, config.indexName, config.indexType, config.readBuffer, config.writeBuffer))
		threads.append(thread)

	for t in threads:
		t.start()

	for t in threads:
		t.join()

	vprint('Refreshing index...')
	elasticServer.refresh()

	vprint('Elapsed: {:f}'.format(time.time() - startTime))
