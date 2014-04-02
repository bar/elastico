#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Config.py: App configuration."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

# from getopt import getopt
# from argparse import FileType
from argparse import ArgumentParser

args = None


class Config:
	"""Configuration object.

	Used to easy bootstrapping.
	"""

	def __init__(self):
		global args
		args = self.parseArgs()
		self.set()

	def convert_arg_line_to_args(self, arg_line):
		"""Fancy parting arguments lines from files

		Args:
			arg_line (string): Argument line.

		http://docs.python.org/2/library/argparse.html#argparse.ArgumentParser.convert_arg_line_to_args
		"""
		for arg in arg_line.split():
			if not arg.strip():
				continue
			yield arg

	def parseArgs(self):
		parser = ArgumentParser(description = 'Index elements from MySQL databases to ElasticSearch servers.', fromfile_prefix_chars='@')
		parser.add_argument('-v', '--verbose', action = 'count')
		parser.add_argument('-d', '--development', action = 'store_true', help='Development environment.')
		parser.add_argument('-c', '--db-connections', dest = 'dbConnections', type = file, help = 'File where the MySQL database connections are defined.')
		parser.add_argument('-b', '--db-name', dest = 'dbName', help='MySQL database name.')
		parser.add_argument('-q', '--db-queue-size', dest = 'dbQueueSize', type = int, choices = range(1, 9), help = 'MySQL databases queue size.')
		parser.add_argument('-e', '--es-connections', dest = 'esConnections', type = file, help = 'File where the ElasticSearch server connections are defined.')
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
		if args.esConnections is not None:
			self.esConnections = self.readEsConnections('es', args.esConnections)

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
			self.dbConnections = self.readConnections('db', args.dbConnections)
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
				'esIndex': 'dmoz',
				'esType': 'category',
				'dbName': 'dmoz',
				'dbConnections': [
					('127.0.0.1', 'elastico', 'elastico'),
				],
				'dbQueueSize': 4,
				'threads': 16,
				'readBuffer': 100,
				'writeBuffer': 1000,
				'limit': 2000,
				'esConnections': [
					('http', '192.168.0.1', '9200')
				],
			}
		else:
			config = {
				'verbose': 0,
				'esIndex': 'dmoz',
				'esType': 'category',
				'dbName': 'dmoz',
				'dbConnections': [
					('127.0.0.1', 'elastico', 'elastico'),
				],
				'dbQueueSize': 4,
				'threads': 16,
				'readBuffer': 100,
				'writeBuffer': 1000,
				'limit': None,
				'esConnections': [
					('http', '127.0.0.1', '9200')
				],
			}

		return config

	def readConnections(self, type, file):
		"""Read connections configuration from file"""
		if type == 'es':
			getConnection = self.getEsConnection
		else:
			getConnection = self.getDbConnection

		list = []
		for line in file:
			try:
				connection = getConnection(line.split())
				list.append(connection)
			except ValueError:
				continue
		if list is not []:
			return list

	def getEsConnection(self, line):
		"""Read connection configuration for Elasticsearch"""
		connection = (hostname, username, password) = line
		return connection

	def getDbConnection(self, line):
		"""Read connection configuration for MySQL"""
		connection = (schema, hostname, port) = line
		return connection
