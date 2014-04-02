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
		args = self.parse_args()
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

	def parse_args(self):
		parser = ArgumentParser(
			description = 'Index elements from MySQL databases to ElasticSearch servers.',
			fromfile_prefix_chars = '@')
		parser.add_argument('-v', '--verbose',
			action = 'count')
		parser.add_argument('-d', '--development',
			action = 'store_true',
			help = 'Development environment.')
		parser.add_argument('-c', '--db-connections',
			dest = 'db_connections',
			type = file,
			help = 'File where the MySQL database connections are defined.')
		parser.add_argument('-b', '--db-name',
			dest = 'db_name',
			help='MySQL database name.')
		parser.add_argument('-q', '--db-queue-size',
			dest = 'db_queue_size',
			type = int,
			choices = range(1, 9),
			help = 'MySQL databases queue size.')
		parser.add_argument('-e', '--es-connections',
			dest = 'es_connections',
			type = file,
			help = 'File where the ElasticSearch server connections are defined.')
		parser.add_argument('-n', '--es-index',
			dest = 'es_index',
			help='ElasticSearch index.')
		parser.add_argument('-y', '--es-type',
			dest = 'es_type',
			help = 'ElasticSearch type.')
		parser.add_argument('-t', '--threads',
			type = int,
			choices = range(1, 17),
			help = 'Number of threads to deploy.')
		parser.add_argument('-r', '--read-buffer',
			type = int,
			dest = 'read_buffer',
			help = 'Size if the buffer used to read from the MySQL databases.')
		parser.add_argument('-w', '--write-buffer',
			type = int,
			dest = 'write_buffer',
			help = 'Size of the buffer used to write to the ElasticSearch servers.')
		parser.add_argument('-l', '--limit',
			type = int,
			help = 'Limit the number of documents to index.')
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
		if args.es_connections is not None:
			self.es_connections = self.readEsConnections('es', args.es_connections)

		# elasticsearch index name
		if args.es_index is not None:
			self.es_index = args.es_index
		# else:
		# 	self.es_index = self.es_index.format(args.prefix)

		# elasticsearch index type
		if args.es_type is not None:
			self.es_type = args.es_type

		# database name
		if args.db_name is not None:
			self.db_name = args.db_name
		# else:
		# 	self.db_name = self.db_name.format(args.prefix)

		# database connections
		if args.db_connections is not None:
			self.db_connections = self.readConnections('db', args.db_connections)
		else:
			for i, dbConnection in enumerate(self.db_connections):
				self.db_connections[i] = list(dbConnection)

		for dbConnection in self.db_connections:
			dbConnection.insert(0, self.db_name)

		# databases queue size
		if args.db_queue_size is not None:
			self.db_queue_size = args.db_queue_size

		# threads
		if args.threads is not None:
			self.threads = args.threads

		# read buffer size
		if args.read_buffer is not None:
			self.read_buffer = args.read_buffer

		# write buffer size
		if args.write_buffer is not None:
			self.write_buffer = args.write_buffer

		# documents limit
		if args.limit is not None:
			self.limit = args.limit

	def load(self, development=False):
		if development:
			config = {
				'verbose': 3,
				'es_connections': [
					('http', '192.168.0.1', '9200')
				],
				'es_index': 'dmoz',
				'es_type': 'category',
				'db_connections': [
					('127.0.0.1', 'elastico', 'elastico'),
				],
				'db_name': 'dmoz',
				'db_queue_size': 4,
				'threads': 16,
				'read_buffer': 100,
				'write_buffer': 1000,
				'limit': 2000,
			}
		else:
			config = {
				'verbose': 0,
				'es_connections': [
					('http', '127.0.0.1', '9200')
				],
				'es_index': 'dmoz',
				'es_type': 'category',
				'db_connections': [
					('127.0.0.1', 'elastico', 'elastico'),
				],
				'db_name': 'dmoz',
				'db_queue_size': 4,
				'threads': 16,
				'read_buffer': 100,
				'write_buffer': 1000,
				'limit': None,
			}

		return config

	def readConnections(self, type, file):
		"""Read connections configuration from file"""
		if type == 'es':
			connection = self.es_connection
		else:
			connection = self.db_connection

		list = []
		for line in file:
			try:
				list.append(connection(line.split()))
			except ValueError:
				continue
		if list is not []:
			return list

	def es_connection(self, line):
		"""Read connection configuration for Elasticsearch"""
		connection = (hostname, username, password) = line
		return connection

	def db_connection(self, line):
		"""Read connection configuration for MySQL"""
		connection = (schema, hostname, port) = line
		return connection
