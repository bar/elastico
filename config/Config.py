#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Config.py: App configurator."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

# from argparse import FileType
from argparse import ArgumentParser

VERBOSE = 0


class Config:
	"""Configuration object.

	Used to easy bootstrapping.
	"""

	db_name = None
	db_connections = None
	db_queue_size = None
	es_connections = None
	es_index = None
	es_type = None
	limit = None
	read_chunk_size = None
	single_process_mode = None
	threads = None
	autostart_threads = None
	verbose = None
	write_chunk_size = None

	def __init__(self):
		args = self.parse_args()
		self.set_verbose(args.verbose)
		self.set_args(args)

		if self.db_connections in [[], None] or self.es_connections in [[], None]:
			raise BadConfigError

		self.print_info()

	def convert_arg_line_to_args(self, arg_line):
		"""Fancy parsing.

		Converts arguments lines from files.

		Args:
			arg_line (string): Argument line.

		http://docs.python.org/2/library/argparse.html#argparse.ArgumentParser.convert_arg_line_to_args
		"""
		for arg in arg_line.split():
			if not arg.strip():
				continue
			yield arg

	def parse_args(self):
		"""Get arguments."""
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
		parser.add_argument('-s', '--single-process-mode',
			dest = 'single_process_mode',
			action = 'store_true',
			help = 'Whether the program should run in single process mode.')
		parser.add_argument('-t', '--threads',
			type = int,
			choices = range(1, 17),
			help = 'Number of threads to deploy.')
		parser.add_argument('-a', '--autostart-threads',
			dest = 'autostart_threads',
			action = 'store_true',
			help = 'Whether the threads should be run immediately after created or synched.')
		parser.add_argument('-r', '--read-chunk-size',
			type = int,
			dest = 'read_chunk_size',
			help = 'Size of the read chunk used when reading from the buffer.')
		parser.add_argument('-w', '--write-chunk-size',
			type = int,
			dest = 'write_chunk_size',
			help = 'Size of the write chunk used when indexing the documents.')
		parser.add_argument('-l', '--limit',
			type = int,
			help = 'Limit the number of documents to index.')
		#parser.add_argument('-i', '--input', nargs='?', type = FileType('r'), default = stdin)
		#parser.add_argument('-o', '--output', nargs='?', type = FileType('w'), default = stdout)
		return parser.parse_args()

	def set_verbose(self, verbose):
		"""Export verbositi level."""
		global VERBOSE
		VERBOSE = verbose

	def set_args(self, args):
		"""Set arguments."""
		for k, v in self.load(args.development).items():
			setattr(self, k, v)

		# Verbose mode
		if args.verbose is not None:
			self.verbose = args.verbose

		# Elasticsearch connections
		if args.es_connections is not None:
			self.es_connections = self.readEsConnections('es', args.es_connections)

		# Elasticsearch index name
		if args.es_index is not None:
			self.es_index = args.es_index

		# Elasticsearch index type
		if args.es_type is not None:
			self.es_type = args.es_type

		# Database name
		if args.db_name is not None:
			self.db_name = args.db_name

		# Database connections
		if args.db_connections is not None:
			self.db_connections = self.readConnections('db', args.db_connections)
		else:
			for i, db_connection in enumerate(self.db_connections):
				self.db_connections[i] = list(db_connection)

		for db_connection in self.db_connections:
			db_connection.insert(0, self.db_name)

		# Databases queue size
		if args.db_queue_size is not None:
			self.db_queue_size = args.db_queue_size

		# Single process mode
		if args.single_process_mode is not None:
			self.single_process_mode = args.single_process_mode

		# Threads
		if args.threads is not None:
			self.threads = args.threads
		if args.autostart_threads is not None:
			self.autostart_threads = args.autostart_threads

		# Read buffer size
		if args.read_chunk_size is not None:
			self.read_chunk_size = args.read_chunk_size

		# Write buffer size
		if args.write_chunk_size is not None:
			self.write_chunk_size = args.write_chunk_size

		# Documents limit
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
				'read_chunk_size': 100,
				'write_chunk_size': 1000,
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
				'read_chunk_size': 100,
				'write_chunk_size': 1000,
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

	def print_info(self):
		# Utility functions
		from utils.utils import vprint
		process_mode = 'Single process' if self.single_process_mode else 'Multi threaded'
		vprint('Process mode: {:s}'.format(process_mode))

		if not self.single_process_mode:
			vprint('Threads: {:d}'.format(self.threads))

		vprint('Index: {:s}'.format(self.es_index))
		vprint('Type: {:s}'.format(self.es_type))
		vprint('DB Queue size: {:d}'.format(self.db_queue_size))
		vprint('Read chunk size: {:d}'.format(self.read_chunk_size))
		vprint('Write chunk size: {:d}'.format(self.write_chunk_size))
