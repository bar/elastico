#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""BaseIndexer.py: Abstract class that takes care of the indexing process."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

from abc import ABCMeta, abstractmethod

from utils import ThreadWatcher # Thread control
from sqlsoup import TableClassType
from threading import Lock
from sys import exit


class BaseIndexer(object):
	"""BaseIndexer class.

	Retrieves the necessary information from MySQL, that is the main model and its associations.
	Later, builds the Elasticsearch documents according the the definitions and indexes them.
	"""

	__metaclass__ = ABCMeta

	buffer = None
	count = 0
	empty = False
	lock = Lock()
	total = 0
	read_buffer_size = 10
	write_buffer_size = 1000

	def __init__(self, Model, limit=None):
		# Producer
		self.produce(Model, limit)

		# Manage threads
		ThreadWatcher.ThreadWatcher()

	@abstractmethod
	def produce(self, Model, limit):
		"""Fills the buffer with a generator based on MySQL ids that will be later used as the entry point
		to construct each objects.

		It handles the creation of the MySQL models that will be used as the source of information.

		http://stackoverflow.com/questions/7389759/memory-efficient-built-in-sqlalchemy-iterator-generator
		http://www.sqlalchemy.org/trac/wiki/UsageRecipes/WindowedRangeQuery

		http://stackoverflow.com/questions/1078383/sqlalchemy-difference-between-query-and-query-all-in-for-loops
		http://www.mail-archive.com/sqlalchemy@googlegroups.com/msg12443.html
		http://stackoverflow.com/questions/1145905/scanning-huge-tables-with-sqlalchemy-using-the-orm

		"""
		raise NotImplementedError()

	@abstractmethod
	def consume(self,
		start_time,
		thread_name,
		db_model_queue,
		es_server,
		es_index,
		es_type,
		read_buffer_size,
		write_buffer_size):
		"""Retrieves data from models an its associations.

		It handles the creation of Elasticsearch documents, and the index process.

		Args:
			start_time (float): Start time in seconds.
			thread_name (string): Thread name
			db_model_queue:
			es_server (string): Elasticsearch server.
			es_index (string): Elasticsearch index.
			es_type (string): Elasticsearch type.
			read_buffer_size (integer): Number of objects to accumulate before indexing.
			write_buffer_size (integer): Number of objects to accumulate in each index chunk.
		"""
		raise NotImplementedError()
