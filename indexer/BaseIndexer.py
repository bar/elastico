#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""BaseIndexer.py: Abstract class that takes care of the indexing process."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

from abc import ABCMeta, abstractmethod

import threading
from utils import ThreadWatcher # Thread control
from sqlsoup import TableClassType
from sys import exit


class BaseIndexer(object):
	"""BaseIndexer class.

	Retrieves the necessary information from MySQL, that is the main model and its associations.
	Later, builds the Elasticsearch documents according the the definitions and indexes them.

	Attributes:
		index_buffer: Buffer filled of items to be indexed.
		index_count (int): Number of indexed items.
		index_total (int): Number of elements to index.
		read_chunk_size (int): Size of the read chunk used when reading from the buffer.
		buffer_empty (bool): Whether the buffer is empty.
		lock (threading.Lock: Internal threading lock.
	"""

	__metaclass__ = ABCMeta
	_lock = threading.Lock()

	index_buffer = None
	index_count = 0
	index_total = 0
	buffer_empty = False
	read_chunk_size = 10

	def __init__(self, model, limit=None):
		self.fill_buffer(model, limit)

		# Threads manager
		ThreadWatcher.ThreadWatcher()

	@abstractmethod
	def fill_buffer(self, model, limit):
		"""Populates the buffer.

		Fills the read buffer with a generator based on MySQL ids that will be later used as the entry point
		to construct the documents to be indexed.

		It handles the creation of the MySQL models that will be used as the source of information.

		http://stackoverflow.com/questions/7389759/memory-efficient-built-in-sqlalchemy-iterator-generator
		http://www.sqlalchemy.org/trac/wiki/UsageRecipes/WindowedRangeQuery

		http://stackoverflow.com/questions/1078383/sqlalchemy-difference-between-query-and-query-all-in-for-loops
		http://www.mail-archive.com/sqlalchemy@googlegroups.com/msg12443.html
		http://stackoverflow.com/questions/1145905/scanning-huge-tables-with-sqlalchemy-using-the-orm

		Args:
			model (float): Start time in seconds.
			limit (int): Thread name

		"""
		raise NotImplementedError()

	@abstractmethod
	def index(self,
		start_time,
		thread_name,
		read_queue,
		es_server,
		es_index,
		es_type,
		read_chunk_size):
		"""Indexes the buffered items.

		Retrieves data from models an its associations.

		It handles the creation of Elasticsearch documents, and the index process.

		Args:
			start_time (float): Start time in seconds.
			thread_name (string): Thread name
			read_queue (queue.Queue): Queue filled with models to be indexed
			es_server (string): Elasticsearch server.
			es_index (string): Elasticsearch index.
			es_type (string): Elasticsearch type.
			read_chunk_size (integer): Number of objects to accumulate before indexing.
			write_buffer_size (integer): Number of objects to accumulate for each indexing chunk.
		"""
		raise NotImplementedError()
