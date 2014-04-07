#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""BaseIndexer.py: Abstract class that takes care of the indexing process."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

from abc import ABCMeta, abstractmethod

# Threading
import threading

# Inflection
import inflection

# Thread control
from utils import ThreadWatcher


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
	_document_map = {}
	_read_queue = None
	_es_connector = None
	_es_index = None
	_es_type = None

	index_buffer = None
	index_count = 0
	index_total = 0
	buffer_empty = False
	read_chunk_size = 10

	def __init__(self,
		db_connector,
		read_queue,
		es_connector,
		es_index,
		es_type,
		document_map={},
		limit=None):
		"""Initialization.

		Args:
			db_connector: Database connector.
			read_queue (queue.Queue): Queue filled with models to be indexed.
			es_connector (string): Elasticsearch connector.
			es_index (string): Elasticsearch index.
			es_type (string): Elasticsearch type.
			document_map (dict): Dict used for mapping the models to the document structure.
			limit (int): Number of documents to index
		"""

		self.fill_buffer(db_connector._model, limit)

		self._read_queue = read_queue
		self._es_connector = es_connector
		self._es_index = es_index
		self._es_type = es_type
		self._document_map = self.document_map(db_connector) if document_map in [None, {}] else document_map

		# Threads manager
		ThreadWatcher.ThreadWatcher()

	def document_map(self, db_connector):
		"""Builds the mapping between the models and the document structure.

		If no document map is set, one will be created using the model information.

		{
			'CamelCaseModel': 'table_name'
		}

		Args:
			db_connector: Database connector.
		"""
		table = db_connector.table_name(db_connector._model)
		return {table: table}

	@abstractmethod
	def fill_buffer(self, model, limit):
		"""Populates the buffer.

		Fills the read buffer with a generator based on MySQL ids that will be later used as the entry point
		to construct the documents to be indexed.

		It handles the creation of the MySQL models that will be used as the source of information.

		Args:
			model: Model used as an entry point to populate the buffer and create the documents.
			limit (int): Number of documents to index
		"""
		raise NotImplementedError()

	@abstractmethod
	def index(self, start_time, read_chunk_size):
		"""Indexes the buffered items.

		Retrieves data from models an its associations.

		It handles the creation of Elasticsearch documents, and the indexing process.

		Args:
			start_time (float): Start time in seconds.
			read_chunk_size (integer): Number of objects to accumulate before indexing.
		"""
		raise NotImplementedError()
