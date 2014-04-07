#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Indexer.py: Indexer implementation."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

from BaseIndexer import BaseIndexer

import itertools # iterate faster
import time

# Threading
import threading

# SQLAlchemy
import sqlalchemy

# Errors
from utils.errors import (
	BadConfigError,
	ConnectorError
)

# Utility functions
from utils.utils import vprint, tprint


class Indexer(BaseIndexer):
	"""Indexer class"""

	# def __init__(self, model, limit=None):
	# 	super(Indexer, self).__init__(model, limit)

	def fill_buffer(self, model, limit=None):
		"""Populates the buffer.

		http://stackoverflow.com/questions/7389759/memory-efficient-built-in-sqlalchemy-iterator-generator
		http://www.sqlalchemy.org/trac/wiki/UsageRecipes/WindowedRangeQuery

		http://stackoverflow.com/questions/1078383/sqlalchemy-difference-between-query-and-query-all-in-for-loops
		http://www.mail-archive.com/sqlalchemy@googlegroups.com/msg12443.html
		http://stackoverflow.com/questions/1145905/scanning-huge-tables-with-sqlalchemy-using-the-orm
		"""

		total = model.session.query(func.count(model.id)).filter(where).scalar()
		self.index_total = min(total, limit) if limit not in [None, 0] else total
		vprint('Number of elements to index: ' + str(self.index_total))

		vprint('Populating the buffer...')

		query = model.filter(where)
		if limit not in [None, 0]:
			query = query.limit(limit)
		self.index_buffer = query.values(model.id)

		vprint('Buffer populated.')

	def index(self, start_time=time.time(), read_chunk_size=None):
		"""Indexes the buffered items."""

		process_name = threading.current_thread().getName()

		if read_chunk_size is None:
			read_chunk_size = self.read_chunk_size

		tprint(process_name, 'Starting...')

		# Sync threads
		time.sleep(0.5)

		# Items to be indexed
		buffer = self.index_buffer

		# Number of elements to index
		total = self.index_total

		es_connector = self._es_connector
		es_index = self._es_index
		es_type = self._es_type
		read_queue = self._read_queue

		try:
			while not self.buffer_empty:
				model_ids = []

				try:
					self._lock.acquire()
					for _ in itertools.repeat(None, read_chunk_size):
						model_ids.append(next(buffer)[0])
				except StopIteration:
					self.buffer_empty = True
					tprint(process_name, 'Empty buffer...')
					if len(model_ids) == 0:
						break
					tprint(process_name, '... terminating')
				finally:
					self._lock.release()

				db_connector = read_queue.get(True)

				model = db_connector._model

				documents = []
				for mapped_model in model.filter(model.id.in_(model_ids)):
					documents.append((mapped_model.id, self.build_document(db_connector, mapped_model)))
				#model.session.close()
				read_queue.put(db_connector)

				for document in documents:
					# import pprint
					# pprint.pprint(document)
					es_connector.index(document[1], es_index, es_type, document[0], bulk=True)

				count = self.index_count = self.index_count + len(model_ids)

				if count % 10 == 0:
					tprint(process_name, '{:d}/{:d} ({:.2%}) {{{:f}}}'.format(count, total, float(count) / total, time.time() - start_time))

				if count % 1000 == 0:
					es_connector.indices.refresh()


		except Exception, e:
			print(type(e))
			raise
		except (KeyboardInterrupt, SystemExit):
			exit(0)

		tprint(process_name, 'I\'m dead!')

	def build_document(self, db_connector, mapped_model):
		"""Builds a dict containing the mapping structure for the document.

		Args:
			db_connector: Database connector.
			mapped_model: Model already mapped (filtered).

		Returns:
			Dictionary containing the mapping structure.
		"""
		document_map = db_connector.map(mapped_model, self._document_map)
		return self.translate_document_map(document_map)

	def translate_document_map(self, document_map):
		"""Converts to dict all members of dict of mapped objects.

		Args:
			document_map: Dict with mapped object as its values.

		Returns:
			Dict with mapped values converted.
		"""
		return {k:self.mapped_to_document(v) for k,v in document_map.items()}

	def mapped_to_document(self, mapped_model):
		"""Converts a Mapped model into a document.

		Args:
			mapped_model: Mapped model.

		Returns:
			Dict with mapped values converted.
		:type Mapped: object
		:param mapped_model: Mapped object to convert.

		:rtype: list
		:return: A list with dicts of converted mapped objects.
		"""
		if mapped_model is None:
			return None

		if isinstance(mapped_model, list):
			models = []
			for v in mapped_model:
				models.append(self.mapped_to_document(v))
			return models
		else:
			empty_schema = {k:None for k in mapped_model._table.c.keys()}
			return {k:mapped_model.__dict__[k] for k in empty_schema}
