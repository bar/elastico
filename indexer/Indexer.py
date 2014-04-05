#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Indexer.py: Indexer."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

from BaseIndexer import BaseIndexer
from sqlalchemy import func

import itertools # iterate faster
import time

# Utility functions
from utils.utils import vprint, tprint, uniqify

# Inflection
import inflection


class Indexer(BaseIndexer):
	"""Indexer class"""

	# def __init__(self, model, limit=None):
	# 	super(Indexer, self).__init__(model, limit)

	def fill_buffer(self, model, limit=None):
		"""Populates the buffer."""
		where = True

		total = model.session.query(func.count(model.id)).filter(where).scalar()
		self.index_total = min(total, limit) if limit not in [None, 0] else total
		vprint('Number of elements to index: ' + str(self.index_total))

		vprint('Populating the buffer...')

		query = model.filter(where)
		if limit not in [None, 0]:
			query = query.limit(limit)
		self.index_buffer = query.values(model.id)

		vprint('Buffer populated.')

	def index(self,
		start_time,
		thread_name,
		read_queue,
		es_connector,
		es_index,
		es_type,
		read_chunk_size=None):
		"""Indexes the buffered items."""

		if read_chunk_size is None:
			read_chunk_size = self.read_chunk_size

		tprint(thread_name, 'Starting...')

		# Sync threads
		time.sleep(0.5)

		# Items to be indexed
		buffer = self.index_buffer

		# Number of elements to index
		total = self.index_total

		try:
			while not self.buffer_empty:
				model_ids = []

				try:
					self._lock.acquire()
					for _ in itertools.repeat(None, read_chunk_size):
						model_ids.append(next(buffer)[0])
				except StopIteration:
					self.buffer_empty = True
					tprint(thread_name, '## Empty buffer... ##')
					if len(model_ids) == 0:
						break
					tprint(thread_name, 'Dying...')
				finally:
					self._lock.release()

				source_model = read_queue.get(True)
				# import ipdb; ipdb.set_trace()

				documents = []
				for filtered_model in source_model.filter(source_model.id.in_(model_ids)):
					documents.append((filtered_model.id, self.build_document(filtered_model)))
				#source_model.session.close()
				read_queue.put(source_model)

				for document in documents:
					print document
					# es_connector.index(document[1], es_index, es_type, document[0], bulk = True)

				count = self.index_count = self.index_count + len(model_ids)

				if count % 10 == 0:
					tprint(thread_name, '{:d}/{:d} ({:.2%}) {{{:f}}}'.format(count, total, float(count) / total, time.time() - start_time), 1)

				if count % 1000 == 0:
					pass
					# es_connector.refresh()


		except Exception, e:
			print(type(e))
			raise
		except (KeyboardInterrupt, SystemExit):
			exit(0)

		tprint(thread_name, 'Ending... I\'m dead')

	def build_document(self, source_model):
		# one_to_many = self.one_to_many
		# many_to_many = self.many_to_many
		# one_to_one = self.one_to_one
		# many_to_one = self.many_to_one

		# return self

		# translations here
		# Tags = many_to_many(source_model, 'Tag')

		# Categories = []
		# for Tag in Tags:
		# 	for Category in many_to_many(Tag, 'Category'):
		# 		Categories.append(Category)
		# Categories = uniqify(Categories)

		# Zones = []
		# Zone = many_to_one(source_model, 'Zone')
		# while Zone:
		# 	Zones.append(Zone)
		# 	ParentZone = many_to_one(Zone, 'ParentZone')
		# 	Zone = ParentZone
		mapped_model = {
			'Category': source_model,
			'AlternativeLanguages': source_model.alternative_languages,
			'NewsGroups': source_model.news_groups,
			'ExternalPages': source_model.external_pages,
		# 	'Element': source_model,

		# 	# many_to_one
		# 	#'User': many_to_one(source_model, 'User'),
		# 	'Zone': Zones,
		# 	'Product': many_to_one(source_model, 'Product'),
		# 	#'Chain': many_to_one(source_model, 'Chain'),
		# 	#'Campaign': self.many_to_one(source_model, 'Campaign'),

		# 	# one_to_many
		# 	#'Phone': one_to_many(source_model, 'Phone'),
		# 	'Review': one_to_many(source_model, 'Review'),
		# 	'Bookmark': one_to_many(source_model, 'Bookmark'),

		# 	# habtm
		# 	'Tag': Tags,
		# 	'Category': Categories,
		# 	'Ware': many_to_many(source_model, 'Ware'),
		# 	'Brand': many_to_many(source_model, 'Brand'),
		}
		import ipdb; ipdb.set_trace()
		return mapped_model

		return self.translate_mapped(mapped_model)

	def translate_mapped(self, mapped_model):
		"""Converts to dict all members of dict of mapped objects.

		Args:
			mapped_model: Dict with mapped object as its values.

		Returns:
			Dict with mapped values converted.
		"""
		model = {}
		for key in mapped_model:
			model[key] = self.mappedToModel(mapped_model[key])

		return model

	def empty_schema(self, mapped_model):
		"""Create an empty schema for the givven table.

		Args:
			mapped_model: Mapped model.

		Returns:
			A dict with {column_name: None}.
		"""
		try:
			return {k:None for k in mapped_model.c.keys()}
		except NoSuchTableError, e:
			raise ConnectorError
		# schema = {}
		# try:
		# 	for c in mapped_model.c.keys():
		# 		schema[c] = None
		# except NoSuchTableError, e:
		# 	raise ConnectorError
		# return schema

	def _mapped_to_model(self, mapped_model):
		""" Map an SqlSoup object to a CakePHP model.

		:type Mapped: object
		:param Mapped: Mapped object to convert.

		:rtype: dict
		:return: Mapped values converted to dict.
		"""

		if not isinstance(type(mapped_model), TableClassType):
			return None

		schema = self.empty_schema(mapped_model._table)
		mappedObjectDict = mapped_model.__dict__

		data = {}
		for k in schema:
			data[k] = mappedObjectDict[k]

		return data

	def mappedToModel(self, mapped_model):
		""" Convert a Mapped object into a dict.

		If Mapped has other maps inside, also converts them too.

		:type Mapped: object
		:param mapped_model: Mapped object to convert.

		:rtype: list
		:return: A list with dicts of converted mapped objects.
		"""
		if isinstance(mapped_model, list):
			model = []
			for v in mapped_model:
				_v = self._mapped_to_model(v)
				if _v != None:
					model.append(_v)
			return model
		else:
			return self._mapped_to_model(mapped_model)

	def translateModel(self, mapped_model):
		""" Converts to dict all members of dict of mapped objects.

		:type Mapped: dict
		:param mapped_model: Dict with mapped object as its values.

		:rtype: dict
		:return: Dict with mapped values converted.
		"""
		model = {}
		for key in mapped_model:
			model[key] = self.mappedToModel(mapped_model[key])

		return model