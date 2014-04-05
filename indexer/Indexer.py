#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Indexer.py: Indexer."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

from BaseIndexer import BaseIndexer
from sqlalchemy import func

# Utility functions
from utils.utils import vprint, tprint

import itertools # iterate faster
import time


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
		es_server,
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

				model = read_queue.get(True)
				import ipdb; ipdb.set_trace()

				Categories = model.categories

				documents = []
				for mappedElement in Categories.filter(Categories.id.in_(model_ids)):
					documents.append((mappedElement.id, self.buildModel(mappedElement)))
				#model.session.close()
				read_queue.put(model)

				for document in documents:
					print document
					# es_server.index(document[1], es_index, es_type, document[0], bulk = True)

				count = self.index_count = self.index_count + len(model_ids)

				if count % 10 == 0:
					tprint(thread_name, '{:d}/{:d} ({:.2%}) {{{:f}}}'.format(count, total, float(count) / total, time.time() - start_time), 1)

				if count % 1000 == 0:
					es_server.refresh()


		except Exception, e:
			print(type(e))
			raise
		except (KeyboardInterrupt, SystemExit):
			exit(0)

		tprint(thread_name, 'Ending... I\'m dead')

	def buildModel(self, PrimaryModel):
		has_many = self.has_many
		belongs_to_many = self.belongs_to_many
		belongs_to = self.belongs_to
		has_one = self.has_one

		return self

		# translations here
		# Tags = belongs_to_many(PrimaryModel, 'Tag')

		# Categories = []
		# for Tag in Tags:
		# 	for Category in belongs_to_many(Tag, 'Category'):
		# 		Categories.append(Category)
		# Categories = uniqify(Categories)

		# Zones = []
		# Zone = belongs_to(PrimaryModel, 'Zone')
		# while Zone:
		# 	Zones.append(Zone)
		# 	ParentZone = belongs_to(Zone, 'ParentZone')
		# 	Zone = ParentZone

		return self.translateMapped({
		# 	'Element': PrimaryModel,

		# 	# belongs_to
		# 	#'User': belongs_to(PrimaryModel, 'User'),
		# 	'Zone': Zones,
		# 	'Product': belongs_to(PrimaryModel, 'Product'),
		# 	#'Chain': belongs_to(PrimaryModel, 'Chain'),
		# 	#'Campaign': self.belongs_to(PrimaryModel, 'Campaign'),

		# 	# has_many
		# 	#'Phone': has_many(PrimaryModel, 'Phone'),
		# 	'Review': has_many(PrimaryModel, 'Review'),
		# 	'Bookmark': has_many(PrimaryModel, 'Bookmark'),

		# 	# habtm
		# 	'Tag': Tags,
		# 	'Category': Categories,
		# 	'Ware': belongs_to_many(PrimaryModel, 'Ware'),
		# 	'Brand': belongs_to_many(PrimaryModel, 'Brand'),
		})

	# def getEmptySchema(self, table):
	# 	"""Get empty structure of a table.

	# 	Args:
	# 		table: Mapped object table.

	# 	Returns:
	# 		A dict with column names with None as values.
	# 	"""
	# 	try:
	# 		schema = {}
	# 		for c in table.c.keys():
	# 			schema[c] = None

	# 		return schema

	# 	except NoSuchTableError, e:
	# 		print('There is no such a table called {}').format(e.args[0])
	# 		exit(1)

	# def _mappedToModel(self, Mapped):
	# 	"""Map an SqlSoup object to a CakePHP model.

	# 	Args:
	# 		Mapped: Mapped object to convert.

	# 	Returns:
	# 		Mapped values converted to dict.
	# 	"""

	# 	if not isinstance(type(Mapped), TableClassType):
	# 		return None

	# 	schema = self.getEmptySchema(Mapped._table)
	# 	mappedObjectDict = Mapped.__dict__

	# 	data = {}
	# 	for k in schema:
	# 		data[k] = mappedObjectDict[k]

	# 	return data

	# def mappedToModel(self, Mapped):
	# 	"""Convert a Mapped object into a dict.

	# 	If Mapped has other maps inside, also converts them too.

	# 	Args:
	# 		Mapped: Mapped object to convert.

	# 	Returns:
	# 		A list with dicts of converted mapped objects.

	# 	"""
	# 	if isinstance(Mapped, list):
	# 		model = []
	# 		for v in Mapped:
	# 			_v = self._mappedToModel(v)
	# 			if _v != None:
	# 				model.append(_v)
	# 		return model
	# 	else:
	# 		return self._mappedToModel(Mapped)

	def translateMapped(self, Mapped):
		"""Converts to dict all members of dict of mapped objects.

		Args:
			Mapped: Dict with mapped object as its values.

		Returns:
			Dict with mapped values converted.
		"""
		model = {}
		for key in Mapped:
			model[key] = self.mappedToModel(Mapped[key])

		return model

	# def getMappedObject(self, Mapped, relation):
	# 	"""Gets a Mapped object based on an existing relation.

	# 	Args:
	# 		Mapped: Mapped object from where we get its relation.
	# 		relation: String with the model to relate to.

	# 	Returns:
	# 		Mapped object if exists, None otherwise.
	# 	"""
	# 	return getattr(Mapped, relation)

	# def belongs_to(self, Mapped, relation):
	# 	"""Gets a Mapped object based on an existing belongs_to relation.

	# 	Args:
	# 		Mapped: Mapped object from where we get its relation.
	# 		relation: String with the model to relate to.

	# 	Returns:
	# 		Mapped object if exists, None otherwise.
	# 	"""
	# 	return self.getMappedObject(Mapped, relation)

	# def has_many(self, Mapped, relation):
	# 	"""Gets a Mapped object based on an existing has_many relation.

	# 	Args:
	# 		Mapped: Mapped object from where we get its relation.
	# 		relation: String with the model to relate to.

	# 	Returns:
	# 		Mapped object if exists, None otherwise.
	# 	"""
	# 	return self.getMappedObject(Mapped, relation)

	# def belongs_to_many(self, Mapped, relation):
	# 	"""Gets a Mapped object based on an existing belongs_to_many relation.

	# 	Args:
	# 		Mapped: Mapped object from where we get its relation.
	# 		relation: String with the model to relate to.

	# 	Returns:
	# 		Mapped object if exists, None otherwise.
	# 	"""

	# 	return self.getMappedObject(Mapped, relation)


	# def getEmptySchema(self, table):
	# 	""" Get empty structure of a table.

	# 	:type table: object
	# 	:param table: Mapped object table.

	# 	:rtype: dict
	# 	:return: A dict with column names with None as values.
	# 	"""
	# 	try:
	# 		schema = {}
	# 		for c in table.c.keys():
	# 			schema[c] = None

	# 		return schema

	# 	except NoSuchTableError, e:
	# 		print('There is no such a table called {}').format(e.args[0])
	# 		exit(1)

	# def _mappedToModel(self, Mapped):
	# 	""" Map an SqlSoup object to a CakePHP model.

	# 	:type Mapped: object
	# 	:param Mapped: Mapped object to convert.

	# 	:rtype: dict
	# 	:return: Mapped values converted to dict.
	# 	"""

	# 	if not isinstance(type(Mapped), TableClassType):
	# 		return None

	# 	schema = self.getEmptySchema(Mapped._table)
	# 	mappedObjectDict = Mapped.__dict__

	# 	data = {}
	# 	for k in schema:
	# 		data[k] = mappedObjectDict[k]

	# 	return data

	# def mappedToModel(self, Mapped):
	# 	""" Convert a Mapped object into a dict.

	# 	If Mapped has other maps inside, also converts them too.

	# 	:type Mapped: object
	# 	:param Mapped: Mapped object to convert.

	# 	:rtype: list
	# 	:return: A list with dicts of converted mapped objects.
	# 	"""
	# 	if isinstance(Mapped, list):
	# 		model = []
	# 		for v in Mapped:
	# 			_v = self._mappedToModel(v)
	# 			if _v != None:
	# 				model.append(_v)
	# 		return model
	# 	else:
	# 		return self._mappedToModel(Mapped)

	# def translateModel(self, Mapped):
	# 	""" Converts to dict all members of dict of mapped objects.

	# 	:type Mapped: dict
	# 	:param Mapped: Dict with mapped object as its values.

	# 	:rtype: dict
	# 	:return: Dict with mapped values converted.
	# 	"""
	# 	model = {}
	# 	for key in Mapped:
	# 		model[key] = self.mappedToModel(Mapped[key])

	# 	return model