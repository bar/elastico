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

	# def __init__(self, Model, limit=None):
		# super(Indexer, self).__init__(Model, limit)

	def produce(self, Model, limit=None):
		"""Produces the dishes."""
		where = True

		total = Model.session.query(func.count(Model.id)).filter(where).scalar()
		self.total = min(total, limit) if limit not in [None, 0] else total
		vprint('Total dishes: ' + str(self.total))

		query = Model.filter(where)
		if limit not in [None, 0]:
			query = query.limit(limit)
		self.buffer = query.values(Model.id)

		vprint('Preparing the food...')

	def consume(self, start_time, thread_name, db_model_queue, es_server, es_index, es_type, read_buffer_size=10, write_buffer_size=1000):
		"""Consumes the dishes."""
		# sync threads
		tprint(thread_name, 'Starting... ({:d} > {:d})'.format(read_buffer_size, write_buffer_size))
		time.sleep(0.5)

		readBuffer = self.buffer
		total = self.total

		try:
			while not self.empty:
				ids = []

				try:
					self.lock.acquire()
					for _ in itertools.repeat(None, read_buffer_size):
						ids.append(next(readBuffer)[0])
				except StopIteration:
					self.empty = True
					tprint(thread_name, '## Empty buffer... bang! ##')
					if len(ids) == 0:
						break
					tprint(thread_name, 'Dying...')
				finally:
					self.lock.release()

				PrimaryModels = []
				Model = db_model_queue.get(True)

				Categories = Model.categories

				for mappedElement in Categories.filter(Categories.id.in_(ids)):
					PrimaryModels.append((mappedElement.id, self.buildModel(mappedElement)))
				#Model.session.close()
				db_model_queue.put(Model)

				for PrimaryModel in PrimaryModels:
					print PrimaryModel
					# es_server.index(PrimaryModel[1], es_index, es_type, PrimaryModel[0], bulk = True)

				count = self.count = self.count + len(ids)

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
	# 		Model = []
	# 		for v in Mapped:
	# 			_v = self._mappedToModel(v)
	# 			if _v != None:
	# 				Model.append(_v)
	# 		return Model
	# 	else:
	# 		return self._mappedToModel(Mapped)

	def translateMapped(self, Mapped):
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
