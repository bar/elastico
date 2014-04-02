#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Indexer.py: Indexer."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

from BaseIndexer import BaseIndexer
from sqlalchemy import func


class Indexer(BaseIndexer):
	"""Indexer class"""

	# def __init__(self, Model, limit = None):
		# super(Indexer, self).__init__(Model, limit)

	def produce(self, Model, limit=None):
		where = True

		import ipdb; ipdb.set_trace()

		Categories = Model.categories

		total = Model.session.query(func.count(Categories.id)).filter(where).scalar()
		self.total = min(total, limit) if limit not in [None, 0] else total
		vprint('Total dishes: ' + str(self.total))

		query = Categories.filter(where)
		if limit not in [None, 0]:
			query = query.limit(limit)
		self.buffer = query.values(Categories.id)

		vprint('Preparing the food...')

	def consume(self, startTime, threadName, dbQueue, esServer, esIndex, esType, readBufferSize=10, writeBufferSize=1000):
		# sync threads
		tprint(threadName, 'Starting... ({:d} > {:d})'.format(readBufferSize, writeBufferSize))
		time.sleep(0.5)

		readBuffer = self.buffer
		total = self.total

		try:
			while not self.empty:
				ids = []

				try:
					self.lock.acquire()
					for _ in itertools.repeat(None, readBufferSize):
						ids.append(next(readBuffer)[0])
				except StopIteration:
					self.empty = True
					tprint(threadName, '## Empty buffer... bang! ##')
					if len(ids) == 0:
						break
					tprint(threadName, 'Dying...')
				finally:
					self.lock.release()

				Elements = []
				Model = dbQueue.get(True)

				Categories = Model.categories

				for mappedElement in Categories.filter(Categories.id.in_(ids)):
					Elements.append((mappedElement.id, self.buildModel(mappedElement)))
				#Model.session.close()
				dbQueue.put(Model)

				for Element in Elements:
					esServer.index(Element[1], esIndex, esType, Element[0], bulk = True)

				count = self.count = self.count + len(ids)

				if count % 10 == 0:
					tprint(threadName, '{:d}/{:d} ({:.2%}) {{{:f}}}'.format(count, total, float(count) / total, time.time() - startTime), 1)

				if count % 1000 == 0:
					esServer.refresh()


		except Exception, e:
			print(type(e))
			raise
		except (KeyboardInterrupt, SystemExit):
			exit(0)

		tprint(threadName, 'Ending... I\'m dead')

	def buildModel(self, Element):
		belongsTo = self.belongsTo
		hasMany = self.hasMany
		hasAndBelongsToMany = self.hasAndBelongsToMany

		Tags = hasAndBelongsToMany(Element, 'Tag')

		Categories = []
		for Tag in Tags:
			for Category in hasAndBelongsToMany(Tag, 'Category'):
				Categories.append(Category)
		Categories = uniqify(Categories)

		Zones = []
		Zone = belongsTo(Element, 'Zone')
		while Zone:
			Zones.append(Zone)
			ParentZone = belongsTo(Zone, 'ParentZone')
			Zone = ParentZone

		return self.translateElement({
			'Element': Element,

			# belongsTo
			#'User': belongsTo(Element, 'User'),
			'Zone': Zones,
			'Product': belongsTo(Element, 'Product'),
			#'Chain': belongsTo(Element, 'Chain'),
			#'Campaign': self.belongsTo(Element, 'Campaign'),

			# hasMany
			#'Phone': hasMany(Element, 'Phone'),
			'Review': hasMany(Element, 'Review'),
			'Bookmark': hasMany(Element, 'Bookmark'),

			# habtm
			'Tag': Tags,
			'Category': Categories,
			'Ware': hasAndBelongsToMany(Element, 'Ware'),
			'Brand': hasAndBelongsToMany(Element, 'Brand'),
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

	# def translateElement(self, Mapped):
	# 	"""Converts to dict all members of dict of mapped objects.

	# 	Args:
	# 		Mapped: Dict with mapped object as its values.

	# 	Returns:
	# 		Dict with mapped values converted.
	# 	"""
	# 	Model = {}
	# 	for key in Mapped:
	# 		Model[key] = self.mappedToModel(Mapped[key])

	# 	return Model

	# def getMappedObject(self, Mapped, relation):
	# 	"""Gets a Mapped object based on an existing relation.

	# 	Args:
	# 		Mapped: Mapped object from where we get its relation.
	# 		relation: String with the model to relate to.

	# 	Returns:
	# 		Mapped object if exists, None otherwise.
	# 	"""
	# 	return getattr(Mapped, relation)

	# def belongsTo(self, Mapped, relation):
	# 	"""Gets a Mapped object based on an existing belongsTo relation.

	# 	Args:
	# 		Mapped: Mapped object from where we get its relation.
	# 		relation: String with the model to relate to.

	# 	Returns:
	# 		Mapped object if exists, None otherwise.
	# 	"""
	# 	return self.getMappedObject(Mapped, relation)

	# def hasMany(self, Mapped, relation):
	# 	"""Gets a Mapped object based on an existing hasMany relation.

	# 	Args:
	# 		Mapped: Mapped object from where we get its relation.
	# 		relation: String with the model to relate to.

	# 	Returns:
	# 		Mapped object if exists, None otherwise.
	# 	"""
	# 	return self.getMappedObject(Mapped, relation)

	# def hasAndBelongsToMany(self, Mapped, relation):
	# 	"""Gets a Mapped object based on an existing hasAndBelongsToMany relation.

	# 	Args:
	# 		Mapped: Mapped object from where we get its relation.
	# 		relation: String with the model to relate to.

	# 	Returns:
	# 		Mapped object if exists, None otherwise.
	# 	"""

	# 	return self.getMappedObject(Mapped, relation)
