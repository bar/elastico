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

	def __init__(self, Model, limit=None):
		self.count = 0
		self.empty = False

		# Producer
		self.produce(Model, limit)

		# Lock
		self.lock = Lock()

		# Manage threads
		ThreadWatcher.ThreadWatcher()

	@abstractmethod
	def produce(self, Model, limit=None):
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
	def consume(self, start_time, thread_name, db_model_queue, es_server, es_index, es_type, read_buffer_size=10, write_buffer_size=1000):
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

	@abstractmethod
	def buildModel(self, Model):
		"""One liner description

		Args:
			param1 (type): description

		Returns:
			type: description
		"""
		raise NotImplementedError()

	# @abstractmethod
	# def fetch_condition(self, site, filter):
	# 	raise NotImplementedError()

	def getMappedObject(self, Mapped, relation):
		""" Gets a Mapped object based on an existing relation.

		:type Mapped: object
		:param Mapped: Mapped object from where we get its relation.

		:type relation: string
		:param relation: String with the model to relate to.

		:rtype: mixed
		:return: Mapped object if exists, None otherwise.
		"""

		return getattr(Mapped, relation)

	def belongsTo(self, Mapped, relation):
		""" Gets a Mapped object based on an existing belongsTo relation.

		:type Mapped: object
		:param Mapped: Mapped object from where we get its relation.

		:type relation: string
		:param relation: String with the model to relate to.

		:rtype: mixed
		:return: Mapped object if exists, None otherwise.
		"""

		return self.getMappedObject(Mapped, relation)

	def hasMany(self, Mapped, relation):
		""" Gets a Mapped object based on an existing hasMany relation.

		:type Mapped: object
		:param Mapped: Mapped object from where we get its relation.

		:type relation: string
		:param relation: String with the model to relate to.

		:rtype: mixed
		:return: Mapped object if exists, None otherwise.
		"""
		return self.getMappedObject(Mapped, relation)

	def hasAndBelongsToMany(self, Mapped, relation):
		""" Gets a Mapped object based on an existing hasAndBelongsToMany relation.

		:type Mapped: object
		:param Mapped: Mapped object from where we get its relation.

		:type relation: string
		:param relation: String with the model to relate to.

		:rtype: mixed
		:return: Mapped object if exists, None otherwise.
		"""
		return self.getMappedObject(Mapped, relation)

	def getEmptySchema(self, table):
		""" Get empty structure of a table.

		:type table: object
		:param table: Mapped object table.

		:rtype: dict
		:return: A dict with column names with None as values.
		"""
		try:
			schema = {}
			for c in table.c.keys():
				schema[c] = None

			return schema

		except NoSuchTableError, e:
			print('There is no such a table called {}').format(e.args[0])
			exit(1)

	def _mappedToModel(self, Mapped):
		""" Map an SqlSoup object to a CakePHP model.

		:type Mapped: object
		:param Mapped: Mapped object to convert.

		:rtype: dict
		:return: Mapped values converted to dict.
		"""

		if not isinstance(type(Mapped), TableClassType):
			return None

		schema = self.getEmptySchema(Mapped._table)
		mappedObjectDict = Mapped.__dict__

		data = {}
		for k in schema:
			data[k] = mappedObjectDict[k]

		return data

	def mappedToModel(self, Mapped):
		""" Convert a Mapped object into a dict.

		If Mapped has other maps inside, also converts them too.

		:type Mapped: object
		:param Mapped: Mapped object to convert.

		:rtype: list
		:return: A list with dicts of converted mapped objects.
		"""
		if isinstance(Mapped, list):
			Model = []
			for v in Mapped:
				_v = self._mappedToModel(v)
				if _v != None:
					Model.append(_v)
			return Model
		else:
			return self._mappedToModel(Mapped)

	def translateModel(self, Mapped):
		""" Converts to dict all members of dict of mapped objects.

		:type Mapped: dict
		:param Mapped: Dict with mapped object as its values.

		:rtype: dict
		:return: Dict with mapped values converted.
		"""
		Model = {}
		for key in Mapped:
			Model[key] = self.mappedToModel(Mapped[key])

		return Model
