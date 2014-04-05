#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""SQLSoup.py: Database connector using SQLSoup."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

from abc import ABCMeta, abstractmethod

# Errors
from utils.errors import (
	BadConfigError,
	ConnectorError
)

# SQLAlchemy
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
#from sqlalchemy.exc import *

# SQLSoup
import sqlsoup


class Connector(object):
	"""Database connector.

	Used as the MySQL repository connector.

	Attributes:
		url (string): Engine url
	"""

	_db = None

	url = None

	def __init__(self, connection, engine='mysql'):
		"""Conenctor initialization."""
		try:
			name, hostname, username, password = connection
		except ValueError:
			raise BadConfigError
		self.url = '{:s}://{:s}:{:s}@{:s}/{:s}'.format(engine, username, password, hostname, name)

	def db(self, session=True, db_charset='utf8'):
		"""Construct the db object used to access the database.

		Note: Using session with autocommit=True is better when working with threads, and avoids calling Soup.session.commit()
		http://forum.griffith.cc/index.php?topic=1082.0

		Creates one session per db object.

		Args:
			session (mixed): Session registry to use for the connector.
				None, let sqlsoup.SQLSoup use its own default session registry.
				True, use the connector custom session registry.
				Session registry object
			db_charset (string): DB connection character set.

		Returns:
			sqlsoup.SQLSoup object.
		"""

		if self.url is None:
			raise BadConfigError('No engine URL configured.')

		engine = '%s?charset=%s' % (self.url, db_charset)

		if session is True:
			session = self.session()

		self._db = sqlsoup.SQLSoup(engine, session=session)

		return self

	def session(self):
		"""Connector custom session registry.

		Returns:
			sqlalchemy.orm.scoping.scoped_session
		"""
		# return sqlsoup.scoped_session(sqlsoup.sessionmaker(
		# 	autoflush=True,
		# 	expire_on_commit=True,
		# 	autocommit=False))
		return sqlsoup.scoped_session(sqlsoup.sessionmaker(
			autoflush=False,
			expire_on_commit=False,
			autocommit=True))

	def build(self, source_table, relationships=False):
		"""Returns the constructed model.

		Bind relationships.

		http://docs.sqlalchemy.org/en/latest/orm/relationships.html#adjacency-list-relationships

		Args:
			source_table (string): Source table name.
			relationships (dict): Models that should be binded to the source model.

		Returns:
			sqlsoup.TableClassType object.

		TODO:
			Maybe later read from config models and its relations.
		"""

		source_model = Connector.model(self._db, source_table)

		if relationships is False:
			return source_model

		if not self.is_database(self._db):
			raise BadConfigError('No database connector configured.')
		db = self._db

		if 'one_to_many' in relationships:
			for relationship_table, relationship in relationships['one_to_many'].iteritems():
				related_model = self.model(self._db, relationship_table)
				foreign_key = self.field(related_model, relationship['foreign_key'])
				source_model.relate(
					relationship_table,
					#relationship['name'],
					related_model,
					foreign_keys=foreign_key,
					primaryjoin=(foreign_key == source_model.id),
					backref=source_table)
					# backref=source_name)
		# import ipdb; ipdb.set_trace()
		return self

	@staticmethod
	def is_database(object):
		"""Test if an object is an sqlsoup.SQLSoup database.

		Args:
			object: Object to test.

		Returns:
			Boolean.
		"""
		return isinstance(object, sqlsoup.SQLSoup)

	@staticmethod
	def is_model(object):
		"""Test if an object is a mapped model.

		Args:
			object: Object to test.

		Returns:
			Boolean.
		"""
		return isinstance(object, sqlsoup.TableClassType)

	@staticmethod
	def model(db, table_name):
		"""Gets a Mapped object based on an existing table.

		Args:
			table_name (string): Model name.

		Returns:
			Mapped model if exists, None otherwise.
		"""
		if not Connector.is_database(db):
			raise BadConfigError('No database connector configured.')

		return db.entity(table_name)

	@staticmethod
	def field(model, field_name):
		"""Gets a field from a mapped model.

		Args:
			model: Mapped model
			field_name (string): Mapped field

		Returns:
			Mapped object if exists, None otherwise.
		"""
		if not Connector.is_model(model):
			raise ConnectorError('No database connector configured.')

		return getattr(model, field_name, None)
