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
import sqlalchemy

# SQLSoup
import sqlsoup


class Connector(object):
	"""Database connector.

	Used as the MySQL repository connector.
	"""

	_db = None
	_model = None
	_url = None

	def __init__(self, connection, engine='mysql', session=True, db_charset='utf8'):
		"""Conenctor initialization."""
		try:
			name, hostname, username, password = connection
		except ValueError:
			raise BadConfigError

		self._url = '{:s}://{:s}:{:s}@{:s}/{:s}'.format(engine, username, password, hostname, name)
		self.db(session, db_charset)

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

		The object represents a SQLSoup mapping to a `sqlalchemy.schema.Table` construct.

		If already queried, the type of the object will hold the same value.

		Args:
			object: Object to test.

		Returns:
			Boolean.
		"""
		return isinstance(object, sqlsoup.TableClassType) or isinstance(type(object), sqlsoup.TableClassType)

	@staticmethod
	def field(model, field_name):
		"""Gets a field from a mapped model.

		Args:
			model: Mapped model.
			field_name (string): Mapped field

		Returns:
			Mapped object if exists, None otherwise.
		"""
		if not Connector.is_model(model):
			raise ConnectorError('No model found.')

		return getattr(model, field_name, None)

	@staticmethod
	def table_name(model):
		if not Connector.is_model(model):
			raise ConnectorError('No model found.')

		return model._table.fullname

	def map(self, mapping):
		"""Builds a dict containing the mapping structure.

		Args:
			document_map (dict): Dict used for mapping the models to the document structure.

		Returns:
			Dictionary containing the mapping structure.
		"""
		mapped_model = {}
		for model_alias, table_name in mapping.iteritems():
			try:
				mapped_model[model_alias] = self.model(table_name)
			except sqlalchemy.exc.NoSuchTableError:
				continue

		return mapped_model

	def model(self, table_name):
		"""Gets a Mapped object based on an existing table.

		Args:
			table_name (string): Model name.

		Returns:
			Mapped model.
		"""
		if not self.is_database(self._db):
			raise BadConfigError('No database connector configured.')

		return self._db.entity(table_name)

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
		if self._url is None:
			raise BadConfigError('No engine URL configured.')

		engine = '%s?charset=%s' % (self._url, db_charset)

		if session is True:
			session = self.session()

		self._db = sqlsoup.SQLSoup(engine, session=session)

		return self

	def session(self):
		"""Custom session registry.

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

		Set an object parameter with the connector used to construct it.
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
		self._model = source_model = self.model(source_table)

		if relationships is False:
			return self

		if 'one_to_many' in relationships:
			for relationship_name, relationship in relationships['one_to_many'].iteritems():
				relationship_table = relationship['table'] if 'table' in relationship else relationship_name
				related_model = self.model(relationship_table)
				foreign_key = self.field(related_model, relationship['foreign_key'])

				source_model.relate(
					relationship_name,
					related_model,
					foreign_keys=foreign_key,
					primaryjoin=(foreign_key == source_model.id),
					backref=source_table)

		if 'many_to_one' in relationships:
			for relationship_name, relationship in relationships['many_to_one'].iteritems():
				relationship_table = relationship['table'] if 'table' in relationship else relationship_name
				related_model = self.model(relationship_table)
				foreign_key = self.field(source_model, relationship['foreign_key'])

				related_model.relate(
					relationship_name,
					source_model,
					foreign_keys=foreign_key,
					primaryjoin=(foreign_key == related_model.id),
					backref=relationship_table)

		if 'many_to_many' in relationships:
			for relationship_name, relationship in relationships['many_to_many'].iteritems():
				pass

		if 'one_to_one' in relationships:
			for relationship_name, relationship in relationships['one_to_one'].iteritems():
				pass

		if 'self_referencial' in relationships:
			for relationship_name, relationship in relationships['self_referencial'].iteritems():
				backref = relationship['backref'] if 'backref' in relationship else None
				foreign_key = self.field(source_model, relationship['foreign_key'])

				source_model.relate(
					relationship_name,
					source_model,
					foreign_keys=foreign_key,
					primaryjoin=(foreign_key == source_model.id),
					remote_side=source_model.id,
					backref=backref)

		return self
