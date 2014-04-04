#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""SQLSoup.py: Database connector using SQLSoup."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

from abc import ABCMeta, abstractmethod

# Exceptions
from utils.exceptions import BadConfigError

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
		_db: sqlsoup.SQLSoup object

	"""

	url = None

	_db = None

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
			session (mixed): Session to use for the connector.
				None, let sqlsoup.SQLSoup use its own session.
				True, use the connector custom session.
				Session object
			db_charset (string): DB connection character set.

		Returns:
			Object: sqlsoup.SQLSoup object.
		"""

		if self.url is None:
			raise BadConfigError('No engine URL configured.')

		engine = '%s?charset=%s' % (self.url, db_charset)

		if session is True:
			session = self.session()

		self._db = sqlsoup.SQLSoup(engine, session=session)

		return self

	def session(self):
		"""Connector custom session."""
		#return sqlsoup.scoped_session(sqlsoup.sessionmaker(autoflush=True, expire_on_commit=True, autocommit=False))
		return sqlsoup.scoped_session(sqlsoup.sessionmaker(autoflush=False, expire_on_commit=False, autocommit=True))

	# @abstractmethod
	# def bind(self, Soup):
	# 	raise NotImplementedError()

	def is_db(self, object):
		"""Test if an object is an sqlsoup.SQLSoup database."""
		return isinstance(object, sqlsoup.SQLSoup)

	def is_table(self, object):
		"""Test if an object is an sqlsoup.SQLSoup table."""
		return isinstance(object, sqlsoup.TableClassType)

	def model(self, model_name, relationships=False,):
		"""Builds the

		Bind relationships.

		http://docs.sqlalchemy.org/en/latest/orm/relationships.html#adjacency-list-relationships

		Args:
			model_name (string): Source model name.
			relationships (dict): Models that should be binded to the source model.

		Returns:
			Object: sqlsoup.SQLSoup object.

		TODO:
			Maybe later read from config models and its relations.
		"""

		primary_model = self.getMappedObject(model_name)

		if relationships is False:
			return primary_model

		if not isinstance(self._db, sqlsoup.SQLSoup):
			raise BadConfigError('No database connector configured.')
		db = self._db

		return db

	def getMappedObject(self, model_name):
		"""Gets a Mapped object based on an existing relation.

		Args:
			model_name: String with the model to relate to.

		Returns:
			Mapped object if exists, None otherwise.
		"""
		if not isinstance(self._db, sqlsoup.SQLSoup):
			raise BadConfigError('No database connector configured.')

		return getattr(self._db, model_name)

	def belongs_to(self, Mapped, relation):
		"""Gets a Mapped object based on an existing belongs_to relation.

		Args:
			Mapped: Mapped object from where we get its relation.
			relation: String with the model to relate to.

		Returns:
			Mapped object if exists, None otherwise.
		"""
		return self.getMappedObject(Mapped, relation)

	def has_many(self, Mapped, relation):
		"""Gets a Mapped object based on an existing has_many relation.

		Args:
			Mapped: Mapped object from where we get its relation.
			relation: String with the model to relate to.

		Returns:
			Mapped object if exists, None otherwise.
		"""
		return self.getMappedObject(Mapped, relation)

	def belongs_to_many(self, Mapped, relation):
		"""Gets a Mapped object based on an existing belongs_to_many relation.

		Args:
			Mapped: Mapped object from where we get its relation.
			relation: String with the model to relate to.

		Returns:
			Mapped object if exists, None otherwise.
		"""

		return self.getMappedObject(Mapped, relation)