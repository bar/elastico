#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Soup.py: Abstract class that serves as a base for MySQL models."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

from abc import ABCMeta, abstractmethod

from sqlalchemy import *
#from sqlalchemy.exc import *
from sqlalchemy.ext.declarative import declarative_base
from sqlsoup import SQLSoup, scoped_session, sessionmaker


class Soup(object):
	"""SQLSoup base class.

	Used as the MySQL repository connector.
	"""

	__metaclass__ = ABCMeta

	_model = None

	def __init__(self, connection, engine = 'mysql'):
		name, hostname, username, password = connection
		self.url = '{:s}://{:s}:{:s}@{:s}/{:s}'.format(engine, username, password, hostname, name)

	def build(self, db_charset = 'utf8'):
		"""Construct the db object used to access the database.

		Note: Using session with autocommit=True is better when working with threads, and avoids calling Soup.session.commit()
		http://forum.griffith.cc/index.php?topic=1082.0

		Args:
			db_charset (string): DB connection character set.

		Returns:
			Object: SQLSoup object.
		"""
		# Connection to MySQL (read)
		#Soup = SQLSoup(self.url + '?charset=' + db_charset)

		#Session = scoped_session(sessionmaker(autoflush = True, expire_on_commit = True, autocommit = False))
		Session = scoped_session(sessionmaker(autoflush = False, expire_on_commit = False, autocommit = True))
		self._model = SQLSoup(self.url + '?charset=' + db_charset, session = Session)

		return self.bind()

	# @abstractmethod
	# def bind(self, Soup):
	# 	raise NotImplementedError()

	def bind(self):
		"""Bind relationships.

		http://docs.sqlalchemy.org/en/latest/orm/relationships.html#adjacency-list-relationships

		Args:
			Model (object): SQLSoup object.

		Returns:
			Object: SQLSoup object.

		TODO:
			Maybe later read from config models and its relations.
		"""
		if self._model is None:
			return

		Model = self._model
		return Model