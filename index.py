#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""index.py: Index MySQL information inside Elasticsearch."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

# main
import time, sys
from Queue import Queue
from pyes import ES # Elasticsearch connector

# Utility functions
from utils.utils import vprint

# # Indexer
# from utils.ThreadWatcher import ThreadWatcher # Thread control
# from sqlsoup import TableClassType
# from threading import Lock
# import itertools # iterate faster
# import time

# Errors
from utils.errors import (
	BadConfigError,
	ConnectorError
)

# Config
from config.Config import Config

# Db connector
from connectors.SQLSoup import Connector as DbConnector

# Indexer
from indexer.Indexer import Indexer

# Thread
from utils.Thread import Thread


def main(script, *args, **kwargs):
	start_time = time.time()

	# Config options
	config = Config()

	# temporary MySQL settings
	source_table = 'categories'
	source_name = 'Categories'
	source_relationships = {
		'one_to_many': {
			'alternative_languages': {
				'name': 'AlternativeLanguages',
				'foreign_key': 'category_id'
			},
			# 'child_categories':{
			# 	'name': 'ChildCategories',
			# 	'foreign_key': 'parent_id'
			# 	# 'foreignKey' => 'parent_id',
			# },
			'external_pages': {
				'name': 'ExternalPages',
				'foreign_key': 'category_id'
			},
			'news_groups': {
				'name': 'NewsGroups',
				'foreign_key': 'category_id'
			}
		},
		'many_to_one': {
		# 	'ParentCategories'
		# 		# 'foreignKey' => 'parent_id',
		},
		'one_to_one': {
		},
		'many_to_many': {
		# 	'AliasedCategories'
		# 		# 'through' => 'Aliases'
		# 	'RelatedCategories'
		# 		# 'through' => 'Relateds'
		# 	'SymbolicCategories'
		# 		# 'through' => 'Symbolics'
		},
	}
	document_mapping = {
		'Category': 'categories',

		# many_to_one
		#'User': many_to_one(source_model, 'User'),
	}
	# source_relationships = False

	# Threads list
	threads = []

	# Db models queue
	read_queue = Queue()

	# Db connections list
	db_connections = config.db_connections

	# Populte db models queue (round robin)
	for _ in range(config.db_queue_size):
		db_connection = db_connections.pop(0)
		model = DbConnector(db_connection).db().build(source_table, source_name, source_relationships)
		read_queue.put(model)
		db_connections.append(db_connection)

	vprint('Index: {:s}'.format(config.es_index))
	vprint('Type: {:s}'.format(config.es_type))
	vprint('Threads: {:d}'.format(config.threads))
	vprint('DB Queue size: {:d}'.format(config.db_queue_size))
	vprint('Read chunk size: {:d}'.format(config.read_chunk_size))
	vprint('Write chunk size: {:d}'.format(config.write_chunk_size))

	# TODO
	db_connector = DbConnector(db_connections[0]).db()

	source_model =  db_connector.build(source_table, source_name, source_relationships)

	if not source_model:
		raise ConnectorError

	# import ipdb; ipdb.set_trace()
	indexer = Indexer(source_model, limit=config.limit)

	# Elasticsearch connector (write)
	# retry_time = 10
	# timeout = 10
	es_connector = ES(server=config.es_connections, bulk_size=config.write_chunk_size)

	# Create index if necessary
	# es_connector.indices.create_index_if_missing(config.es_index)

	indexer.index(
		start_time,
		str(1),
		read_queue,
		es_connector,
		config.es_index,
		config.es_type,
		config.read_chunk_size)

# 	# Create new threads
	# for i in range(config.threads):
		# thread = Thread(indexer.index, (
		# 	start_time,
		# 	str(i+1),
		# 	read_queue,
		# 	es_connector,
		# 	config.es_index,
		# 	config.es_type,
		# 	config.read_chunk_size)
		# threads.append(thread)

	# for t in threads:
	# 	t.start()

	# for t in threads:
	# 	t.join()

	vprint('Refreshing index...')
	# es_connector.refresh()

	vprint('Elapsed: {:f}'.format(time.time() - start_time))

if __name__ == '__main__':
	main(*sys.argv)
