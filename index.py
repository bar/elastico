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
from utils.errors import BadConfigError

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

	# Threads list
	threads = []

	# Db connections queue
	db_connections_queue = Queue()

	# Db connections list
	db_connections = config.db_connections

	# temporary MySQL settings
	source_table = 'categories'
	source_relationships = {
		'belongs_to_many': {
		# 	'AliasedCategories'
		# 		# 'through' => 'Aliases'
		# 	'RelatedCategories'
		# 		# 'through' => 'Relateds'
		# 	'SymbolicCategories'
		# 		# 'through' => 'Symbolics'
		},
		'has_many': {
			'alternative_languages'
			# 'ChildCategories'
				# 'foreignKey' => 'parent_id',
			'external_pages'
			'news_groups'
		},
		'belongs_to': {
		# 	'ParentCategories'
		# 		# 'foreignKey' => 'parent_id',
		},
		'has_one': {
		}
	}


	# Populte db connections queue (round robin)
	for _ in range(config.db_queue_size):
		db_connection = db_connections.pop(0)
		db_connections_queue.put(DbConnector(db_connection).db())
		db_connections.append(db_connection)

	vprint('Index: {:s}'.format(config.es_index))
	vprint('Type: {:s}'.format(config.es_type))
	vprint('Threads: {:d}'.format(config.threads))
	vprint('DB Queue size: {:d}'.format(config.db_queue_size))
	vprint('Read chunk size: {:d}'.format(config.read_chunk_size))
	vprint('Write chunk size: {:d}'.format(config.write_chunk_size))

	# TODO
	IndexerConnector = DbConnector(db_connections[0]).db()
	source_relationships = False

	PrimaryModel =  IndexerConnector.build(source_table, source_relationships)

	if not PrimaryModel:
		raise ConnectorError

	# Connection to ElasticSearch (write)
	# retry_time = 10
	# timeout = 10
	es_server = ES(server=config.es_connections, bulk_size=config.write_chunk_size)

	indexer = Indexer(PrimaryModel, limit=config.limit)

	indexer.index(
		start_time,
		str(1),
		db_connections_queue,
		es_server,
		config.es_index,
		config.es_type,
		config.read_chunk_size)

	vprint('Refreshing index...')
	# es_server.refresh()

	vprint('Elapsed: {:f}'.format(time.time() - start_time))

if __name__ == '__main__':
	main(*sys.argv)
