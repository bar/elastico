#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""index.py: Index MySQL data as Elasticsearch documents."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

import time, sys
from Queue import Queue

# Elasticsearch connector
import pyes

# Utility functions
from utils.utils import vprint

# # Indexer
# from utils.ThreadWatcher import ThreadWatcher # Thread control
# from sqlsoup import TableClassType
# from threading import Lock
# import itertools # iterate faster
# import time

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
	source_relationships = {
		'one_to_many': {
			'alternative_languages': {
				'foreign_key': 'category_id'
			},
			'external_pages': {
				'foreign_key': 'category_id'
			},
			'news_groups': {
				'foreign_key': 'category_id'
			}
		},
		'many_to_one': {
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
		'adjacency_list': {
			'parent_category': {
				'foreign_key': 'parent_id',
				'backref': 'child_categories'
			},
		}
	}
	source_table = 'external_pages'
	source_relationships = {
		'one_to_many': {
		},
		'many_to_one': {
			'categories': {
				'foreign_key': 'category_id'
			}
		},
		'one_to_one': {
		},
		'many_to_many': {
		},
		'adjacency_list': {
		}
	}
	document_map = {
		'Category': 'categories',
		'ExternalPages': 'external_pages',
		'AlternativeLanguages': 'alternative_languages',
	}

	# Threads list
	threads = []

	# Db connector queue
	read_queue = Queue()

	# Db connections list
	db_connections = config.db_connections

	# Populte db connector queue (round robin)
	for _ in range(config.db_queue_size):
		db_connection = db_connections.pop(0)
		db_connector = DbConnector(db_connection).build(source_table, source_relationships)
		read_queue.put(db_connector)
		db_connections.append(db_connection)

	# Elasticsearch connector
	es_connector = pyes.ES(server=config.es_connections, bulk_size=config.write_chunk_size)

	# Create index if necessary
	# es_connector.indices.create_index_if_missing(config.es_index)

	indexer = Indexer(
		db_connector=db_connector,
		read_queue=read_queue,
		es_connector=es_connector,
		es_index=config.es_index,
		es_type=config.es_type,
		document_map=document_map,
		limit=config.limit)

	# Start indexing
	if config.single_process_mode:
		indexer.index(start_time, read_chunk_size=config.read_chunk_size)
	else:
		# Create new threads
		for i in range(config.threads):
				thread = Thread(
					indexer.index,
					start_time,
					read_chunk_size=config.read_chunk_size,
					autostart=config.autostart_threads)
				threads.append(thread)

		# Starts threads, by calling run()
		if not config.autostart_threads:
			for thread in threads:
				thread.start()

		# Wait for threads to terminate
		for thread in threads:
			thread.join()

	vprint('Refreshing index...')
	# es_connector.refresh()

	vprint('Elapsed: {:f}'.format(time.time() - start_time))

if __name__ == '__main__':
	main(*sys.argv)
