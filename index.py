#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""index.py: Index MySQL data as Elasticsearch documents."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

import time, sys
from Queue import Queue

# Elasticsearch connector
from pyes import ES

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
		'one_to_one': {
		},
		'many_to_one': {
		},
		'many_to_many': {
		},
		'self_referential': {
			'parent_category': {
				'foreign_key': 'parent_id',
				'backref': 'child_categories'
			},
		}
	}
	# source_table = 'external_pages'
	# source_relationships = {
	# 	'one_to_many': {
	# 	},
	# 	'many_to_one': {
	# 		'categories': {
	# 			'foreign_key': 'category_id'
	# 		}
	# 	},
	# 	'one_to_one': {
	# 	},
	# 	'many_to_many': {
	# 	},
	# 	'self_referential': {
	# 	}
	# }
	document_map = {
		'alternative_languages': 'alternative_languages',
		'categories': 'categories',
		'external_pages': 'external_pages',
		'news_groups': 'news_groups',
		'parent_category': 'parent_category',
		'related_categories': 'related_categories',
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
	es_connector = ES(server=config.es_connections, bulk_size=config.write_chunk_size)

	# Create index if necessary
	es_connector.indices.create_index_if_missing(config.es_index)

	# Define mapping
	# es_connector.cluster.put_mapping(config.es_type, {'properties':gralSettings['mapping']}, config.indexName)

	# Update index settings to improve indexing speed.
	#
	# Disable refresh interval
	# Improve indexing speed by augmenting the merge factor (uses more RAM).
	# http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-update-settings.html#bulk
	# http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/index-modules.html#index-modules-settings
	#
	# http://blog.sematext.com/2013/07/08/elasticsearch-refresh-interval-vs-indexing-performance/
	# http://www.elasticsearch.org/blog/update-settings/
	# https://github.com/aparo/pyes/blob/master/docs/guide/reference/api/admin-indices-update-settings.rst
	# http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/index-modules-merge.html#log-byte-size
	vprint('Optimizing for bulk indexing...')
	es_connector.indices.update_settings(config.es_index, {
		'index.refresh_interval': '-1',
		'index.merge.policy.merge_factor': '30'
	})

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

	vprint('Optimizing for interactive indexing...')
	es_connector.indices.update_settings(config.es_index, {
		'index.refresh_interval': '1s',
		'index.merge.policy.merge_factor': '10'
	})

	vprint('Refreshing index...')
	es_connector.indices.refresh()

	vprint('Elapsed: {:f}'.format(time.time() - start_time))

if __name__ == '__main__':
	main(*sys.argv)
