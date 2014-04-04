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

# Soup
from model.Soup import Soup

# Indexer
from indexer.Indexer import Indexer

# Thread
# from utils.Thread import Thread


def main(script, *args, **kwargs):
	startTime = time.time()

	IndexerConfig = Config()

	threadList = range(IndexerConfig.threads)
	threads = []
	DbQueue = Queue()

	db_connections = IndexerConfig.db_connections

	for _ in range(IndexerConfig.db_queue_size):
		db_connection = db_connections.pop(0)
		IndexerSoup = Soup(db_connection)
		DbQueue.put(IndexerSoup.build())
		db_connections.append(db_connection)

	vprint('Index: {:s}'.format(IndexerConfig.es_index))
	vprint('Type: {:s}'.format(IndexerConfig.es_type))
	vprint('Threads: {:d}'.format(IndexerConfig.threads))
	vprint('DB Queue size: {:d}'.format(IndexerConfig.db_queue_size))
	vprint('Read buffer: {:d}'.format(IndexerConfig.read_buffer))
	vprint('Write buffer: {:d}'.format(IndexerConfig.write_buffer))

	# import ipdb; ipdb.set_trace()
	EsIndexer = Indexer(Soup(db_connections.pop(0)).build(), IndexerConfig.limit)

# 	# Connection to ElasticSearch (write)
# 	# retry_time = 10
# 	# timeout = 10
# 	EsServer = ES(server = IndexerConfig.elasticConnections, bulk_size = IndexerConfig.write_buffer)
# 	EsServer.indices.create_index_if_missing(IndexerConfig.es_index)

# 	# Create new threads
# 	for i in threadList:
		# thread = Thread.Thread(EsIndexer.consume, (
		# 	startTime,
		# 	str(i+1),
		# 	DbQueue,
		# 	EsServer,
		# 	IndexerConfig.es_index,
		# 	IndexerConfig.es_type,
		# 	IndexerConfig.read_buffer,
		# 	IndexerConfig.write_buffer))
# 		threads.append(thread)

# 	for t in threads:
# 		t.start()

# 	for t in threads:
# 		t.join()

# 	vprint('Refreshing index...')
# 	EsServer.refresh()

# 	vprint('Elapsed: {:f}'.format(time.time() - startTime))

if __name__ == '__main__':
	main(*sys.argv)


# def main(script, flag='with'):
#     """This example runs two threads that print a sequence, sleeping
#     one second between each.  If you run it with no command-line args,
#     or with the argument 'with', you should be able it interrupt it
#     with Control-C.

#     If you run it with the command-line argument 'without', and press
#     Control-C, you will probably get a traceback from the main thread,
#     but the child thread will run to completion, and then print a
#     traceback, no matter how many times you try to interrupt.
#     """

#     if flag == 'with':
#         Watcher()
#     elif flag != 'without':
#         print 'unrecognized flag: ' + flag
#         sys.exit()

#     t = range(1, 10)

#     # create a child thread that runs counter
#     ThreadWatcher.MyThread(counter, t)

#     # run counter in the parent thread
#     ThreadWatcher.counter(t)

# if __name__ == '__main__':
#     main(*sys.argv)