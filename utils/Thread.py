#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Thread.py: Thread wrapper.

http://code.activestate.com/recipes/496735/
https://tinyos-main.googlecode.com/svn/trunk/support/sdk/python/tinyos/utils/Watcher.py
"""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"

import threading


class Thread(threading.Thread):
	"""This is a wrapper for threading.Thread that improves
	the syntax for creating and starting threads.
	"""
	def __init__(self, target, *args):
		if not isinstance(args, tuple):
			args = (args,)
		threading.Thread.__init__(self, target=target, args=args)
		# self.start()
