#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Thread.py: Custom Thread implementation.

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
	def __init__(self, target, *args, **kwargs):
		if 'autostart' in kwargs:
			autostart = kwargs['autostart']
			del kwargs['autostart']

		super(Thread, self).__init__(target=target, args=args, kwargs=kwargs)
		if autostart:
			self.start()
