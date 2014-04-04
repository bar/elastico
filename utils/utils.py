#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function # http://stackoverflow.com/questions/5980042/how-to-implement-the-verbose-or-v-option-into-a-python-script

"""utils.py: Just a bunch of utility functions."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"


from config import Config


def vprint(*a, **k):
	return print(*a) if Config.VERBOSE >= 1 else lambda *a, **k: None

def vvprint(*a, **k):
	return print(*a) if Config.VERBOSE >= 2 else lambda *a, **k: None

def vvvprint(*a, **k):
	return print(*a) if Config.VERBOSE >= 3 else lambda *a, **k: None

def tprint(name, text, tab=0):
	vvprint('\t' * tab + '[{:s}] {:s}'.format(name, text))

def uniqify(collection):
	"""Utility for 'uniqify' items in collections.

	Args:
		collection: The collection to analize.

	Returns:
		A collection with unique items.
	"""

	seen = set()
	seen_add = seen.add
	return [ x for x in collection if x not in seen and not seen_add(x)]