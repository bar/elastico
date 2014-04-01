#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""utils.py: Just a bunch of utility functions."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"


def tprint(name, text, tab = 0):
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