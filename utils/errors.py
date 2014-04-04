#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Exceptions.py: Custom exceptions."""

__author__      = "Ber Clausen"
__copyright__   = "Copyright 2014, Planet Earth"


class Error(Exception):
    """Base class for exceptions in this module.

    Attributes:
        msg  -- explanation of the error
    """
    msg = 'Something went wrong :('

    def __init__(self, msg=None):
        if msg is not None:
            self.msg = msg

    def __str__(self):
        if self.msg:
            return repr(self.msg)


class BadConfigError(Error):
    """Exception raised for configuration errors."""
    msg = 'Configuration errors were found.'


class ConnectorError(Error):
    """Exception raised for configuration errors."""
    msg = 'Connector errors were found.'
