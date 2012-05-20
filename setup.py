# -*- coding: utf-8 -*-
"""
A ThriftDB (http://www.thriftdb.com/) client for Python.

Usage:
    >>> from thrifdb import ThriftDB
    >>> db = ThriftDB('yourusername', 'yourpassword')
    >>> db.make_bucket('a-new-bucket')
    >>> db.make_collection('a-new-bucket', 'some-collection', schema)
    >>> db.put_item('a-new-bucket', 'some-collection', 'item_id', item_data)

Schemas and item data should be JSON strings. Essentially this is a very thin
wrapper around Requests to make working with ThriftDB a bit easier.
"""

from setuptools import setup

setup(
    name='ThriftDB',
    version='0.1',
    url='https://github.com/chrisguitarguy/Python-ThriftDB',
    license='BSD',
    author='Christopher Davis',
    author_email='chris@classicalguitar.org',
    description='A Python client for ThriftDB',
    long_description=__doc__,
    py_modules=['thriftdb'],
    zip_safe=False,
    include_package_data=True,
    platforms='any'
)
