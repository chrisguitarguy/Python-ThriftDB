# -*- coding: utf-8 -*-

from copy import deepcopy
import sys
import unittest
import json

from thriftdb import ThriftDB

SCHEMA = {
    '__class__': 'StructSchema',
    'make': {
        '__class__': 'AttributeDescriptor',
        'thrift_index': 1,
        'datatype': {'__class__': 'StringType'}
    },
    'model': {
        '__class__': 'AttributeDescriptor',
        'thrift_index': 2,
        'datatype': {'__class__': 'StringType'}
    },
    'year': {
        '__class__': 'AttributeDescriptor',
        'thrift_index': 3,
        'datatype': {'__class__': 'IntegerType'}
    }
}

DATA = [
    {
        '_id': '1',
        'make': 'Chevy',
        'model': 'Aveo',
        'year': 1900
    },
    {
        '_id': '2',
        'make': 'Ford',
        'model': 'Taurus',
        'year': 1999
    },
    {
        '_id': '3',
        'make': 'Toyota',
        'model': 'Camery',
        'year': 2010
    }
]


class ThriftTestBuckets(unittest.TestCase):
    def setUp(self):
        self.db = ThriftDB()
        # delete buckets, lame
        self.db.delete_bucket('test_bucket')

    def tearDown(self):
        self.db.delete_bucket('test_bucket')

    def test_create_bucket(self):
        r = self.db.make_bucket('test_bucket')
        self.assertEqual(r.status_code, 201)

    def test_already_created_bucket(self):
        self.db.make_bucket('test_bucket')
        r = self.db.make_bucket('test_bucket')
        self.assertEqual(r.status_code, 409)

    def test_delete_bucket(self):
        self.db.make_bucket('test_bucket')
        r = self.db.delete_bucket('test_bucket')
        self.assertEqual(r.status_code, 200)

    def test_delete_nonexistent_bucket(self):
        r = self.db.delete_bucket('test_bucket')
        self.assertEqual(r.status_code, 404)
    
    def test_get_bucket(self):
        self.db.make_bucket('test_bucket')
        r = self.db.get_bucket('test_bucket')
        self.assertEqual(r.status_code, 200)

    def test_get_nonexistent_bucket(self):
        r = self.db.get_bucket('test_bucket')
        self.assertEqual(r.status_code, 404)


class ThriftTestCollections(unittest.TestCase):
    def setUp(self):
        self.db = ThriftDB()
        # create a test bucket
        self.db.make_bucket('test_bucket')
        self.schema = deepcopy(SCHEMA)

    def tearDown(self):
        self.db.delete_bucket('test_bucket')

    def test_create_collection(self):
        self.db.delete_collection('test_bucket', 'cars')
        r = self.db.make_collection(
                'test_bucket', 'cars', json.dumps(self.schema))
        self.assertEqual(r.status_code, 201)

    def test_update_collection(self):
        # make the collection
        self.db.make_collection('test_bucket', 'cars', json.dumps(self.schema))
        self.schema['a_field'] = {
            '__class__': 'AttributeDescriptor',
            'thrift_index': 4,
            'datatype': {'__class__': 'StringType'}
        }
        r = self.db.make_collection(
                'test_bucket', 'cars', json.dumps(self.schema))
        self.assertEqual(r.status_code, 200)

    def test_get_collection(self):
        self.db.make_collection('test_bucket', 'cars', json.dumps(self.schema))
        r = self.db.get_collection('test_bucket', 'cars')
        self.assertEqual(r.status_code, 200)

    def test_get_nonexistent_collection(self):
        r = self.db.get_collection('test_bucket', 'cars')
        self.assertEqual(r.status_code, 404)

    def test_delete_collection(self):
        self.db.make_collection('test_bucket', 'cars', json.dumps(self.schema))
        r = self.db.delete_collection('test_bucket', 'cars')
        self.assertEqual(r.status_code, 200)

    def test_delete_nonexistent_collection(self):
        r = self.db.delete_collection('test_bucket', 'cars')
        self.assertEqual(r.status_code, 404)


class ThriftTestItems(unittest.TestCase):
    def setUp(self):
        self.db = ThriftDB()
        # remove the collection
        self.db.delete_collection('test_bucket', 'cars')
        # remove the bucket, just in case
        self.db.delete_bucket('test_bucket')
        # create a test bucket
        self.db.make_bucket('test_bucket')
        self.schema = deepcopy(SCHEMA)
        self.data = deepcopy(DATA)
        self.db.make_collection('test_bucket', 'cars', json.dumps(self.schema))

    def tearDown(self):
        self.db.delete_collection('test_bucket', 'cars')
        self.db.delete_bucket('test_bucket')

    def test_create_item(self):
        r = self.db.put_item('test_bucket', 'cars', 1, json.dumps(self.data[1]))
        self.assertEqual(r.status_code, 201)

    def test_update_item(self):
        # put the item in
        self.db.put_item('test_bucket', 'cars', 1, json.dumps(self.data[1]))
        data = deepcopy(self.data[1])
        data['model'] = 'updated model'
        r = self.db.put_item('test_bucket', 'cars', 1, json.dumps(data))
        self.assertEqual(r.status_code, 200)

    def test_delete_item(self):
        self.db.put_item('test_bucket', 'cars', 1, json.dumps(self.data[1]))
        r = self.db.delete_item('test_bucket', 'cars', 1)
        self.assertEqual(r.status_code, 200)

    def test_delete_nonexistent_item(self):
        r = self.db.delete_item('test_bucket', 'cars', 1)
        # this fails, thriftdb issue?
        self.assertEqual(r.status_code, 404)

    def test_get_item(self):
        self.db.put_item('test_bucket', 'cars', 1, json.dumps(self.data[0]))
        r = self.db.get_item('test_bucket', 'cars', 1)
        self.assertEqual(r.status_code, 200)

    def test_get_nonexistent_item(self):
        r = self.db.get_item('test_bucket', 'cars', 1)
        self.assertEqual(r.status_code, 404)

    def test_put_item_multi(self):
        r = self.db.put_item_multi('test_bucket', 'cars', json.dumps(self.data))
        self.assertEqual(r.status_code, 200)

    def test_put_item_multi_bad_bucket(self):
        r = self.db.put_item_multi('test_bucket1', 'cars', json.dumps(self.data))
        self.assertEqual(r.status_code, 404)

    def test_put_item_multi_bad_coll(self):
        r = self.db.put_item_multi('test_bucket', 'nope', json.dumps(self.data))
        self.assertEqual(r.status_code, 404)

    def test_get_item_multi(self):
        self.db.put_item_multi('test_bucket', 'cars', json.dumps(self.data))
        r = self.db.get_item_multi('test_bucket', 'cars', ids=[1, 2, 3])
        self.assertEqual(r.status_code, 200)

    def test_get_item_multi_no_ids(self):
        self.db.put_item_multi('test_bucket', 'cars', json.dumps(self.data))
        r = self.db.get_item_multi('test_bucket', 'cars')
        self.assertEqual(r.status_code, 200)

    def test_get_item_multi_nonexistent(self):
        self.db.put_item_multi('test_bucket', 'cars', json.dumps(self.data))
        r = self.db.get_item_multi('test_bucket', 'cars', ids=[4, 5, 6])
        self.assertEqual(r.status_code, 200)

    def test_get_item_multi_bad_bucket(self):
        r = self.db.get_item_multi('test_bucket1', 'cars', ids=[4, 5, 6])
        self.assertEqual(r.status_code, 404)

    def test_get_item_multi_bad_coll(self):
        r = self.db.get_item_multi('test_bucket', 'nope', ids=[4, 5, 6])
        self.assertEqual(r.status_code, 404)

    def test_delete_item_multi(self):
        self.db.put_item_multi('test_bucket', 'cars', json.dumps(self.data))
        r = self.db.delete_item_multi('test_bucket', 'cars', ids=[1, 2, 3])
        self.assertEqual(r.status_code, 200)

    def test_delete_item_multi_bad_bucket(self):
        self.db.put_item_multi('test_bucket', 'cars', json.dumps(self.data))
        r = self.db.delete_item_multi('test_bucket1', 'cars', ids=[1, 2, 3])
        self.assertEqual(r.status_code, 404)

    def test_delete_item_multi_bad_coll(self):
        self.db.put_item_multi('test_bucket', 'cars', json.dumps(self.data))
        r = self.db.delete_item_multi('test_bucket', 'nope', ids=[1, 2, 3])
        self.assertEqual(r.status_code, 404)

        
