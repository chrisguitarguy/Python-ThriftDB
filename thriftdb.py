# -*- coding: utf-8 -*- 

import requests


class ThriftDB(object):
    """
    A thing wrapper around requests for the thrift db API (or what we need to
    use of it).
    """

    url = 'http://api.thriftdb.com/'

    def __init__(self, user=None, password=None):
        self.user = user
        self.password = password

    def _request(self, url, method='get', **kwargs):
        """
        Hit the ThriftDB api with an http request
        """
        return requests.request(method, url,
                auth=(self.user, self.password), **kwargs)

    def _url(self, bucket, coll=None):
        """
        Make a url from a `bucket` and collection (`coll).
        """
        u = self.url + bucket
        if coll is not None:
            u += '/' + coll
        return u

    def _item_url(self, bucket, coll, item_id):
        """
        Make a url for an item
        """
        u = self._url(bucket, coll)
        u += '/' + str(item_id) # should be in try block?
        return u

    def _bulk_url(self, bucket, coll, action):
        """
        Create a url for working with bulk items
        """
        u = self._url(bucket, coll)
        u += '/_bulk/' + action
        return u

    def _join_ids(self, ids):
        """
        Join a list of ids with a comma
        """
        return ','.join([str(i) for i in ids])

    def make_bucket(self, bucket):
        """
        Create a bucket.

        :param bucket: The ThriftDB bucket to create
        """
        return self._request(self._url(bucket), 'put')
    
    def delete_bucket(self, bucket):
        """
        Delete a bucket.

        :param bucket: The ThriftDB bucket to delete
        """
        return self._request(self._url(bucket), 'delete')

    def get_bucket(self, bucket):
        """
        Get a bucket.

        :param bucket: The ThriftDB bucket to fetch
        """
        return self._request(self._url(bucket))

    def make_collection(self, bucket, coll, schema):
        """
        Make or update a collection (`coll`) in the bucket with a `schema`.

        :param bucket: The ThriftDB bucket to import osin which the collection resides
        :param coll: The collection to create or update
        :param schema: The collection's schema (as a string)
        """
        return self._request(self._url(bucket, coll), 'put', data=schema)

    def delete_collection(self, bucket, coll):
        """
        Delete a collection.

        :param bucket: The ThriftDB bucket to in which the collection resides
        :param coll: the collection to delete
        """
        return self._request(self._url(bucket, coll), 'delete')

    def get_collection(self, bucket, coll):
        """
        Get a collection

        :param bucket: The ThriftDB bucket to in which the collection resides
        :param coll: the collection to delete
        """
        return self._request(self._url(bucket, coll))
    
    def put_item(self, bucket, coll, item_id, data):
        """
        Create or update an item.

        :param bucket: The ThriftDB bucket in which this collection and item resides
        :param coll: The collection in whcih this item resides
        :param item_id: The id to use for this item
        :param data: The items data to update. Should make up the collection's scheam
        """
        return self._request(
                self._item_url(bucket, coll, item_id), 'put', data=data)

    def delete_item(self, bucket, coll, item_id):
        """
        Delete an item

        :param bucket: The ThriftDB bucket in which this collection and item resides
        :param coll: The collection in which this item resides
        :param item_id: The id of the item to delete
        """
        return self._request(self._item_url(bucket, coll, item_id), 'delete')

    def get_item(self, bucket, coll, item_id, **kwargs):
        """
        Get an existing item.

        :param bucket: The ThriftDB bucket in which this collection and item resides
        :param coll: The collection in which this item resides
        :param item_id: The id of the item to fetch
        :param **kwargs: Any additional items to send as a query string
        """
        return self._request(self._item_url(bucket, coll, item_id), params=kwargs)

    def put_item_multi(self, bucket, coll, data):
        """
        Add multiple items to a collection.

        :param bucket: The ThriftDB bucket in which the collection and items reside
        :param coll: The collection in which these items will reside
        :param data: the data (in JSON format) to add
        """
        return self._request(
                self._bulk_url(bucket, coll, 'put_multi'), 'post', data=data)

    def get_item_multi(self, bucket, coll, ids=[], **kwargs):
        """
        Get multiple items.

        :param bucket: The ThriftDB bucket in which the collection and items reside
        :param coll: The collection in which these items will reside
        :param ids: A list of item IDs to fetch
        :param **kwargs: Any additional items to send as a query string
        """
        u = self._bulk_url(bucket, coll, 'get_multi')
        u += '?ids=' + ','.join(str(i) for i in ids) # hack to avoid urlencoding
        return self._request(u, params=kwargs)

    def delete_item_multi(self, bucket, coll, ids=[]):
        """
        Delete multiple items.

        :param bucket: The ThriftDB bucket in which the collection and items reside
        :param coll: The collection in which these items reside
        :param ids: A list of item IDs to delete
        """
        u = self._bulk_url(bucket, coll, 'delete_multi')
        u += '?ids=' + ','.join(str(i) for i in ids) # hack to avoid urlencoding
        return self._request(u, 'post')

    def reindex_collection(self, bucket, coll):
        """
        Reindex a collection.

        :param bucket: The bucket in which the collection resides.
        :param coll: The collection to reindex
        """
        return self._request(self._bulk_url(bucket, coll, 'reindex'), 'post')
