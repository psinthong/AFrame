import aframe as af
import pandas as pd
import pandas.io.json as json
import numpy as np
from aframe.missing import notna
import unittest
from unittest.mock import MagicMock
import urllib.parse
import urllib.request
from aframe.aframe import AFrame, AFrameObj, OrderedAFrame
from aframe.aframe import NestedAFrame
from aframe.missing import notna
from aframe.window import Window
from aframe.groupby import AFrameGroupBy
from unittest.mock import patch
import pandas as pd
from pandas.io.json import json_normalize
import numpy as np

class TestBasicFunction(unittest.TestCase):

    def testInit(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', 'schema')
        self.assertEqual(af_obj._schema, 'schema')
        self.assertEqual(af_obj._query, None)
        self.assertEqual(af_obj._data, None)
        self.assertEqual(af_obj._dataverse, 'test_dataverse')
        self.assertEqual(af_obj._dataset, 'test_dataset')
        self.assertEqual(af_obj._predicate, None)       
        
    def testStr(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', 'schema')
        expected = 'Column: '+'schema'
        actual = str(af_obj)
        self.assertEqual(expected, actual)

    #key is AFrameObj
    def testGetItem_ErrorCase(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema')
        key = AFrameObj('key_dataverse', 'key_dataset', 'key_schema')
        with self.assertRaises(NotImplementedError): 
            af_obj[key] ###raise NotImplemented

    #key is str, af_obj._schema is not None
    def testGetItem_NormalCase1(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema')
        key = 'key'
        dataset = af_obj._dataverse + '.' + af_obj._dataset
        new_query = 'SELECT VALUE t.%s FROM %s t WHERE %s;' % (key, dataset, af_obj._schema)
        expected = AFrameObj(af_obj._dataverse, af_obj._dataset, key, new_query, af_obj._schema)
        actual = af_obj.__getitem__(key)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._predicate, actual._predicate)

    #key is str, af_obj._query is not None
    def testGetItem_NormalCase2(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', None, 'test_query;')
        key = 'key'
        dataset = af_obj._dataverse + '.' + af_obj._dataset
        predicate = None
        new_query = 'SELECT VALUE t.%s FROM (%s) t;' % (key, af_obj._query[:-1])
        expected = AFrameObj(af_obj._dataverse, af_obj._dataset, key, new_query, predicate)
        actual = af_obj.__getitem__(key)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._predicate, actual._predicate)

    #key is str, af_obj._schema is None and af_obj._query is None
    def testGetItem_NormalCase3(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', None, None)
        key = 'key'
        dataset = af_obj._dataverse + '.' + af_obj._dataset
        predicate = None
        new_query = 'SELECT VALUE t.%s FROM %s t;' % (key, dataset)
        expected = AFrameObj(af_obj._dataverse, af_obj._dataset, key, new_query, predicate)
        actual = af_obj.__getitem__(key)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._predicate, actual._predicate)

    #key is list, af_obj._schema is not None
    def testGetItem_NormalCase4(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema')
        key = ['attr1', 'attr2', 'attr3']
        dataset = af_obj._dataverse + '.' + af_obj._dataset
        fields = ''
        for i in range(len(key)):
            if i > 0:
                fields += ', '
            fields += 't.%s' % key[i]
        predicate = af_obj._schema
        new_query = 'SELECT %s FROM %s t WHERE %s;' % (fields, dataset, af_obj._schema)
        actual = af_obj.__getitem__(key)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._predicate, actual._predicate)

if __name__ == '__main__':
    unittest.main()


















