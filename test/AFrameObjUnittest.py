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
import io

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
    '''
    def testGetItem_ErrorCase(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema')
        key = AFrameObj('key_dataverse', 'key_dataset', 'key_schema')
        with self.assertRaises(NotImplementedError): 
            af_obj[key] ###raise NotImplemented'''

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
        af_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', None, 'test_predicate')
        key = ['attr1', 'attr2', 'attr3']
        dataset = af_obj._dataverse + '.' + af_obj._dataset
        fields = ''
        for i in range(len(key)):
            if i > 0:
                fields += ', '
            fields += 't.%s' % key[i]
        predicate = af_obj._schema
        new_query = 'SELECT %s FROM %s t WHERE %s;' % (fields, dataset, af_obj._schema)
        expected = AFrameObj(af_obj._dataverse, af_obj._dataset, key, new_query, af_obj._schema)
        actual = af_obj.__getitem__(key)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._predicate, actual._predicate)

    
    def testSchema(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', None, None)
        expected = 'test_schema'
        actual = aframe_obj.schema
        self.assertEqual(expected, actual)

    def testQuery(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query', None)
        expected = 'test_query'
        actual = aframe_obj.query
        self.assertEqual(expected, actual)

    #'_uuid' not in result.columns
    def testCollect_Case1(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query', 'test_predicate')
        json_response = [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        af.AFrame.send_request = MagicMock(return_value = json_response)
        json_result = af.AFrame.send_request(aframe_obj._query)
        json_str = json.dumps(json_result)
        expected = pd.DataFrame(data = json.read_json(json_str))

        actual = aframe_obj.collect()
        self.assertEqual(actual.equals(expected), True)


    #'_uuid' in result.colums
    '''
    def testCollect_Case2(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query', 'test_predicate')
        json_response = [{"_uuid": 1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        af.AFrame.send_request = MagicMock(return_value = json_response)
        json_result = af.AFrame.send_request(aframe_obj._query)
        json_str = json.dumps(json_result)
        original = pd.DataFrame(data = json.read_json(json_str))
        print()
        print('_uuid' in original.columns)
        expected = original.drop('_uuid', axis=1, inplace=True)

        actual = aframe_obj.collect()
        self.assertEqual(actual.equals(expected), True)'''

    def testToPandas(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query', 'test_predicate')
        expected = aframe_obj.collect()
        actual = aframe_obj.toPandas()

        self.assertEqual(actual.equals(expected), True)

    def testSimpleHead(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query', 'test_predicate')
        new_query = aframe_obj.query[:-1]+' LIMIT %d;' % 5
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        af.AFrame.send_request = MagicMock(return_value = json_response)

        actual = aframe_obj.head()
        af.AFrame.send_request.assert_called_once_with(new_query)
        self.assertEqual(len(actual), 5)
        row0 = pd.Series([1, 'str1'], index=['attr1', 'attr2'])
        row1 = pd.Series([2, 'str2'], index=['attr1', 'attr2'])
        row2 = pd.Series([3, 'str3'], index=['attr1', 'attr2'])
        row3 = pd.Series([4, 'str4'], index=['attr1', 'attr2'])
        row4 = pd.Series([5, 'str5'], index=['attr1', 'attr2'])
        self.assertTrue(actual.iloc[0].equals(row0))
        self.assertTrue(actual.iloc[1].equals(row1))
        self.assertTrue(actual.iloc[2].equals(row2))
        self.assertTrue(actual.iloc[3].equals(row3))
        self.assertTrue(actual.iloc[4].equals(row4))

    def testAdd_ErrorCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.add(None)

    def testAdd_NormalCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id + 3', 'SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;')
        
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.add(3)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testSub_ErrorCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.sub(None)

    def testSub_NormalCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id - 3', 'SELECT VALUE t.id - 3 FROM test_dataverse.test_dataset t;')
        
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.sub(3)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testDiv_ErrorCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.div(None)

    def testDiv_NormalCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id / 3', 'SELECT VALUE t.id / 3 FROM test_dataverse.test_dataset t;')
        
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.div(3)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testMul_ErrorCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.mul(None)

    def testMul_NormalCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id * 3', 'SELECT VALUE t.id * 3 FROM test_dataverse.test_dataset t;')
        
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.mul(3)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testMod_ErrorCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.mod(None)

    def testMod_NormalCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id % 3', 'SELECT VALUE t.id % 3 FROM test_dataverse.test_dataset t;')
        
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.mod(3)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testPow_ErrorCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.pow(None)

    def testPow_NormalCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id ^ 3', 'SELECT VALUE t.id ^ 3 FROM test_dataverse.test_dataset t;')
        
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.pow(3)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testAddOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id + 3', 'SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.add = MagicMock(return_value=expected)
        actual = aframe_obj+3
        
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testSubOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id - 3', 'SELECT VALUE t.id - 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.sub = MagicMock(return_value=expected)
        actual = aframe_obj-3
        
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testMulOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id * 3', 'SELECT VALUE t.id * 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.mul = MagicMock(return_value=expected)
        actual = aframe_obj*3
        
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testModOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id % 3', 'SELECT VALUE t.id % 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.mod = MagicMock(return_value=expected)
        actual = aframe_obj%3
        
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testPowOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id ** 3', 'SELECT VALUE t.id ** 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.pow = MagicMock(return_value=expected)
        actual = aframe_obj**3
        
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query) ###^ or ** ###

    def testTrueDivOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id / 3', 'SELECT VALUE t.id / 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.div = MagicMock(return_value=expected)
        actual = aframe_obj/3
        
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testLen(self):
        expected = 7
        af.AFrame.send_request = MagicMock(return_value = [expected])
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')

        query = 'SELECT VALUE count(*) FROM (%s) t;' % aframe_obj.query[:-1]
        actual = aframe_obj.__len__()
        af.AFrame.send_request.assert_called_once_with(query)

        self.assertEqual(expected, actual)

    def testArithmeticOp(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id + 3', 'SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;')
        actual = aframe_obj.arithmetic_op(3, '+')

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testMax(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        new_query = 'SELECT max(t.%s) FROM %s t;' % (aframe_obj.schema, aframe_obj._dataverse+'.'+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataverse+'.'+aframe_obj._dataset, 'max(%s)'%aframe_obj.schema, new_query)

        actual = aframe_obj.max()
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testMin(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        new_query = 'SELECT min(t.%s) FROM %s t;' % (aframe_obj.schema, aframe_obj._dataverse+'.'+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataverse+'.'+aframe_obj._dataset, 'min(%s)'%aframe_obj.schema, new_query)

        actual = aframe_obj.min()
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testAvg(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        new_query = 'SELECT avg(t.%s) FROM %s t;' % (aframe_obj.schema, aframe_obj._dataverse+'.'+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataverse+'.'+aframe_obj._dataset, 'avg(%s)'%aframe_obj.schema, new_query)

        actual = aframe_obj.avg()
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #self is AFrameObj, other is str
    def testBinaryOpt_Case2(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '='
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        actual = aframe_obj.binary_opt(other, opt)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #self is AFrameObj, other is not str
    def testBinaryOpt_Case3(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = None
        opt = '='
        schema = 't.%s %s %s' % (aframe_obj.schema, opt, other) #???
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        actual = aframe_obj.binary_opt(other, opt)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)
        
    def testEq(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '='
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj.__eq__(other)
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testNe(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '!='
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj.__ne__(other)
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testGt(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '>'
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj.__gt__(other)
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testLt(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '<'
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj.__lt__(other)
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testGe(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '>='
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj.__ge__(other)
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testLe(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '<='
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj.__le__(other)
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testBooleanOp(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = AFrameObj('other_datatverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate')
        op = 'test_op'
        schema = '%s %s %s' % (aframe_obj._schema , op, other._schema)
        new_query = 'SELECT VALUE %s FROM %s t;' % (schema, aframe_obj._dataverse+'.'+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, new_query)
        actual = aframe_obj.boolean_op(other, op)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)
    
    #self is AFrameObj, other is AFrameObj
    def testAnd(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = AFrameObj('other_datatverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate')
        op = 'AND'
        schema = '%s %s %s' % (aframe_obj._schema , op, other._schema)
        new_query = 'SELECT VALUE %s FROM %s t;' % (schema, aframe_obj._dataverse+'.'+aframe_obj._dataset)

        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, new_query)
        aframe_obj.boolean_op = MagicMock(return_value = expected)
        actual = aframe_obj & other

        aframe_obj.boolean_op.assert_called_once_with(other, op)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #self is AFrameObj, other is AFrameObj
    def testOr(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = AFrameObj('other_datatverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate')
        op = 'OR'
        schema = '%s %s %s' % (aframe_obj._schema , op, other._schema)
        new_query = 'SELECT VALUE %s FROM %s t;' % (schema, aframe_obj._dataverse+'.'+aframe_obj._dataset)

        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, new_query)
        aframe_obj.boolean_op = MagicMock(return_value = expected)
        actual = aframe_obj | other

        aframe_obj.boolean_op.assert_called_once_with(other, op)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testGetDataverse(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = ('test_dataverse', 'test_dataset')
        actual = aframe_obj.get_dataverse()
        self.assertEqual(expected, actual)

    @patch.object(AFrame, 'get_dataset')
    def testToAFrame(self, mock_init):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = af.AFrame('test_dataverse', 'test_dataset')
        actual = aframe_obj.toAframe()
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)

    def testMap_ErrorCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        func = None
        with self.assertRaises(TypeError):
            aframe_obj.map(func)

    #self._predicate is None, values are str
    def testMap_NormalCase1(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', None)
        args1 = 'test_args1'
        args2 = 'test_args2'
        args = [args1, args2]
        kwargs = {'test_key': 'test_value'}
        func = 'test_func'
        args_str = ', "test_args1", "test_args2", {\'test_key\': \'test_value\'}'
        schema = '%s(t.%s%s)' % (func, aframe_obj.schema, args_str)
        dataset = aframe_obj._dataverse+'.'+aframe_obj._dataset
        new_query = 'SELECT VALUE %s(t.%s%s) FROM %s t;' % (func, aframe_obj.schema, args_str, dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, new_query, None)
        actual = aframe_obj.map(func, args1, args2, kwargs)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #self._predicate is not None, values are str
    def testMap_NormalCase2(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        args1 = 'test_args1'
        args2 = 'test_args2'
        args = [args1, args2]
        kwargs = {'test_key': 'test_value'}
        func = 'test_func'
        args_str = ', "test_args1", "test_args2", {\'test_key\': \'test_value\'}'
        schema = '%s(t.%s%s)' % (func, aframe_obj.schema, args_str)
        dataset = aframe_obj._dataverse+'.'+aframe_obj._dataset
        predicate = aframe_obj._predicate
        new_query = 'SELECT VALUE %s(t.%s%s) FROM %s t WHERE %s;' % (func, aframe_obj.schema, args_str, dataset, aframe_obj._predicate)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, new_query, predicate)
        actual = aframe_obj.map(func, args1, args2, kwargs)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #self._predicate is None, values are not str
    def testMap_NormalCase3(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', None)
        args1 = 1
        args2 = 2
        args = [args1, args2]
        kwargs = {'test_key': 3}
        func = 'test_func'
        args_str = ', 1, 2, {\'test_key\': 3}'
        schema = '%s(t.%s%s)' % (func, aframe_obj.schema, args_str)
        dataset = aframe_obj._dataverse+'.'+aframe_obj._dataset
        new_query = 'SELECT VALUE %s(t.%s%s) FROM %s t;' % (func, aframe_obj.schema, args_str, dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, new_query, None)
        actual = aframe_obj.map(func, args1, args2, kwargs)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #self._predicate is not None, values are not str
    def testMap_NormalCase4(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        args1 = 1
        args2 = 2
        args = [args1, args2]
        kwargs = {'test_key': 3}
        func = 'test_func'
        args_str = ', 1, 2, {\'test_key\': 3}'
        schema = '%s(t.%s%s)' % (func, aframe_obj.schema, args_str)
        dataset = aframe_obj._dataverse+'.'+aframe_obj._dataset
        predicate = aframe_obj._predicate
        new_query = 'SELECT VALUE %s(t.%s%s) FROM %s t WHERE %s;' % (func, aframe_obj.schema, args_str, dataset, aframe_obj._predicate)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, new_query, predicate)
        actual = aframe_obj.map(func, args1, args2, kwargs)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #self.schema is None
    def testPersist_ErrorCase1(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            aframe_obj.persist(None, 'name')

    #name is None
    def testPersist_ErrorCase2(self):
        aframe_obj_error = AFrameObj('test_dataverse', 'test_dataset', None, 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            aframe_obj_error.persist('id', None)

    #if dataverse
    @patch.object(AFrame, 'get_dataset')
    def testPersist_NormalCase1(self, mock_init):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrame('test_dataverse', 'test_dataset')

        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        AFrameObj.create_tmp_dataverse = MagicMock(return_value = json_response)
        AFrame.send = MagicMock(return_value = json_response)
        new_q = 'create dataset %s.%s(TempType) primary key _uuid autogenerated;' % ('test_dataverse', 'test_dataset')
        new_q += '\n insert into %s.%s select value ((%s));' % ('test_dataverse', 'test_dataset', aframe_obj.query[:-1])
          
        actual = aframe_obj.persist('test_dataset', 'test_dataverse')
        AFrameObj.create_tmp_dataverse.assert_called_once_with('test_dataverse')
        AFrame.send.assert_called_once_with(new_q)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)

    #No dataverse
    @patch.object(AFrame, 'get_dataset')
    def testPersist_NormalCase2(self, mock_init):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrame('_Temp', 'test_dataset')

        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        AFrameObj.create_tmp_dataverse = MagicMock(return_value = json_response)
        AFrame.send = MagicMock(return_value = json_response)
        new_q = 'create dataset _Temp.%s(TempType) primary key _uuid autogenerated;' % 'test_dataset'
        new_q += '\n insert into _Temp.%s select value ((%s));' % ('test_dataset', aframe_obj.query[:-1])

        actual = aframe_obj.persist('test_dataset', None)
        AFrameObj.create_tmp_dataverse.assert_called_once_with(None)
        AFrame.send.assert_called_once_with(new_q)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)

    def testGetDataType(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        af.AFrame.send_request = MagicMock(return_value = ['test_result1', 'test_result2'])
        expected = 'test_result1'
        query = 'select value t. DatatypeName from Metadata.`Dataset` t where' \
                ' t.DataverseName = \"%s\" and t.DatasetName = \"%s\"' % (aframe_obj._dataverse, aframe_obj._dataset)
        actual = aframe_obj.get_dataType()
        self.assertEqual(expected, actual)

    def testGetPrimaryKey(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        response = [['something1','something2', 'something3'], ['something4','something5', 'something6']]
        af.AFrame.send_request = MagicMock(return_value = response)
        expected = 'something1'
        actual = aframe_obj.get_primary_key()
        self.assertEqual(expected, actual)

    #if name
    @patch.object(AFrame, 'get_dataset')
    def testCreateTmpDataverse_NormalCase1(self, mock_init):
        #aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        name = 'test_name'
        query = 'create dataverse %s if not exists; ' \
                    '\n create type %s.TempType if not exists as open{ _uuid: uuid};' % (name, name)
        af.AFrame.send = MagicMock(return_value = 'tmp_dataverse')

        expected = 'tmp_dataverse'
        actual = AFrameObj.create_tmp_dataverse(name)
        af.AFrame.send.assert_called_once_with(query)
        self.assertEqual(expected, actual)

    #name is None
    @patch.object(AFrame, 'get_dataset')
    def testCreateTmpDataverse_NormalCase1(self, mock_init):
        #aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        query = query = 'create dataverse _Temp if not exists; ' \
                '\n create type _Temp.TempType if not exists as open{ _uuid: uuid};'
        af.AFrame.send = MagicMock(return_value = 'tmp_dataverse')

        expected = 'tmp_dataverse'
        actual = AFrameObj.create_tmp_dataverse()
        af.AFrame.send.assert_called_once_with(query)
        self.assertEqual(expected, actual)

    #key is not str
    def testSetitem_ErrorCase1(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        key = None
        value = AFrameObj('value_dataverse', 'value_dataset', 'value_schema', 'value_query;', 'value_predicate')
        with self.assertRaises(ValueError):
            aframe_obj.__setitem__(key, value)

    #key is str, value is not AFrameObj
    def testSetitem_ErrorCase2(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        key = 'test_key'
        value = None#AFrameObj('value_dataverse', 'value_dataset', 'value_schema', 'value_query;', 'value_predicate')
        with self.assertRaises(ValueError):
            aframe_obj.__setitem__(key, value)

    #self.schema is list
    def testSetitem_NormalCase1(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', ['schema1', 'schema2','schema3'], 'test_query;', 'test_predicate')
        key = 'test_key'
        value = AFrameObj('value_dataverse', 'value_dataset', 'value_schema', 'value_query;', 'value_predicate')
        expected_schema = ['schema1', 'schema2', 'schema3', 'test_key']
        expected_query = 'SELECT t.schema1, t.schema2, t.schema3, value_schema test_key FROM (test_query) t;'
        aframe_obj.__setitem__(key, value)
        actual_schema = aframe_obj.schema
        actual_query = aframe_obj._query
        self.assertEqual(expected_schema, actual_schema)
        self.assertEqual(expected_query, actual_query)

    #self.schema is not list
    def testSetitem_NormalCase2(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        key = 'test_key'
        value = AFrameObj('value_dataverse', 'value_dataset', 'value_schema', 'value_query;', 'value_predicate')
        
        expected_schema = 'value_schema'
        expected_query = 'SELECT t.*, %s %s FROM (%s) t;' % (value.schema, key, aframe_obj.query[:-1])
        aframe_obj.__setitem__(key, value)
        actual_schema = aframe_obj._schema
        actual_query = aframe_obj._query

        self.assertEqual(expected_schema, actual_schema)
        self.assertEqual(expected_query, actual_query)

    #name is not str
    def testWithColumn_ErrorCase1(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        name = None
        col = AFrameObj('other_dataverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate')
        with self.assertRaises(ValueError):
            aframe_obj.withColumn(name, col)

    #col is not AFrameObj
    def testWithColumn_ErrorCase2(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        name = 'test_name'
        col = None#AFrameObj('other_dataverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate')
        with self.assertRaises(ValueError):
            aframe_obj.withColumn(name, col)

    #self.schema is list
    def testWithColumn_NormalCase1(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', ['schema1', 'schema2','schema3'], 'test_query;', 'test_predicate')
        name = 'test_name'
        col = AFrameObj('other_dataverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate')

        expected_schema = ['schema1', 'schema2', 'schema3', 'test_name']
        expected_query = 'SELECT t.schema1, t.schema2, t.schema3, other_schema test_name FROM (test_query) t;'
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, expected_schema, expected_query )
        actual = aframe_obj.withColumn(name, col)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)
        self.assertEqual(aframe_obj._schema, expected_schema)

    #self.schema is not list
    def testWithColumn_NormalCase2(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        name = 'test_name'
        col = AFrameObj('other_dataverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate')

        new_query = 'SELECT t.*, %s %s FROM (%s) t;' % (col.schema, name, aframe_obj.query[:-1])
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, col.schema, new_query)
        actual = aframe_obj.withColumn(name, col)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testGetCount(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        
        af.AFrame.send_request = MagicMock(return_value = [5])
        expected = 5
        actual = aframe_obj.get_count()

        af.AFrame.send_request.assert_called_once_with('SELECT VALUE count(*) FROM (%s) t;' % aframe_obj.query[:-1])
        self.assertEqual(expected, actual)

    #by is str, ascending is True
    def testSortValue_NormalCase1(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        by = 'test_by'
        ascending = True
        expected_query = 'SELECT VALUE t FROM (test_query) t ORDER BY t.test_by ;'
        expected_schema = 'ORDER BY %s' % by

        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, expected_schema, expected_query)
        actual = aframe_obj.sort_values(by, ascending)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #by is str, ascending is False
    def testSortValue_NormalCase2(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        by = 'test_by'
        ascending = False

        expected_query = 'SELECT VALUE t FROM (test_query) t ORDER BY t.test_by DESC;'
        expected_schema = 'ORDER BY %s' % by

        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, expected_schema, expected_query)
        actual = aframe_obj.sort_values(by, ascending)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #by is list, ascending is True
    def testSortValue_NormalCase3(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        by = ['test_by1', 'test_by2', 'test_by3']
        ascending = True
        expected_query = 'SELECT VALUE t FROM (test_query) t ORDER BY , t.test_by2, t.test_by3;'
        expected_schema = 'ORDER BY %s' % by

        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, expected_schema, expected_query)
        actual = aframe_obj.sort_values(by, ascending)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #by is list, ascending is False
    def testSortValue_NormalCase4(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        by = ['test_by1', 'test_by2', 'test_by3']
        ascending = False
        expected_query = 'SELECT VALUE t FROM (test_query) t ORDER BY , t.test_by2, t.test_by3 DESC;'
        expected_schema = 'ORDER BY %s' % by

        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, expected_schema, expected_query)
        actual = aframe_obj.sort_values(by, ascending)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #by is np.ndarray, ascending is True
    def testSortValue_NormalCase5(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        by = np.array([['attr1','attr2'],['attr3','attr4']])
        ascending = True
        
        expected_query = 'SELECT VALUE t FROM (test_query) t ORDER BY , t.[\'attr3\' \'attr4\'];'
        expected_schema = 'ORDER BY %s' % by

        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, expected_schema, expected_query)
        actual = aframe_obj.sort_values(by, ascending)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    #by is np.ndarray, ascending is False
    def testSortValue_NormalCase6(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        by = np.array([['attr1','attr2'],['attr3','attr4']])
        ascending = False
        
        expected_query = 'SELECT VALUE t FROM (test_query) t ORDER BY , t.[\'attr3\' \'attr4\'] DESC;'
        expected_schema = 'ORDER BY %s' % by

        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, expected_schema, expected_query)
        actual = aframe_obj.sort_values(by, ascending)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)

    def testNotna(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, 't.test_schema IS KNOWN', 'SELECT VALUE t FROM (test_query) AS t WHERE t.test_schema IS KNOWN;')
        notna = MagicMock(return_value = expected)
        actual = aframe_obj.notna()

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)        

    def testNotna(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, 't.test_schema IS KNOWN', 'SELECT VALUE t FROM (test_query) AS t WHERE t.test_schema IS KNOWN;')
        actual = notna(aframe_obj)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        self.assertEqual(expected._data, actual._data)
        self.assertEqual(expected._predicate, actual._predicate)
    
if __name__ == '__main__':
    unittest.main()
