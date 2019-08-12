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
    def testGetItem_KeyIsStrSchemaIsNotNone(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema')
        key = 'key'
        dataset = af_obj._dataverse + '.' + af_obj._dataset
        new_query = 'SELECT VALUE t.%s FROM %s t WHERE %s;' % (key, dataset, af_obj._schema)
        actual = af_obj[key]

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('key', actual._schema)
        self.assertEqual(new_query, actual._query)
        self.assertEqual('test_schema', actual._predicate)

    def testGetItem_KeyIsStrQueryIsNotNone(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', None, 'test_query;')
        key = 'key'
        dataset = af_obj._dataverse + '.' + af_obj._dataset
        predicate = None
        new_query = 'SELECT VALUE t.%s FROM (%s) t;' % (key, af_obj._query[:-1])
        actual = af_obj[key]

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('key', actual._schema)
        self.assertEqual(new_query, actual._query)
        self.assertEqual(None, actual._predicate)

    def testGetItem_KeyIsStrSchemaIsNoneQueryIsNone(self):
        af_obj = AFrameObj('test_dataverse', 'test_dataset', None, None)
        key = 'key'
        dataset = af_obj._dataverse + '.' + af_obj._dataset
        predicate = None
        new_query = 'SELECT VALUE t.%s FROM %s t;' % (key, dataset)
        actual = af_obj[key]

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('key', actual._schema)
        self.assertEqual(new_query, actual._query)
        self.assertEqual(None, actual._predicate)

    def testGetItem_KeyIsListSchemaIsNotNone(self):
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
        actual = af_obj[key]

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(['attr1', 'attr2', 'attr3'], actual._schema)
        self.assertEqual(new_query, actual._query)
        self.assertEqual('test_schema', actual._predicate)

    
    def testSchema(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', None, None)
        actual = aframe_obj.schema
        self.assertEqual('test_schema', actual)

    def testQuery(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query', None)
        actual = aframe_obj.query
        self.assertEqual('test_query', actual)

    def testCollect_uuidNotInResultColumns(self):
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

    def testCollect_uuidInResultColumns(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query', 'test_predicate')
        json_response = [{"_uuid": 1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        af.AFrame.send_request = MagicMock(return_value = json_response)
        json_result = af.AFrame.send_request(aframe_obj._query)
        json_str = json.dumps(json_result)
        expected = pd.DataFrame(data = json.read_json(json_str))
        expected.drop('_uuid', axis=1, inplace=True)

        actual = aframe_obj.collect()
        self.assertEqual(actual.equals(expected), True)

    def testToPandas(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query', 'test_predicate')
        aframe_obj.collect = MagicMock(return_value = 'test_pdDataFrame')

        actual = aframe_obj.toPandas()
        self.assertEqual(actual, 'test_pdDataFrame')

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

    def testAdd_ValueIsNotIntNotFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.add(None)

    def testAdd_ValueIsIntOrFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id + 3', 'SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;')
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)

        actual = aframe_obj.add(3)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id + 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;', actual.query)

    def testSub_ValueIsNotIntNotFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.sub(None)

    def testSub_ValueIsIntOrFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id - 3', 'SELECT VALUE t.id - 3 FROM test_dataverse.test_dataset t;')
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.sub(3)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id - 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id - 3 FROM test_dataverse.test_dataset t;', actual.query)

    def testDiv_ValueIsNotIntNotFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.div(None)

    def testDiv_ValueIsIntOrFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id / 3', 'SELECT VALUE t.id / 3 FROM test_dataverse.test_dataset t;')
        
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.div(3)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id / 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id / 3 FROM test_dataverse.test_dataset t;', actual.query)

    def testMul_ValueIsNotIntNotFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.mul(None)

    def testMul_ValueIsIntOrFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id * 3', 'SELECT VALUE t.id * 3 FROM test_dataverse.test_dataset t;')
        
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.mul(3)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id * 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id * 3 FROM test_dataverse.test_dataset t;', actual.query)

    def testMod_ValueIsNotIntNotFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.mod(None)

    def testMod_ValueIsIntOrFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id % 3', 'SELECT VALUE t.id % 3 FROM test_dataverse.test_dataset t;')
        
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.mod(3)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id % 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id % 3 FROM test_dataverse.test_dataset t;', actual.query)

    def testPow_ValueIsNotIntNotFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.pow(None)

    def testPow_ValueIsIntOrFloat(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id ^ 3', 'SELECT VALUE t.id ^ 3 FROM test_dataverse.test_dataset t;')
        
        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.pow(3)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id ^ 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id ^ 3 FROM test_dataverse.test_dataset t;', actual.query)

    def testAddOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id + 3', 'SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.add = MagicMock(return_value=expected)
        actual = aframe_obj+3
        
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id + 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;', actual.query)

    def testSubOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id - 3', 'SELECT VALUE t.id - 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.sub = MagicMock(return_value=expected)
        actual = aframe_obj-3
        
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id - 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id - 3 FROM test_dataverse.test_dataset t;', actual.query)

    def testMulOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id * 3', 'SELECT VALUE t.id * 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.mul = MagicMock(return_value=expected)
        actual = aframe_obj*3
        
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id * 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id * 3 FROM test_dataverse.test_dataset t;', actual.query)

    def testModOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id % 3', 'SELECT VALUE t.id % 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.mod = MagicMock(return_value=expected)
        actual = aframe_obj%3
        
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id % 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id % 3 FROM test_dataverse.test_dataset t;', actual.query)

    def testPowOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id ** 3', 'SELECT VALUE t.id ** 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.pow = MagicMock(return_value=expected)
        actual = aframe_obj**3
        
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id ** 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id ** 3 FROM test_dataverse.test_dataset t;', actual.query) ###^ or ** ###

    def testTrueDivOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id / 3', 'SELECT VALUE t.id / 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.div = MagicMock(return_value=expected)
        actual = aframe_obj/3
        
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id / 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id / 3 FROM test_dataverse.test_dataset t;', actual.query)


    def testLen(self):
        af.AFrame.send_request = MagicMock(return_value = [7])
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')

        query = 'SELECT VALUE count(*) FROM (%s) t;' % aframe_obj.query[:-1]
        actual = aframe_obj.__len__()
        af.AFrame.send_request.assert_called_once_with(query)

        self.assertEqual(7, actual)

    def testArithmeticOp(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id + 3', 'SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;')
        actual = aframe_obj.arithmetic_op(3, '+')

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id + 3', actual.schema)
        self.assertEqual('SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;', actual.query)


    def testMax(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        new_query = 'SELECT max(t.%s) FROM %s t;' % (aframe_obj.schema, aframe_obj._dataverse+'.'+aframe_obj._dataset)

        actual = aframe_obj.max()
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('max(id)', actual._schema)
        self.assertEqual(new_query, actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testMin(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        new_query = 'SELECT min(t.%s) FROM %s t;' % (aframe_obj.schema, aframe_obj._dataverse+'.'+aframe_obj._dataset)

        actual = aframe_obj.min()
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('min(id)', actual._schema)
        self.assertEqual(new_query, actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)
        self.assertEqual(expected._predicate, actual._predicate)

    def testAvg(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        new_query = 'SELECT avg(t.%s) FROM %s t;' % (aframe_obj.schema, aframe_obj._dataverse+'.'+aframe_obj._dataset)

        actual = aframe_obj.avg()
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('avg(id)', actual._schema)
        self.assertEqual(new_query, actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testBinaryOpt_OtherIsStr(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '='
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)

        actual = aframe_obj.binary_opt(other, opt)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset), actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testBinaryOpt_OtherIsNotStr(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = None
        opt = '='
        schema = 't.%s %s %s' % (aframe_obj.schema, opt, other)

        actual = aframe_obj.binary_opt(other, opt)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset), actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)
        
    def testEq(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '='
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)
        aframe_obj.binary_opt = MagicMock(return_value = expected)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj==other
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset), actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testNe(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '!='
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)
        aframe_obj.binary_opt = MagicMock(return_value = expected)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj!=other
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset), actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testGt(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '>'
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)
        aframe_obj.binary_opt = MagicMock(return_value = expected)

        actual = aframe_obj>other
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset), actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testLt(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '<'
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj<other
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset), actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testGe(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '>='
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj>=other
        aframe_obj.binary_opt.assert_called_once_with(other, opt)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset), actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testLe(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = 'test_other'
        opt = '<='
        schema = 't.%s %s \"%s\"' %(aframe_obj.schema, opt, other)
        query = 'SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, query)

        aframe_obj.binary_opt = MagicMock(return_value = expected)
        actual = aframe_obj<=other
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE %s FROM %s t;' %(schema, aframe_obj._dataverse+"."+aframe_obj._dataset), actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testBooleanOp(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        other = AFrameObj('other_datatverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate')
        op = 'test_op'
        schema = '%s %s %s' % (aframe_obj._schema , op, other._schema)
        actual = aframe_obj.boolean_op(other, op)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE %s FROM %s t;' % (schema, aframe_obj._dataverse+'.'+aframe_obj._dataset), actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)
    
    def testAnd_SelfAndOtherAreAFrameObj(self):
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

    def testOr_SelfAndOtherAreAFrameObj(self):
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
        actual = aframe_obj.get_dataverse()
        self.assertEqual(('test_dataverse', 'test_dataset'), actual)

    @patch.object(AFrame, 'get_dataset')
    def testToAFrame(self, mock_init):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        actual = aframe_obj.toAframe()
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)

    def testMap_ErrorCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        func = None
        with self.assertRaises(TypeError):
            aframe_obj.map(func)

    def testMap_PredicateIsNoneValuesAreStr(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', None)
        func = 'test_func'
        
        actual = aframe_obj.map(func, 'test_args1', 'test_args2', {'test_key': 'test_value'})
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('test_func(t.test_schema, "test_args1", "test_args2", {\'test_key\': \'test_value\'})', actual._schema)
        self.assertEqual('SELECT VALUE test_func(t.test_schema, "test_args1", "test_args2", {\'test_key\': \'test_value\'}) FROM test_dataverse.test_dataset t;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testMap_PredicateIsNotNoneValuesAreStr(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        func = 'test_func'
        
        actual = aframe_obj.map(func, 'test_args1', 'test_args2', {'test_key': 'test_value'})
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('test_func(t.test_schema, "test_args1", "test_args2", {\'test_key\': \'test_value\'})', actual._schema)
        self.assertEqual('SELECT VALUE test_func(t.test_schema, "test_args1", "test_args2", {\'test_key\': \'test_value\'}) FROM test_dataverse.test_dataset t WHERE test_predicate;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual('test_predicate', actual._predicate)

    def testMap_PredicateIsNoneValuesAreNotStr(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', None)
        func = 'test_func'

        actual = aframe_obj.map(func, 1, 2, {'test_key': 3})
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('test_func(t.test_schema, 1, 2, {\'test_key\': 3})', actual._schema)
        self.assertEqual('SELECT VALUE test_func(t.test_schema, 1, 2, {\'test_key\': 3}) FROM test_dataverse.test_dataset t;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testMap_PredicateIsNotNoneValuesAreNotStr(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        func = 'test_func'

        actual = aframe_obj.map(func, 1, 2, {'test_key': 3})
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('test_func(t.test_schema, 1, 2, {\'test_key\': 3})', actual._schema)
        self.assertEqual('SELECT VALUE test_func(t.test_schema, 1, 2, {\'test_key\': 3}) FROM test_dataverse.test_dataset t WHERE test_predicate;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual('test_predicate', actual._predicate)

    def testPersist_SchemaIsNone(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            aframe_obj.persist(None, 'name')

    def testPersist_NameIsNone(self):
        aframe_obj_error = AFrameObj('test_dataverse', 'test_dataset', None, 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            aframe_obj_error.persist('id', None)

    @patch.object(AFrame, 'get_dataset')
    def testPersist_DataverseNotNone(self, mock_init):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
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
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)

    @patch.object(AFrame, 'get_dataset')
    def testPersist_DataverseNone(self, mock_init):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')

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
        self.assertEqual('_Temp', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)

    def testGetDataType(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        af.AFrame.send_request = MagicMock(return_value = ['test_result1', 'test_result2'])
        actual = aframe_obj.get_dataType()
        self.assertEqual('test_result1', actual)

    def testGetPrimaryKey(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        response = [['something1','something2', 'something3'], ['something4','something5', 'something6']]
        af.AFrame.send_request = MagicMock(return_value = response)
        actual = aframe_obj.get_primary_key()
        self.assertEqual('something1', actual)

    @patch.object(AFrame, 'get_dataset')
    def testCreateTmpDataverse_NameNotNone(self, mock_init):
        name = 'test_name'
        query = 'create dataverse %s if not exists; ' \
                    '\n create type %s.TempType if not exists as open{ _uuid: uuid};' % (name, name)
        af.AFrame.send = MagicMock(return_value = 'tmp_dataverse')

        actual = AFrameObj.create_tmp_dataverse(name)
        af.AFrame.send.assert_called_once_with(query)
        self.assertEqual('tmp_dataverse', actual)

    @patch.object(AFrame, 'get_dataset')
    def testCreateTmpDataverse_NameIsNone(self, mock_init):
        query = query = 'create dataverse _Temp if not exists; ' \
                '\n create type _Temp.TempType if not exists as open{ _uuid: uuid};'
        af.AFrame.send = MagicMock(return_value = 'tmp_dataverse')

        actual = AFrameObj.create_tmp_dataverse(None)
        af.AFrame.send.assert_called_once_with(query)
        self.assertEqual('tmp_dataverse', actual)

    def testSetitem_KeyIsNotStr(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        key = None
        value = AFrameObj('value_dataverse', 'value_dataset', 'value_schema', 'value_query;', 'value_predicate')
        with self.assertRaises(ValueError):
            aframe_obj[key] = value

    def testSetitem_KeyIsStrValueIsNotAFrameObj(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        key = 'test_key'
        value = None
        with self.assertRaises(ValueError):
            aframe_obj[key] = value

    def testSetitem_SchemaIsList(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', ['schema1', 'schema2','schema3'], 'test_query;', 'test_predicate')
        key = 'test_key'
        value = AFrameObj('value_dataverse', 'value_dataset', 'value_schema', 'value_query;', 'value_predicate')
        aframe_obj[key] = value
        actual_query = aframe_obj._query
        self.assertEqual(['schema1', 'schema2', 'schema3', 'test_key'], aframe_obj.schema)
        self.assertEqual('SELECT t.schema1, t.schema2, t.schema3, value_schema test_key FROM (test_query) t;', aframe_obj._query)

    def testSetitem_SchemaIsNotList(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        key = 'test_key'
        value = AFrameObj('value_dataverse', 'value_dataset', 'value_schema', 'value_query;', 'value_predicate')
        
        aframe_obj[key] = value
        self.assertEqual('value_schema', aframe_obj._schema)
        self.assertEqual('SELECT t.*, value_schema test_key FROM (test_query) t;', aframe_obj._query)

    def testWithColumn_NameIsNotStr(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        with self.assertRaises(ValueError):
            aframe_obj.withColumn(None, AFrameObj('other_dataverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate'))

    def testWithColumn_ColIsNotAFrameObj(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        with self.assertRaises(ValueError):
            aframe_obj.withColumn('test_name', None)

    def testWithColumn_SchemaIsList(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', ['schema1', 'schema2','schema3'], 'test_query;', 'test_predicate')
        actual = aframe_obj.withColumn('test_name', AFrameObj('other_dataverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate'))

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(['schema1', 'schema2', 'schema3', 'test_name'], actual._schema)
        self.assertEqual('SELECT t.schema1, t.schema2, t.schema3, other_schema test_name FROM (test_query) t;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)
        self.assertEqual(aframe_obj._schema, ['schema1', 'schema2', 'schema3', 'test_name'])

    def testWithColumn_SchemaIsNotList(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        actual = aframe_obj.withColumn('test_name', AFrameObj('other_dataverse', 'other_dataset', 'other_schema', 'other_query', 'other_predicate'))

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('other_schema', actual._schema)
        self.assertEqual('SELECT t.*, other_schema test_name FROM (test_query) t;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testGetCount(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        af.AFrame.send_request = MagicMock(return_value = [5])
        actual = aframe_obj.get_count()

        af.AFrame.send_request.assert_called_once_with('SELECT VALUE count(*) FROM (%s) t;' % aframe_obj.query[:-1])
        self.assertEqual(5, actual)
        
    def testSortValue_ByIsStrAscendingIsTrue(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        actual = aframe_obj.sort_values('test_by', True)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('ORDER BY test_by', actual._schema)
        self.assertEqual('SELECT VALUE t FROM (test_query) t ORDER BY t.test_by ;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testSortValue_ByIsStrAscendingIsFalse(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        actual = aframe_obj.sort_values('test_by', False)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('ORDER BY test_by', actual._schema)
        self.assertEqual('SELECT VALUE t FROM (test_query) t ORDER BY t.test_by DESC;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testSortValue_ByIsListAscendingIsTrue(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        actual = aframe_obj.sort_values(['test_by1', 'test_by2', 'test_by3'], True)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('ORDER BY [\'test_by1\', \'test_by2\', \'test_by3\']', actual._schema)
        self.assertEqual('SELECT VALUE t FROM (test_query) t ORDER BY , t.test_by2, t.test_by3;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testSortValue_ByIsListAscendingIsFalse(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        actual = aframe_obj.sort_values(['test_by1', 'test_by2', 'test_by3'], False)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('ORDER BY [\'test_by1\', \'test_by2\', \'test_by3\']', actual._schema)
        self.assertEqual('SELECT VALUE t FROM (test_query) t ORDER BY , t.test_by2, t.test_by3 DESC;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testSortValue_ByIsNdarrayAscendingIsTrue(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        expected_schema = 'ORDER BY %s' % np.array([['attr1','attr2'],['attr3','attr4']])
        actual = aframe_obj.sort_values(np.array([['attr1','attr2'],['attr3','attr4']]), True)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(expected_schema, actual._schema)
        self.assertEqual('SELECT VALUE t FROM (test_query) t ORDER BY , t.[\'attr3\' \'attr4\'];', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testSortValue_ByIsNdarrayAscendingIsFalse(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        expected_schema = 'ORDER BY %s' % np.array([['attr1','attr2'],['attr3','attr4']])
        actual = aframe_obj.sort_values(np.array([['attr1','attr2'],['attr3','attr4']]), False)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(expected_schema, actual._schema)
        self.assertEqual('SELECT VALUE t FROM (test_query) t ORDER BY , t.[\'attr3\' \'attr4\'] DESC;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)

    def testNotna(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, 't.test_schema IS KNOWN', 'SELECT VALUE t FROM (test_query) AS t WHERE t.test_schema IS KNOWN;')
        notna = MagicMock(return_value = expected)
        actual = aframe_obj.notna()

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('t.test_schema IS KNOWN', actual._schema)
        self.assertEqual('SELECT VALUE t FROM (test_query) AS t WHERE t.test_schema IS KNOWN;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)        

    def testNotna_ClassMissing(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'test_schema', 'test_query;', 'test_predicate')
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, 't.test_schema IS KNOWN', 'SELECT VALUE t FROM (test_query) AS t WHERE t.test_schema IS KNOWN;')
        actual = notna(aframe_obj)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('t.test_schema IS KNOWN', actual._schema)
        self.assertEqual('SELECT VALUE t FROM (test_query) AS t WHERE t.test_schema IS KNOWN;', actual._query)
        self.assertEqual(None, actual._data)
        self.assertEqual(None, actual._predicate)  
    
if __name__ == '__main__':
    unittest.main()
