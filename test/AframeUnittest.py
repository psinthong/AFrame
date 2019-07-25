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

    @patch('aframe.AFrame.get_dataset')
    def testSimpleInit(self, mock_method):
        af = AFrame('test_dataverse', 'test_dataset')
        mock_method.assert_called_once_with('test_dataset')
        self.assertEqual(af._dataverse, 'test_dataverse')
        self.assertEqual(af._dataset, 'test_dataset')
        self.assertIsNone(af._columns)
        self.assertIsNone(af._datatype)
        self.assertIsNone(af._datatype_name)
        self.assertIsNone(af.query)

    @patch.object(AFrame, 'get_dataset')
    def testLen(self, mock_init):
        expected = 7
        af = AFrame('test_dataverse', 'test_dataset')
        mock_init.assert_called_once_with('test_dataset')

        af.send_request = MagicMock(return_value=[expected])
        actual_len = len(af)
        af.send_request.assert_called_once_with('SELECT VALUE count(*) FROM test_dataverse.test_dataset;')
        self.assertEqual(expected, actual_len)

    @patch.object(AFrame, 'get_dataset')
    def testSimpleHead(self, mock_init):
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value = json_response)

        actual = af.head()

        af.send_request.assert_called_once_with('SELECT VALUE t FROM test_dataverse.test_dataset t limit 5;')
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

    @patch.object(AFrame, 'get_dataset')
    def testHeadWithSample(self, mock_init):
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                          {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                          {"attr1":5, "attr2":"str5"}, {"attr1":6, "attr2":"str6"},
                          {"attr1":7, "attr2":"str7"}, {"attr1":8, "attr2":"str8"},
                          {"attr1":9, "attr2":"str9"}, {"attr1":10,"attr2":"str10"}]
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value = json_response)

        actual = af.head(10)
        af.send_request.assert_called_once_with('SELECT VALUE t FROM test_dataverse.test_dataset t limit 10;')
        self.assertEqual(len(actual), 10)
        row0 = pd.Series([1, 'str1'], index=['attr1', 'attr2'])
        row1 = pd.Series([2, 'str2'], index=['attr1', 'attr2'])
        row2 = pd.Series([3, 'str3'], index=['attr1', 'attr2'])
        row3 = pd.Series([4, 'str4'], index=['attr1', 'attr2'])
        row4 = pd.Series([5, 'str5'], index=['attr1', 'attr2'])
        row5 = pd.Series([6, 'str6'], index=['attr1', 'attr2'])
        row6 = pd.Series([7, 'str7'], index=['attr1', 'attr2'])
        row7 = pd.Series([8, 'str8'], index=['attr1', 'attr2'])
        row8 = pd.Series([9, 'str9'], index=['attr1', 'attr2'])
        row9 = pd.Series([10, 'str10'], index=['attr1', 'attr2'])
        self.assertTrue(actual.iloc[0].equals(row0))
        self.assertTrue(actual.iloc[1].equals(row1))
        self.assertTrue(actual.iloc[2].equals(row2))
        self.assertTrue(actual.iloc[3].equals(row3))
        self.assertTrue(actual.iloc[4].equals(row4))
        self.assertTrue(actual.iloc[5].equals(row5))
        self.assertTrue(actual.iloc[6].equals(row6))
        self.assertTrue(actual.iloc[7].equals(row7))
        self.assertTrue(actual.iloc[8].equals(row8))
        self.assertTrue(actual.iloc[9].equals(row9))
        

    @patch.object(AFrame, 'get_dataset')
    def testGetCount(self, mock_init):
        expected = 10
        af = AFrame('test_dataverse', 'test_dataset')
        
        af.send_request = MagicMock(return_value = [expected])
        actual_count = af.get_count()
        af.send_request.assert_called_once_with('SELECT VALUE count(*) FROM test_dataverse.test_dataset;')
        self.assertEqual(expected, actual_count)

    @patch.object(AFrame, 'get_dataset')
    def testToPandas_Exception(self, mock_init):
        from pandas.io import json
        af = AFrame('test_dataverse', None)
        with self.assertRaises(ValueError):
            af.toPandas()

    #sample == 0
    @patch.object(AFrame, 'get_dataset')
    def testToPandas_NormalCase1(self, mock_init):
        from pandas.io import json
        
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        
        
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value = json_response)

        actual = af.toPandas()

        af.send_request.assert_called_once_with('SELECT VALUE t FROM test_dataverse.test_dataset t;')
        self.assertEqual(len(actual), 5)
        data = json.read_json(json.dumps(actual))
        df = pd.DataFrame(data)
        row0 = pd.Series([1, 'str1'], index=['attr1', 'attr2'])
        row1 = pd.Series([2, 'str2'], index=['attr1', 'attr2'])
        row2 = pd.Series([3, 'str3'], index=['attr1', 'attr2'])
        row3 = pd.Series([4, 'str4'], index=['attr1', 'attr2'])
        row4 = pd.Series([5, 'str5'], index=['attr1', 'attr2'])
        self.assertTrue(df.loc[0].equals(row0))
        self.assertTrue(df.loc[1].equals(row1))
        self.assertTrue(df.loc[2].equals(row2))
        self.assertTrue(df.loc[3].equals(row3))
        self.assertTrue(df.loc[4].equals(row4))

    #sample > 0, self.query is None
    @patch.object(AFrame, 'get_dataset')
    def testToPandas_NormalCase2(self, mock_init):
        from pandas.io import json
        
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value = json_response)

        sample = 5
        dataset = af._dataverse+'.'+af._dataset
        query = 'SELECT VALUE t FROM %s t LIMIT %d;' % (dataset, sample)
        actual = af.toPandas(sample)

        af.send_request.assert_called_once_with(query)
        self.assertEqual(len(actual), 5)
        data = json.read_json(json.dumps(actual))
        df = pd.DataFrame(data)
        row0 = pd.Series([1, 'str1'], index=['attr1', 'attr2'])
        row1 = pd.Series([2, 'str2'], index=['attr1', 'attr2'])
        row2 = pd.Series([3, 'str3'], index=['attr1', 'attr2'])
        row3 = pd.Series([4, 'str4'], index=['attr1', 'attr2'])
        row4 = pd.Series([5, 'str5'], index=['attr1', 'attr2'])
        self.assertTrue(df.loc[0].equals(row0))
        self.assertTrue(df.loc[1].equals(row1))
        self.assertTrue(df.loc[2].equals(row2))
        self.assertTrue(df.loc[3].equals(row3))
        self.assertTrue(df.loc[4].equals(row4))

    #sample > 0, self.query is not None
    '''
    @patch.object(AFrame, 'get_dataset')
    def testToPandas_NormalCase3(self, mock_init):
        from pandas.io import json
        
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        
        af = AFrame('test_dataverse', 'test_dataset')
        af.query = 'SELECT VALUE t FROM test_dataverse.test_dataset t;'
        af.send_request = MagicMock(return_value = json_response)

        sample = 5
        dataset = af._dataverse+'.'+af._dataset
        query = 'SELECT VALUE t FROM (%s) t LIMIT %d;' % (dataset, af.query[:-1])
        actual = af.toPandas(sample)

        af.send_request.assert_called_once_with(query)
        self.assertEqual(len(actual), 5)
        data = json.read_json(json.dumps(actual))
        df = pd.DataFrame(data)
        row0 = pd.Series([1, 'str1'], index=['attr1', 'attr2'])
        row1 = pd.Series([2, 'str2'], index=['attr1', 'attr2'])
        row2 = pd.Series([3, 'str3'], index=['attr1', 'attr2'])
        row3 = pd.Series([4, 'str4'], index=['attr1', 'attr2'])
        row4 = pd.Series([5, 'str5'], index=['attr1', 'attr2'])
        self.assertTrue(df.loc[0].equals(row0))
        self.assertTrue(df.loc[1].equals(row1))
        self.assertTrue(df.loc[2].equals(row2))
        self.assertTrue(df.loc[3].equals(row3))
        self.assertTrue(df.loc[4].equals(row4))'''

    @patch.object(AFrame, 'get_dataset')
    def testCollectQuery_NormalCase(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        query = af.collect_query()
        self.assertEqual(query, 'SELECT VALUE t FROM test_dataverse.test_dataset t;')


    @patch.object(AFrame, 'get_dataset')
    def testCollectQuery_ErrorCase(self, mock_init):
        af1 = AFrame('test_dataverse', None)
        self.assertRaises(ValueError, af1.collect_query)
        

    @patch.object(AFrame, 'get_dataset')
    def testApply(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        actual = af.apply('get_object_fields') 

        new_query = 'SELECT VALUE get_object_fields(t) FROM test_dataverse.test_dataset t;'
        expected = AFrameObj('test_dataverse', 'test_dataset', 'get_object_fields(t)', new_query)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)
        
    @patch.object(AFrame, 'get_dataset')
    def testUnnest_ErrorCase(self, mock_init):
        new_query = 'SELECT VALUE get_object_fields(t) FROM test_dataverse.test_dataset t;'
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'get_object_fields(t)', new_query)
        
        af = AFrame('test_dataverse', 'test_dataset')
        with self.assertRaises(ValueError):
            af.unnest(1)
            af.unnest(aframe_obj, None, True, None)

    @patch.object(AFrame, 'get_dataset')
    def testUnnest_NormalCase(self, mock_init):
        new_query = 'SELECT VALUE get_object_fields(t) FROM test_dataverse.test_dataset t;'
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'get_object_fields(t)', new_query)

        af = AFrame('test_dataverse', 'test_dataset')
        schema = 'unnest(%s)' % aframe_obj.schema
        expected_query = 'SELECT VALUE e FROM (%s) t unnest t e;' % aframe_obj.query[:-1]
        expected = AFrameObj('test_dataverse', 'test_dataset', schema, expected_query)
        actual = af.unnest(aframe_obj, False)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    @patch.object(AFrame, 'get_dataset')
    def testToAFrameObj(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset') 
        expected = AFrameObj('test_dataverse', 'test_dataset', None, None)
        actual = af.toAFrameObj()
        self.assertEqual(actual, None)

        new_query = 'SELECT VALUE get_object_fields(t) FROM test_dataverse.test_dataset t;'
        af.query = new_query
        expected2 = AFrameObj('test_dataverse', 'test_dataset', None, new_query[:-1])
        actual2 = af.toAFrameObj()
        self.assertEqual(expected2._dataverse, actual2._dataverse)
        self.assertEqual(expected2._dataset, actual2._dataset)
        self.assertEqual(expected2.schema, actual2.schema)
        self.assertEqual(expected2.query, actual2.query)

    def testArithmeticOp(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id + 3', 'SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;')
        actual = aframe_obj.arithmetic_op(3, '+')

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

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
        
    def testAddOperator(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id + 3', 'SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;')

        aframe_obj.add = MagicMock(return_value=expected)
        actual = aframe_obj+3
        
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

    @patch.object(AFrame, 'get_dataset')
    def testGetColumnCount_ErrorCase(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            af.get_column_count(None)

    @patch.object(AFrame, 'get_dataset')
    def testGetColumnCount_NormalCase(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        expected = 5
        AFrame.send_request = MagicMock(return_value = [expected])
        actual = af.get_column_count(aframe_obj)
        af.send_request.assert_called_once_with('SELECT VALUE count(*) FROM (%s) t;' % aframe_obj.query[:-1])

    @patch.object(AFrame, 'get_dataset')
    def testWithColumn_ErrorCase(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', None, None)
        with self.assertRaises(ValueError):
            af.withColumn(None, aframe_obj)
        with self.assertRaises(ValueError):
            af.withColumn('id', None)

    @patch.object(AFrame, 'get_dataset')
    def testWithColumn_NormalCase(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')

        new_query = 'SELECT t.*, %s %s FROM %s t;' % (aframe_obj.schema, 'id', 'test_dataverse.test_dataset')
        schema = aframe_obj.schema
        expected = AFrameObj('test_dataverse', 'test_dataset', schema, new_query)

        actual = af.withColumn('id', aframe_obj)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    def testCreateTmpDataverseNone(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')       
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        AFrame.send = MagicMock(return_value = json_response)

        actual = aframe_obj.create_tmp_dataverse(None)
        query = 'create dataverse _Temp if not exists; ' \
                '\n create type _Temp.TempType if not exists as open{ _uuid: uuid};'
        AFrame.send.assert_called_once_with(query)
        self.assertEqual(json_response, actual)

    def testCreateTmpDataverseNotNone(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')       
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        AFrame.send = MagicMock(return_value = json_response)

        actual = aframe_obj.create_tmp_dataverse('id')
        query = 'create dataverse %s if not exists; ' \
                    '\n create type %s.TempType if not exists as open{ _uuid: uuid};' % ('id', 'id')
        AFrame.send.assert_called_once_with(query)
        self.assertEqual(json_response, actual)

    def testPersist_ErrorCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            aframe_obj.persist(None, None)

        aframe_obj_error = AFrameObj('test_dataverse', 'test_dataset', None, 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            aframe_obj_error.persist('id', None)

    @patch.object(AFrame, 'get_dataset')
    def testPersistNormalWithDataverse(self, mock_init):
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
        
    @patch.object(AFrame, 'get_dataset')
    def testPersistNormalNoDataverse(self, mock_init):
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

    @patch.object(AFrame, 'get_dataset')
    def testDrop(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')

        expected = []
        AFrame.send = MagicMock(return_value = expected)
        
        actual = AFrame.drop(af)
        query = 'DROP DATASET %s.%s;' % ('test_dataverse', 'test_dataset')
        AFrame.send.assert_called_once_with(query)
        self.assertEqual(expected, actual)

    def testMap_funcNotStr(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(TypeError):
            aframe_obj.map(None)

    def testMap_normalCase_NoPredicate(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        aframe_obj._predicate = None

        func = 'len'
        schema = '%s(t.%s%s)' % (func, aframe_obj.schema, '')
        dataset = aframe_obj._dataverse + '.' + aframe_obj._dataset
        new_query = 'SELECT VALUE %s(t.%s%s) FROM %s t;' % (func, aframe_obj.schema, '', dataset)
        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, new_query)
        expected._predicate = None

        actual = aframe_obj.map(func)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)
        #print(expected._predicate)
        #print(actual._predicate)
        #self.assertEqual(expected._predicate, actual._predicate)
        
##    def testMap_normalCase_withPredicate(self):
##        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
##        aframe_obj._predicate = 't.id LIKE \'1%\''
##
##        func = 'len'
##        dataset = aframe_obj._dataverse + '.' + aframe_obj._dataset
##        schema = '%s(t.%s%s)' % (func, aframe_obj.schema, '')
##        new_query = 'SELECT VALUE %s(t.%s%s) FROM %s t WHERE %s;' % (func, aframe_obj.schema, '', dataset, aframe_obj._predicate)
##        expected = AFrameObj(aframe_obj._dataverse, aframe_obj._dataset, schema, new_query)
##        expected._predicate = 't.id LIKE \'1%\''
##
##        actual = aframe_obj.map(func)
##        self.assertEqual(expected._dataverse, actual._dataverse)
##        self.assertEqual(expected._dataset, actual._dataset)
##        self.assertEqual(expected._schema, actual._schema)
##        self.assertEqual(expected._query, actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testStr_Empty(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        expected = 'Empty AsterixDB DataFrame'
        actual = str(af)
        self.assertEqual(expected, actual)

    @patch.object(AFrame, 'get_dataset')
    def testStr_Empty(self, mock_init):
        columns = ['name','age','GPA']
        af = AFrame('test_dataverse', 'test_dataset', columns)
        txt = 'AsterixDB DataFrame with the following pre-defined columns: \n\t'
        expected = txt + str(af._columns)
        actual = str(af)
        self.assertEqual(expected, actual)

    @patch.object(AFrame, 'get_dataset')
    def testRepr(self, mock_init):
        af = AFrame('test_dataverse','test_dataset')
        af.__str__ = MagicMock(return_value = 'Empty AsterixDB DataFrame')

        expected = 'Empty AsterixDB DataFrame'
        actual = repr(af)
        af.__str__.assert_called_once()
        self.assertEqual(expected, actual)

    @patch.object(AFrame, 'get_dataset')
    def testGetItem_AFrameObjKey_NoQuery(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 't.id>0')
        dataset = af._dataverse + '.' + af._dataset
        new_query = 'SELECT VALUE t FROM %s t WHERE %s;' %(dataset, aframe_obj.schema)
        expected = AFrameObj('test_dataverse', 'test_dataset', aframe_obj.schema, new_query)
        actual = af.__getitem__(aframe_obj)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

##    @patch.object(AFrame, 'get_dataset')
##    def testGetItem_AFrameObjKey_WithQuery(self, mock_init):
##        af = AFrame('test_dataverse', 'test_dataset')
##        af_query = 'SELECT t.* FROM %s t;' % 'test_dataverse.test_dataset'
##        af.query = af_query
##        print(af.query)
##        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 't.id>0')
##        dataset = af._dataverse + '.' + af._dataset
##        print('dataset:', dataset)
##        print('af.query:', af.query[:-1])
##        print('aframe_obj.schema:', aframe_obj.schema)
##        new_query = 'SELECT VALUE t FROM (%s) t WHERE %s;' % (dataset, aframe_obj.schema)
##        #should be new_query = 'SELECT VALUE t FROM (%s) t WHERE %s;' % (dataset, af.query[:-1], aframe_obj.schema)
##        #TypeError: not all arguments converted during string formatting
##        print(new_query)
##        expected = AFrameObj('test_dataverse', 'test_dataset', aframe_obj.schema, new_query)
##        print(af.query is None)
##        actual = af.__getitem__(aframe_obj)
##        print(expected._query)
##        print(actual._query)
##        self.assertEqual(expected._dataverse, actual._dataverse)
##        self.assertEqual(expected._dataset, actual._dataset)
##        self.assertEqual(expected._schema, actual._schema)
##        self.assertEqual(expected._query, actual._query)
        
    @patch.object(AFrame, 'get_dataset')
    def testGetItem_StrKey_NoQuery(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse + '.' + af._dataset
        key = 'id'
        query = 'SELECT VALUE t.%s FROM %s t;' % (key, dataset)

        expected = AFrameObj('test_dataverse', 'test_dataset', key, query)
        actual = af.__getitem__(key)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testGetItem_StrKey_WithQuery(self, mock_init):
        af_query = 'SELECT VALUE t.* FROM %s t;' % ('test_dataverse.test_dataset')
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse + '.' + af._dataset
        af.query = af_query
        key = 'id'
        query = 'SELECT VALUE t.%s FROM (%s) t;' % (key, af.query[:-1])

        expected = AFrameObj('test_dataverse', 'test_dataset', key, query)
        actual = af.__getitem__(key)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testGetItem_ListKey_NoQuery(self, mock_init):
        key = [1,2,3,4,5]
        fields = ''
        for i in range(len(key)):
            if i > 0:
                fields += ', '
            fields += 't.%s'%key[i]
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse+'.'+af._dataset
        query = 'SELECT %s FROM %s t;' % (fields, dataset)
        expected = AFrameObj(af._dataverse, af._dataset, key, query)

        actual = af.__getitem__(key)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testGetItem_ListKey_WithQuery(self, mock_init):
        key = [1,2,3,4,5]
        fields = ''
        for i in range(len(key)):
            if i > 0:
                fields += ', '
            fields += 't.%s'%key[i]
        af = AFrame('test_dataverse', 'test_dataset')
        af_query = 'SELECT t.* FROM %s t;' % 'test_dataverse.test_dataset'
        af.query = af_query
        dataset = af._dataverse+'.'+af._dataset
        query = 'SELECT %s FROM (%s) t;' % (fields, af.query[:-1])
        expected = AFrameObj(af._dataverse, af._dataset, key, query)

        actual = af.__getitem__(key)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testGetItem_NdarrayKey_NoQuery(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        key = np.array([[1,2],[3,4]])
        fields = ''
        for i in range(len(key)):
            if i > 0:
                fields += ', '
            fields += 't.%s'%key[i]
        dataset = af._dataverse+'.'+af._dataset
        query = 'SELECT %s FROM %s t;' % (fields, dataset)
        expected = AFrameObj(af._dataverse, af._dataset, key, query)

        actual = af.__getitem__(key)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema.all(), actual._schema.all())
        self.assertEqual(expected._query, actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testSetItem_Exception(self, mock_init):
        af = AFrame('test_dataverse','test_dataset')
        columns = 'id'
        on = None
        query = None
        value = OrderedAFrame('test_dataverse','test_dataset', columns, on, query)
        with self.assertRaises(ValueError):
            af.__setitem__(1, value)

    @patch.object(AFrame, 'get_dataset')
    def testSetItem_NormalCase(self, mock_init):
        af = AFrame('test_dataverse','test_dataset')
        columns = 'id'
        on = None
        query = None
        value = OrderedAFrame('test_dataverse','test_dataset', columns, on, query)
        key = 'id'
        dataset = af._dataverse+'.'+af._dataset
        new_query = 'SELECT t.*, %s %s FROM %s t;' % (value._columns, key, dataset)
        af.__setitem__(key, value)
        expected = new_query
        actual = af.query
        self.assertEqual(expected, actual)

    @patch.object(AFrame, 'get_dataset')
    def testFlatten(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        expected = NestedAFrame(af._dataverse, af._dataset, af.columns, af.query)
        actual = af.flatten()
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.columns, actual.columns)
        self.assertEqual(expected._query, actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testColumns(self, mock_init):
        columns = ['name', 'id', 'GPA']
        af = AFrame('test_dataverse', 'test_dataset', columns)
        expected = columns

        actual = af.columns
        self.assertEqual(expected, actual)

    @patch.object(AFrame, 'get_dataset')
    def testCollectQuery_Exception(self, mock_init):
        af = AFrame('test_dataverse', None)
        with self.assertRaises(ValueError):
            af.collect_query()

    @patch.object(AFrame, 'get_dataset')
    def testCollectQuery_NormalCase(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse+'.'+af._dataset
        expected = 'SELECT VALUE t FROM %s t;' % dataset

        actual = af.collect_query()
        self.assertEqual(expected, actual)

    def test_attach_row_id_empty(self):
        result_lst = []
        expected = result_lst
        actual = AFrame.attach_row_id(result_lst)
        self.assertEqual(expected, actual)

    #can't define a result_lst
    '''
    def test_attach_row_id_NormalCase(self):
        result_lst = [['row_id', 'data']]
        flatten_results = []
        for i in result_lst:
            i['data']['row_id'] = i['row_id']
            flatten_results.append(i['data'])
        expected = flatten_results
        print(expected)
        
        actual = AFrame.attach_row_id(result_lst)
        print(actual)'''

    @patch.object(AFrame, 'get_dataset')
    def testNotna(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        notna = MagicMock(side_effect=NotImplementedError)
        with self.assertRaises(NotImplementedError):
            af.notna()

#don't know how to test...
##    @patch.object(AFrame, 'get_dataset')
##    def testCreate(self, mock_init):
        
    @patch.object(AFrame, 'get_dataset')
    def testInitColumns(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        columns = None
        with self.assertRaises(ValueError):
            af.init_columns(columns)

##    def testGetDataset(self):

    @patch.object(AFrame, 'get_dataset')
    def testJoin_ValueError(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        left_on = None
        right_on = 'something'
        with self.assertRaises(ValueError):
            af.join(other, left_on, right_on)

        left_on = 'something'
        right_on = None
        with self.assertRaises(ValueError):
            af.join(other, left_on, right_on)

        left_on = None
        right_on = None
        with self.assertRaises(ValueError):
            af.join(other, left_on, right_on)

    @patch.object(AFrame, 'get_dataset')
    def testJoin_NotImplementedError(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        left_on = 'something'
        right_on = 'something'
        how = 'right'
        with self.assertRaises(NotImplementedError):
            af.join(other, left_on, right_on, how)

    #self.query is None, other.query is None, left_on != right_on
    @patch.object(AFrame, 'get_dataset')
    def testJoin_NormalCase1(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        l_dataset = af._dataverse+'.'+af._dataset
        r_dataset = other._dataverse+'.'+other._dataset
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'anotherthing'
        query = 'SELECT VALUE object_merge(%s,%s) '% (lsuffix, rsuffix) + 'FROM %s %s ' %(l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' %(r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        expected = AFrameObj(af._dataverse, af._dataset, schema, query)

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #self.query is not None, other.query is None, left_on != right_on
    @patch.object(AFrame, 'get_dataset')
    def testJoin_NormalCase2(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        af.query = 'af_query'
        other = AFrame('other_dataverse', 'other_dataset')
        l_dataset = '(%s)' % af.query[:-1]
        r_dataset = other._dataverse+'.'+other._dataset
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'anotherthing'
        query = 'SELECT VALUE object_merge(%s,%s) '% (lsuffix, rsuffix) + 'FROM %s %s ' %(l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' %(r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        expected = AFrameObj(af._dataverse, af._dataset, schema, query)

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #self.query is None, other.query is not None, left_on != right_on
    @patch.object(AFrame, 'get_dataset')
    def testJoin_NormalCase3(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        other.query = 'other_query'
        l_dataset = af._dataverse+'.'+af._dataset
        r_dataset = '(%s)' % other.query[:-1]
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'anotherthing'
        query = 'SELECT VALUE object_merge(%s,%s) '% (lsuffix, rsuffix) + 'FROM %s %s ' %(l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' %(r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        expected = AFrameObj(af._dataverse, af._dataset, schema, query)

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #self.query is not None, other.query is not None, left_on != right_on
    @patch.object(AFrame, 'get_dataset')
    def testJoin_NormalCase4(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        af.query = 'af_query'
        other.query = 'other_query'
        l_dataset = '(%s)' % af.query[:-1]
        r_dataset = '(%s)' % other.query[:-1]
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'anotherthing'
        query = 'SELECT VALUE object_merge(%s,%s) '% (lsuffix, rsuffix) + 'FROM %s %s ' %(l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' %(r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        expected = AFrameObj(af._dataverse, af._dataset, schema, query)

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #self.query is None, other.query is None, left_on == right_on
    @patch.object(AFrame, 'get_dataset')
    def testJoin_NormalCase5(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        l_dataset = af._dataverse+'.'+af._dataset
        r_dataset = other._dataverse+'.'+other._dataset
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'something'
        query = 'SELECT %s,%s '% (lsuffix, rsuffix) + 'from %s %s ' % (l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        expected = AFrameObj(af._dataverse, af._dataset, schema, query)

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #self.query is not None, other.query is None, left_on == right_on
    @patch.object(AFrame, 'get_dataset')
    def testJoin_NormalCase6(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        af.query = 'af_query'
        other = AFrame('other_dataverse', 'other_dataset')
        l_dataset = '(%s)' % af.query[:-1]
        r_dataset = other._dataverse+'.'+other._dataset
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'something'
        query = 'SELECT %s,%s '% (lsuffix, rsuffix) + 'from %s %s ' % (l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        expected = AFrameObj(af._dataverse, af._dataset, schema, query)

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #self.query is None, other.query is not None, left_on == right_on
    @patch.object(AFrame, 'get_dataset')
    def testJoin_NormalCase7(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        other.query = 'other_query'
        l_dataset = af._dataverse+'.'+af._dataset
        r_dataset = '(%s)' % other.query[:-1]
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'something'
        query = 'SELECT %s,%s '% (lsuffix, rsuffix) + 'from %s %s ' % (l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        expected = AFrameObj(af._dataverse, af._dataset, schema, query)

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #self.query is not None, other.query is not None, left_on == right_on
    @patch.object(AFrame, 'get_dataset')
    def testJoin_NormalCase8(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        af.query = 'af_query'
        other.query = 'other_query'
        l_dataset = '(%s)' % af.query[:-1]
        r_dataset = '(%s)' % other.query[:-1]
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'something'
        query = 'SELECT %s,%s '% (lsuffix, rsuffix) + 'from %s %s ' % (l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
        expected = AFrameObj(af._dataverse, af._dataset, schema, query)

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testGroupBy(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        by = 'by_something'
        expected = AFrameGroupBy(af._dataverse, af._dataset, by)
        actual = af.groupby(by)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._by, actual._by)

    #by is str, ascending is True
    @patch.object(AFrame, 'get_dataset')
    def testSortValues_1(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse+'.'+af._dataset
        by = 'by_something'
        new_query = 'SELECT VALUE t FROM %s t ' % dataset
        new_query += 'ORDER BY t.%s ;' % by
        schema = 'ORDER BY %s' % by
        ascending = True
        expected = AFrameObj(af._dataverse, af._dataset, schema, new_query)
        actual = af.sort_values(by, ascending)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #by is str, ascending is False
    @patch.object(AFrame, 'get_dataset')
    def testSortValues_2(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse+'.'+af._dataset
        by = 'by_something'
        new_query = 'SELECT VALUE t FROM %s t ' % dataset
        new_query += 'ORDER BY t.%s DESC;' % by
        schema = 'ORDER BY %s' % by
        ascending = False
        expected = AFrameObj(af._dataverse, af._dataset, schema, new_query)
        actual = af.sort_values(by, ascending)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #by is list, ascending is True
    @patch.object(AFrame, 'get_dataset')
    def testSortValues_3(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse+'.'+af._dataset
        new_query = 'SELECT VALUE t FROM %s t ' % dataset
        by = ['attri1', 'attri2', 'attri3']
        by_list = ''
        for i in range(len(by)):
            if i > 0:
                by_list += ', '
                by_list += 't.%s' % by[i]
        ascending = True
        new_query += 'ORDER BY %s;' % by_list
        schema = 'ORDER BY %s' % by
        expected = AFrameObj(af._dataverse, af._dataset, schema, new_query)
        actual = af.sort_values(by, ascending)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #by is list, ascending is False
    @patch.object(AFrame, 'get_dataset')
    def testSortValues_4(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse+'.'+af._dataset
        new_query = 'SELECT VALUE t FROM %s t ' % dataset
        by = ['attri1', 'attri2', 'attri3']
        by_list = ''
        for i in range(len(by)):
            if i > 0:
                by_list += ', '
                by_list += 't.%s' % by[i]
        ascending = False
        new_query += 'ORDER BY %s DESC;' % by_list
        schema = 'ORDER BY %s' % by
        expected = AFrameObj(af._dataverse, af._dataset, schema, new_query)
        actual = af.sort_values(by, ascending)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #by is ndarray, ascending is True
    @patch.object(AFrame, 'get_dataset')
    def testSortValues_5(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse+'.'+af._dataset
        new_query = 'SELECT VALUE t FROM %s t ' % dataset
        by = np.array([['attr1', 'attr2'],['attr3', 'attr4']])
        by_list = ''
        for i in range(len(by)):
            if i > 0:
                by_list += ', '
                by_list += 't.%s' % by[i]
        ascending = True
        new_query += 'ORDER BY %s;' % by_list
        schema = 'ORDER BY %s' % by
        expected = AFrameObj(af._dataverse, af._dataset, schema, new_query)
        actual = af.sort_values(by, ascending)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    #by is ndarray, ascending is False
    @patch.object(AFrame, 'get_dataset')
    def testSortValues_6(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse+'.'+af._dataset
        new_query = 'SELECT VALUE t FROM %s t ' % dataset
        by = np.array([['attr1', 'attr2'],['attr3', 'attr4']])
        by_list = ''
        for i in range(len(by)):
            if i > 0:
                by_list += ', '
                by_list += 't.%s' % by[i]
        ascending = False
        new_query += 'ORDER BY %s DESC;' % by_list
        schema = 'ORDER BY %s' % by
        expected = AFrameObj(af._dataverse, af._dataset, schema, new_query)
        actual = af.sort_values(by, ascending)
        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._schema, actual._schema)
        self.assertEqual(expected._query, actual._query)

    ###DESCRIBE NOT TESTED YET###
    '''
    @patch.object(AFrame, 'get_dataset')
    def testDescribe(self, mock_init):
        af_columns = [{'id': 'int64'},{'alias': 'string'},{'name': 'string'},{'userSince': 'datetime'},{'friendIds': 'GleambookUserType_friendIds'},{'employment': 'GleambookUserType_employment'}]
        af = AFrame('test_dataverse', 'test_dataset')
        af._columns = af_columns
        index = ['count', 'mean', 'std', 'min', 'max']
        columns = ['alias','name','id']
        data = []
        
        af.send_request = MagicMock(return_value =
                        [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}])
        num_cols = []
        str_cols = []
        numeric_types = ['int','int8','int16', 'int32', 'int64', 'double','integer', 'smallint', 'tinyint', 'bigint', 'float']
        index = ['count', 'mean', 'std', 'min', 'max']
        data = []
        dataset = af._dataverse + '.' + af._dataset
        fields = ''
        cols = af.columns
        for col in cols:
            if list(col.values())[0] in numeric_types:
                key = list(col.keys())[0]
                num_cols.append(key)
                fields += 'count(t.%s) %s_count, ' \
                    'min(t.%s) %s_min, ' \
                    'max(t.%s) %s_max, ' \
                    'avg(t.%s) %s_mean, ' % (key,key,key,key,key,key,key,key)
            if list(col.values())[0] == 'string':
                key = list(col.keys())[0]
                str_cols.append(key)
                fields += 'count(t.%s) %s_count, ' \
                          'min(t.%s) %s_min, ' \
                          'max(t.%s) %s_max, ' % (key, key, key, key, key, key)
        query = 'SELECT %s FROM %s AS t;' % (fields[:-2], dataset)
        print(query)
        stats = af.send_request(query)[0]
        std_query = 'SELECT '
        sqr_query = '(SELECT '
        for key in num_cols:
            attr_std = 'sqrt(avg(square.%s)) AS %s_std,' % (key, key)
            attr_sqr = 'power(%s - t.%s, 2) AS %s,' % (stats[key+'_mean'], key, key)
            sqr_query += attr_sqr
            std_query += attr_std
        std_query = std_query[:-1]
        sqr_query = sqr_query[:-1]
        std_query += ' FROM '
        std_query += sqr_query
        std_query += ' FROM %s t) square;' % dataset
        stds = af.send_request(std_query)[0]
        all_cols = str_cols+num_cols
        for ind in index:
            row_values = []
            if ind != 'std':
                for key in str_cols:
                    if key+'_'+ind in stats:    # check for existing key (cannot get avg() of string attributes )
                        value = stats[key+'_'+ind]  # e.g. stats[unique1_min]
                        row_values.append(value)
                    else:
                        row_values.append(None)
                for key in num_cols:
                    value = stats[key + '_' + ind]
                    row_values.append(value)
            else:
                for i in range(len(str_cols)):  # cannot get std() of string attributes
                    row_values.append(None)
                for key in num_cols:
                    value = stds[key + '_' + ind]
                    row_values.append(value)
            data.append(row_values)

        expected = pd.DataFrame(data, index=index, columns=all_cols)
        actual = af.describe()
        af.send_request.assert_called_with(query)'''
        

    #window is not None and not Window
    @patch.object(AFrame, 'get_dataset')
    def testRolling_ValueError1(self, mock_init):
        window = 'something'
        on = None
        af = AFrame('test_dataverse', 'test_dataset')
        with self.assertRaises(ValueError):
            af.rolling(window, on)

    #window is None and on is None
    @patch.object(AFrame, 'get_dataset')
    def testRolling_ValueError2(self, mock_init):
        window = None
        on = None
        af = AFrame('test_dataverse', 'test_dataset')
        with self.assertRaises(ValueError):
            af.rolling(window, on)

    #normal case
    @patch.object(AFrame, 'get_dataset')
    def testRolling_NormalCase(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        window = Window(None, None, None)
        on = None
        expected = OrderedAFrame(af._dataverse, af._dataset, af._columns, on, af.query, window)
        actual = af.rolling(window, on)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected._columns, actual._columns)
        self.assertEqual(expected.on, actual.on)
        self.assertEqual(expected.query, actual.query)
        self.assertEqual(expected._window, actual._window)

    ###send_request & send & send_perf: bad request (can't open 19002)###
    '''def testSendRequest(self):
        dataset = 'test_dataverse'+'.'+'test_dataset'
        query = 'SELECT VALUE t FROM %s t ' % dataset
        host = 'http://localhost:19002/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            result = json.loads(handler.read())
            expected = result['results']
        
        actual = AFrame.send_request(query)
        self.assertEqual(expected, actual)
    
    def testSend(self):
        dataset = 'test_dataverse'+'.'+'test_dataset'
        query = 'SELECT VALUE t FROM %s t ' % dataset
        host = 'http://localhost:19002/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            result = json.loads(handler.read())
            expected = result['status']

        actual = AFrame.send(query)
        self.assertEqual(expected, actual)

    def testSendPerf(self):
        dataset = 'test_dataverse'+'.'+'test_dataset'
        query = 'SELECT VALUE t FROM %s t ' % dataset
        host = 'http://localhost:19002/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            ret = handler.read()

        expected = ret
        actual = AFrame.send_perf(query)
        self.assertEqual(expected, actual)'''

    @patch.object(AFrame, 'get_dataset')
    def testDrop(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        AFrame.send = MagicMock(return_value = 'something')
        expected = 'something'

        actual = AFrame.drop(af)
        dataverse = af._dataverse
        dataset = af._dataset
        query = 'DROP DATASET %s.%s;' % (dataverse, dataset)
        AFrame.send.assert_called_once_with(query)
        self.assertEqual(expected, actual)
    
if __name__ == '__main__':
    unittest.main()


































