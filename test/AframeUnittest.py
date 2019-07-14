import unittest
from unittest.mock import MagicMock
from aframe.aframe import AFrame, AFrameObj
from unittest.mock import patch
import pandas as pd

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
    def testGetCount(self, mock_init):
        expected = 10
        af = AFrame('test_dataverse', 'test_dataset')
        
        af.send_request = MagicMock(return_value = [expected])
        actual_count = af.get_count()
        af.send_request.assert_called_once_with('SELECT VALUE count(*) FROM test_dataverse.test_dataset;')
        self.assertEqual(expected, actual_count)

    @patch.object(AFrame, 'get_dataset')
    def testToPandas(self, mock_init):
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
    def testUnnest(self, mock_init):
        new_query = 'SELECT VALUE get_object_fields(t) FROM test_dataverse.test_dataset t;'
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'get_object_fields(t)', new_query)
        
        af = AFrame('test_dataverse', 'test_dataset')
        with self.assertRaises(ValueError):
            af.unnest(1)
            af.unnest(aframe_obj, True, None)

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
        expected2 = AFrameObj('test_dataverse', 'test_dataset', None, new_query)
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

    def testAdd(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id + 3', 'SELECT VALUE t.id + 3 FROM test_dataverse.test_dataset t;')
        
        with self.assertRaises(ValueError):
            aframe_obj.add(None)
        
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

    def testMul(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        expected = AFrameObj('test_dataverse', 'test_dataset', 'id * 3', 'SELECT VALUE t.id * 3 FROM test_dataverse.test_dataset t;')

        with self.assertRaises(ValueError):
            aframe_obj.mul(None)

        aframe_obj.arithmetic_op = MagicMock(return_value = expected)
        actual = aframe_obj.mul(3)

        self.assertEqual(expected._dataverse, actual._dataverse)
        self.assertEqual(expected._dataset, actual._dataset)
        self.assertEqual(expected.schema, actual.schema)
        self.assertEqual(expected.query, actual.query)

    @patch.object(AFrame, 'get_dataset')
    def testGetColumnCount(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            af.get_column_count(None)
            
        expected = 5
        AFrame.send_request = MagicMock(return_value = [expected])
        actual = af.get_column_count(aframe_obj)
        af.send_request.assert_called_once_with('SELECT VALUE count(*) FROM (%s) t;' % aframe_obj.query[:-1])

    @patch.object(AFrame, 'get_dataset')
    def testWithColumnErrorCase(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', None, None)
        with self.assertRaises(ValueError):
            af.withColumn(None, aframe_obj)
        with self.assertRaises(ValueError):
            af.withColumn('id', None)

    @patch.object(AFrame, 'get_dataset')
    def testWithColumnNormalCase(self, mock_init):
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

    def testPersistErrorCase(self):
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
        
    
if __name__ == '__main__':
    unittest.main()


































