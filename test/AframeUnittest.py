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
    def testCollectQuery(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        query = af.collect_query()
        self.assertEqual(query, 'SELECT VALUE t FROM test_dataverse.test_dataset t;')

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

    #def testCreateTmpDataverse(self):

    def testPersistErrorCase(self):
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            aframe_obj.persist(None, None)

        aframe_obj_error = AFrameObj('test_dataverse', 'test_dataset', None, 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            aframe_obj_error.persist('id', None)

##    def testPersistNormal(self):
##        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
##        expected = 

            

    '''@patch.object(, 'get_dataset')
    def testAttachRowId(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')'''
        
        
    '''@patch('aframe.AFrame.get_dataset')
    def testSimpleMap(self, mock_init):
        json_response = [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                        {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                        {"attr1":5, "attr2":"str5"}, {"attr1":6, "attr2":"str6"},
                        {"attr1":7, "attr2":"str7"}, {"attr1":8, "attr2":"str8"},
                        {"attr1":9, "attr2":"str9"}, {"attr1":10, "attr2":"str10"}]
        
        af = AFrame('test_dataverse', 'test_dataset')
        
        af.send_request = MagicMock(return_value = json_response)

        actual1 = af['name']
        af.send_request.assert_called_once_with('SELECT VALUE t.name FROM test_dataverse.test_dataset t;')
        
        acrual2 = actual1.map('length')
        

        af.send_request.assert_called_once_with('SELECT VALUE length(t.name) FROM test_dataverse.test_dataset t;')
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
        row0 = pd.Series([10, 'str10'], index=['attr1', 'attr2'])
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
    def testSimpleCollect(self, mock_init):
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value = json_response)

        actual = af.c'''
        
        
if __name__ == '__main__':
    unittest.main()

