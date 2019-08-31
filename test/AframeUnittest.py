import unittest
from unittest.mock import MagicMock, Mock, call
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
import pandas.io.json as json

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
    def testSimpleHead_LT5(self, mock_init):
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}]
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value = json_response)

        actual = af.head()

        af.send_request.assert_called_once_with('SELECT VALUE t FROM test_dataverse.test_dataset t limit 5;')
        self.assertEqual(len(actual), 3)
        row0 = pd.Series([1, 'str1'], index=['attr1', 'attr2'])
        row1 = pd.Series([2, 'str2'], index=['attr1', 'attr2'])
        row2 = pd.Series([3, 'str3'], index=['attr1', 'attr2'])
        self.assertTrue(actual.iloc[0].equals(row0))
        self.assertTrue(actual.iloc[1].equals(row1))
        self.assertTrue(actual.iloc[2].equals(row2))

    @patch.object(AFrame, 'get_dataset')
    def testHead_RecordsLTDefault(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        

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
        af = AFrame('test_dataverse', 'test_dataset')
        
        af.send_request = MagicMock(return_value = [10])
        actual_count = af.get_count()
        af.send_request.assert_called_once_with('SELECT VALUE count(*) FROM test_dataverse.test_dataset;')
        self.assertEqual(10, actual_count)

    @patch.object(AFrame, 'get_dataset')
    def testToPandas_Exception(self, mock_init):
        from pandas.io import json
        af = AFrame('test_dataverse', None)
        with self.assertRaises(ValueError):
            af.toPandas()

    @patch.object(AFrame, 'get_dataset')
    def testToPandas_SampleEqualZero(self, mock_init):
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
    def testToPandas_SampleGTZeroQueryNone(self, mock_init):
        from pandas.io import json
        
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value = json_response)

        sample = 5
        dataset = af._dataverse+'.'+af._dataset
        query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
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

    @patch.object(AFrame, 'get_dataset')
    def testToPandas_TotalRecordsLTSample(self, mock_init):
        from pandas.io import json
        
        json_response =  [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}]
        
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value = json_response)

        sample = 5
        dataset = af._dataverse+'.'+af._dataset
        query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        actual = af.toPandas(sample)

        af.send_request.assert_called_once_with(query)
        self.assertEqual(len(actual), 3)
        data = json.read_json(json.dumps(actual))
        df = pd.DataFrame(data)
        row0 = pd.Series([1, 'str1'], index=['attr1', 'attr2'])
        row1 = pd.Series([2, 'str2'], index=['attr1', 'attr2'])
        row2 = pd.Series([3, 'str3'], index=['attr1', 'attr2'])
        self.assertTrue(df.loc[0].equals(row0))
        self.assertTrue(df.loc[1].equals(row1))
        self.assertTrue(df.loc[2].equals(row2))

    @patch.object(AFrame, 'get_dataset')
    def testToPandas_SampleGTZeroQueryNotNone(self, mock_init):
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
        self.assertTrue(df.loc[4].equals(row4))

    @patch.object(AFrame, 'get_dataset')
    def testCollectQuery_DatasetNotNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        query = af.collect_query()
        self.assertEqual(query, 'SELECT VALUE t FROM test_dataverse.test_dataset t;')


    @patch.object(AFrame, 'get_dataset')
    def testCollectQuery_DatasetNone(self, mock_init):
        af1 = AFrame('test_dataverse', None)
        self.assertRaises(ValueError, af1.collect_query)
        
    @patch.object(AFrame, 'get_dataset')
    def testApply_FuncNotStr(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        func = 1
        with self.assertRaises(TypeError):
            af.apply(func)

    @patch.object(AFrame, 'get_dataset')
    def testApply_QueryNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        actual = af.apply('get_object_fields')
        
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('get_object_fields(t)', actual.schema)
        self.assertEqual('SELECT VALUE get_object_fields(t) FROM test_dataverse.test_dataset t;', actual.query)

    @patch.object(AFrame, 'get_dataset')
    def testApply_QueryNotNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        af.query = 'SELECT VALUE t FROM test_dataverse.test_dataset t;'
        
        actual = af.apply('get_object_fields') 
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('get_object_fields(t)', actual.schema)
        self.assertEqual('SELECT VALUE get_object_fields(t) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t) t;', actual.query)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testUnnest_ColIsAFrameObjAppendedIsTrueNotName(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse + '.' + af._dataset
        name = None
        col = AFrameObj('another_dataverse', 'another_dataset', 'AFrameObj_schema', 'AFrameObj_query;')
        with self.assertRaises(ValueError):
            af.unnest(col, None, True, None)

    @patch.object(AFrame, 'get_dataset')
    def testUnnest_ColIsStrMetaIsNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')

        actual = af.unnest('something')
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('unnest(t.something)', actual._schema)
        self.assertEqual('SELECT VALUE something FROM test_dataverse.test_dataset t unnest t.something something;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testUnnest_ColIsStrMetaIsList(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        
        actual = af.unnest('something', ['attr1', 'attr2', 'attr3'])
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('t.attr1, t.attr2, t.attr3, t.something', actual._schema)
        self.assertEqual('SELECT t.attr1, t.attr2, t.attr3, something FROM test_dataverse.test_dataset t unnest t.something something;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testUnnest_ColIsStrMetaIsNdarray(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')

        actual = af.unnest('something', np.array(['attr1','attr2']))
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('t.attr1, t.attr2, t.something', actual._schema)
        self.assertEqual('SELECT t.attr1, t.attr2, something FROM test_dataverse.test_dataset t unnest t.something something;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testUnnest_ColIsADrameObjAppendedIsFalse(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        col = AFrameObj('another_dataverse', 'another_dataset', 'AFrameObj_schema', 'SELECT VALUE t FROM another_dataverse.another_dataset t;')

        actual = af.unnest(col, None, False, None)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('unnest(AFrameObj_schema)', actual._schema)
        self.assertEqual('SELECT VALUE e FROM (SELECT VALUE t FROM another_dataverse.another_dataset t) t unnest t e;', actual._query) ###wrong query (syntax error) (need help ....)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testUnnest_ColIsAFrameObjAppendedIsTrue(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        col = AFrameObj('another_dataverse', 'another_dataset', 'AFrameObj_schema', 'AFrameObj_query;')

        actual = af.unnest(col, None, True, 'name')
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('AFrameObj_schema', actual._schema)
        self.assertEqual('SELECT u name, t.* FROM test_dataverse.test_dataset t unnest t.AFrameObj_schema u;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testToAFrameObj_QueryIsNone(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset') 
        expected = AFrameObj('test_dataverse', 'test_dataset', None, None)
        actual = af.toAFrameObj()
        self.assertEqual(actual, None)

    @patch.object(AFrame, 'get_dataset')
    def testToAFrameObj_QueryIsNotNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        af.query = 'SELECT VALUE get_object_fields(t) FROM test_dataverse.test_dataset t;'
        actual = af.toAFrameObj()
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(None, actual.schema)
        self.assertEqual('SELECT VALUE get_object_fields(t) FROM test_dataverse.test_dataset t;', actual.query) ###semicolon missed

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testGetColumnCount_OtherIsNotAFrameObj(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        with self.assertRaises(ValueError):
            af.get_column_count(None)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testGetColumnCount_OtherIsAFrameObj(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        
        AFrame.send_request = MagicMock(return_value = [5])
        actual = af.get_column_count(aframe_obj)
        af.send_request.assert_called_once_with('SELECT VALUE count(*) FROM (SELECT VALUE t.id FROM test_dataverse.test_dataset t) t;')
        self.assertEqual(5, actual)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testWithColumn_NameIsNotStr(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', None, None)
        with self.assertRaises(ValueError):
            af.withColumn(None, aframe_obj)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testWithColumn_ColIsNotAFrameObj(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', None, None)
        with self.assertRaises(ValueError):
            af.withColumn('id', None)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testWithColumn_NameIsStrColIsAFrameObj(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')

        actual = af.withColumn('id', aframe_obj)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id', actual.schema)
        self.assertEqual('SELECT t.*, id id FROM test_dataverse.test_dataset t;', actual.query)

    @patch.object(AFrame, 'get_dataset')
    def testDrop(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        AFrame.send = MagicMock(return_value = [])
        
        actual = AFrame.drop(af)
        AFrame.send.assert_called_once_with('DROP DATASET test_dataverse.test_dataset;')
        self.assertEqual([], actual)

    @patch.object(AFrame, 'get_dataset')
    def testStr_ColumnsIsNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        actual = str(af)
        self.assertEqual('Empty AsterixDB DataFrame', actual)

    @patch.object(AFrame, 'get_dataset')
    def testStr_ColumnsIsNotNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset', columns = ['name','age','GPA'])
        actual = str(af)
        self.assertEqual('AsterixDB DataFrame with the following pre-defined columns: \n\t'+'[\'name\', \'age\', \'GPA\']', actual)

    @patch.object(AFrame, 'get_dataset')
    def testRepr(self, mock_init):
        af = AFrame('test_dataverse','test_dataset')
        af.__str__ = MagicMock(return_value = 'Empty AsterixDB DataFrame')
        actual = repr(af)
        af.__str__.assert_called_once()
        self.assertEqual('Empty AsterixDB DataFrame', actual)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testGetItem_KeyIsAFrameObjQueryIsNone(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 't.id>0')
        actual = af[aframe_obj]

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('t.id>0', actual._schema)
        self.assertEqual('SELECT VALUE t FROM test_dataverse.test_dataset t WHERE t.id>0;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testGetItem_KeyIsAFrameObjQueryIsNotNone(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        af.query = 'af_query;'
        key = AFrameObj('test_dataverse', 'test_dataset', 't.id>0')
        dataset = af._dataverse + '.' + af._dataset
        new_query = 'SELECT VALUE t FROM (%s) t WHERE %s;' % (dataset, af.query[:-1], key.schema) ###string format wrong
        expected = AFrameObj(af._dataverse, af._dataset, key.schema, new_query)
        actual = af[key]

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('t.id>0', actual._schema)
        self.assertEqual(expected._query, actual._query)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.aframeObj.AFrameObj')
    def testGetItem_KeyIsStrQueryIsNone(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')

        expected = AFrameObj('test_dataverse', 'test_dataset', 'id', 'SELECT VALUE t.id FROM test_dataverse.test_dataset t;')
        actual = af['id']
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id', actual._schema)
        self.assertEqual('SELECT VALUE t.id FROM test_dataverse.test_dataset t;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testGetItem_KeyIsStrQueryIsNotNone(self, mock_init):
        af_query = 'SELECT VALUE t.* FROM %s t;' % ('test_dataverse.test_dataset')
        af = AFrame('test_dataverse', 'test_dataset')
        af.query = af_query
        
        actual = af['id']
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('id', actual._schema)
        self.assertEqual('SELECT VALUE t.id FROM (SELECT VALUE t FROM test_dataverse.test_dataset t) t;', actual._query) ##original: t.* => * cannot be recognized

    @patch.object(AFrame, 'get_dataset')
    def testGetItem_KeyIsListQueryIsNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')

        actual = af[['attr1','attr2','attr3']]
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(['attr1','attr2','attr3'], actual._schema)
        self.assertEqual('SELECT t.attr1, t.attr2, t.attr3 FROM test_dataverse.test_dataset t;', actual._query) 

    @patch.object(AFrame, 'get_dataset')
    def testGetItem_KeyIsListQueryIsNotNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        af.query = 'SELECT t.* FROM %s t;' % 'test_dataverse.test_dataset'

        actual = af[['attr1','attr2','attr3']]
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(['attr1','attr2','attr3'], actual._schema)
        self.assertEqual('SELECT t.attr1, t.attr2, t.attr3 FROM (SELECT t.* FROM test_dataverse.test_dataset t) t;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testGetItem_KeyIsNdarrayQueryIsNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')

        actual = af[np.array(['attr1','attr2'])]
        print(type(actual._schema))
        print(actual._schema == ['attr1', 'attr2'])
        print(np.array(['attr1','attr2']))
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(['attr1' 'attr2'], actual._schema) ####Error in this line => cannot proceed ...
        self.assertEqual('SELECT t.attr1, t.attr2 FROM test_dataverse.test_dataset t;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testSetItem_KeyIsNotStr(self, mock_init):
        af = AFrame('test_dataverse','test_dataset')
        columns = 'id'
        on = None
        query = None
        value = OrderedAFrame('test_dataverse','test_dataset', columns, on, query)
        with self.assertRaises(ValueError):
            af[1] = value

    @patch.object(AFrame, 'get_dataset')
    def testSetItem_ValueIsOrderedAFrame(self, mock_init):
        af = AFrame('test_dataverse','test_dataset')
        value = OrderedAFrame('test_dataverse','test_dataset', 'id', None, None)
        af['id'] = value
        self.assertEqual('SELECT t.*, id id FROM test_dataverse.test_dataset t;', af.query)

    @patch.object(AFrame, 'get_dataset')
    def testFlatten(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset', query = 'test_query') 
        actual = af.flatten()

        self.assertEqual(type(actual), NestedAFrame)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(af.columns, actual.columns)
        self.assertEqual('test_query', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testColumns(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset', ['name', 'id', 'GPA'])

        actual = af.columns
        self.assertEqual(['name', 'id', 'GPA'], actual)

    @patch.object(AFrame, 'get_dataset')
    def testCollectQuery_DatasetIsNone(self, mock_init):
        af = AFrame('test_dataverse', None)
        with self.assertRaises(ValueError):
            af.collect_query()

    @patch.object(AFrame, 'get_dataset')
    def testCollectQuery_DatasetIsNotNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')

        actual = af.collect_query()
        self.assertEqual('SELECT VALUE t FROM test_dataverse.test_dataset t;', actual)

    def test_attach_row_id_EmtyResultLst(self):
        actual = AFrame.attach_row_id([])
        self.assertEqual([], actual)

    def test_attach_row_id_NotEmptyResultLst(self):
        result_lst = [{'data': {'row_id': 1}, 'row_id': 2},
                      {'data': {'row_id': 3}, 'row_id': 4},
                      {'data': {'row_id': 5}, 'row_id': 6}]

        actual = AFrame.attach_row_id(result_lst)
        self.assertEqual([{'row_id': 2}, {'row_id': 4}, {'row_id': 6}], actual)

    @patch.object(AFrame, 'get_dataset')
    def testNotna(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        notna = MagicMock(side_effect=NotImplementedError)
        with self.assertRaises(NotImplementedError):
            af.notna()
            
    @patch.object(AFrame, 'get_dataset')
    def testInitColumns(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        columns = None
        with self.assertRaises(ValueError):
            af.init_columns(columns)

    @patch.object(AFrame, 'get_dataset')
    def testJoin_LeftonIsNoneRightonIsNotNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        left_on = None
        right_on = 'something'
        with self.assertRaises(ValueError):
            af.join(other, left_on, right_on)

    @patch.object(AFrame, 'get_dataset')
    def testJoin_LeftonIsNotNoneRightonIsNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        left_on = 'something'
        right_on = None
        with self.assertRaises(ValueError):
            af.join(other, left_on, right_on)

    @patch.object(AFrame, 'get_dataset')
    def testJoin_LeftonRightonBothNone(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        left_on = None
        right_on = None
        with self.assertRaises(ValueError):
            af.join(other, left_on, right_on)
            
    @patch.object(AFrame, 'get_dataset')
    def testJoin_LeftonRightonBothNotNoneHowNotInJoinTypes(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        left_on = 'something'
        right_on = 'something'
        how = 'right'
        with self.assertRaises(NotImplementedError):
            af.join(other, left_on, right_on, how)
            
    @patch.object(AFrame, 'get_dataset')
    def testJoin_SelfQueryIsNoneOtherQueryIsNoneLeftonNotEqualRightOn(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'anotherthing'

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('test_dataverse.test_dataset l JOIN other_dataverse.other_dataset r on l.something=r.anotherthing', actual._schema)
        self.assertEqual('SELECT VALUE object_merge(l,r) FROM test_dataverse.test_dataset l JOIN other_dataverse.other_dataset r on l.something /*+ indexnl */ = r.anotherthing;', actual._query) 
    @patch.object(AFrame, 'get_dataset')
    def testJoin_SelfQueryIsNotNoneOtherQueryInNoneLeftonNotEqualRighton(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        af.query = 'SELECT VALUE t FROM test_dataverse.test_dataset t;'
        other = AFrame('other_dataverse', 'other_dataset')
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'anotherthing'

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('(SELECT VALUE t FROM test_dataverse.test_dataset t) l JOIN other_dataverse.other_dataset r on l.something=r.anotherthing', actual._schema)
        self.assertEqual('SELECT VALUE object_merge(l,r) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t) l JOIN other_dataverse.other_dataset r on l.something /*+ indexnl */ = r.anotherthing;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testJoin_SelfQueryIsNoneOtherQueryIsNotNoneLeftonNotEqualRighton(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        other.query = 'SELECT VALUE t FROM test_dataverse.test_dataset t;'
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'anotherthing'

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('test_dataverse.test_dataset l JOIN (SELECT VALUE t FROM test_dataverse.test_dataset t) r on l.something=r.anotherthing', actual._schema)
        self.assertEqual('SELECT VALUE object_merge(l,r) FROM test_dataverse.test_dataset l JOIN (SELECT VALUE t FROM test_dataverse.test_dataset t) r on l.something /*+ indexnl */ = r.anotherthing;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testJoin_SelfQueryIsNotNoneOtherQueryIsNotNoneLeftOnNotEqualRighton(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        af.query = 'SELECT VALUE t FROM test_dataverse.test_dataset t;'
        other.query = 'SELECT VALUE t FROM test_dataverse.test_dataset t;'
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'anotherthing'

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('(SELECT VALUE t FROM test_dataverse.test_dataset t) l JOIN (SELECT VALUE t FROM test_dataverse.test_dataset t) r on l.something=r.anotherthing', actual._schema)
        self.assertEqual('SELECT VALUE object_merge(l,r) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t) l JOIN (SELECT VALUE t FROM test_dataverse.test_dataset t) r on l.something /*+ indexnl */ = r.anotherthing;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testJoin_SelfQueryIsNoneOtherQueryIsNoneLeftonEqualRighton(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'something'

        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('test_dataverse.test_dataset l JOIN other_dataverse.other_dataset r on l.something=r.something', actual._schema)
        self.assertEqual('SELECT l,r from test_dataverse.test_dataset l JOIN other_dataverse.other_dataset r on l.something /*+ indexnl */ = r.something;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testJoin_SelfQueryIsNotNoneOtherQueryIsNoneLeftonEqualRighton(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        af.query = 'SELECT VALUE t FROM test_dataverse.test_dataset t;'
        other = AFrame('other_dataverse', 'other_dataset')
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'something'
        
        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('(SELECT VALUE t FROM test_dataverse.test_dataset t) l JOIN other_dataverse.other_dataset r on l.something=r.something', actual._schema)
        self.assertEqual('SELECT l,r from (SELECT VALUE t FROM test_dataverse.test_dataset t) l JOIN other_dataverse.other_dataset r on l.something /*+ indexnl */ = r.something;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testJoin_SelfQueryIsNoneOtherQueryIsNotNoneLeftonEqualRighton(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        other.query = 'SELECT VALUE t FROM test_dataverse.test_dataset t;'
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'something'
        
        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('test_dataverse.test_dataset l JOIN (SELECT VALUE t FROM test_dataverse.test_dataset t) r on l.something=r.something', actual._schema)
        self.assertEqual('SELECT l,r from test_dataverse.test_dataset l JOIN (SELECT VALUE t FROM test_dataverse.test_dataset t) r on l.something /*+ indexnl */ = r.something;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testJoin_SelfQueryIsNotNoneOtherQueryIsNotNoneLeftonEqualRighton(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        other = AFrame('other_dataverse', 'other_dataset')
        af.query = 'SELECT VALUE t FROM test_dataverse.test_dataset t;'
        other.query = 'SELECT VALUE t FROM test_dataverse.test_dataset t;'
        how = 'inner'
        lsuffix = 'l'
        rsuffix = 'r'
        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        left_on = 'something'
        right_on = 'something'
        
        actual = af.join(other, left_on, right_on, how, lsuffix, rsuffix)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('(SELECT VALUE t FROM test_dataverse.test_dataset t) l JOIN (SELECT VALUE t FROM test_dataverse.test_dataset t) r on l.something=r.something', actual._schema)
        self.assertEqual('SELECT l,r from (SELECT VALUE t FROM test_dataverse.test_dataset t) l JOIN (SELECT VALUE t FROM test_dataverse.test_dataset t) r on l.something /*+ indexnl */ = r.something;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.groupby.AFrameGroupBy')
    def testGroupBy(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        actual = af.groupby('by_something')

        self.assertIsInstance(actual, AFrameGroupBy)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('by_something', actual._by)

    @patch.object(AFrame, 'get_dataset')
    def testSortValues_ByisStrAscendingIsTrue(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        
        actual = af.sort_values('by_something', True)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('ORDER BY by_something', actual._schema)
        self.assertEqual('SELECT VALUE t FROM test_dataverse.test_dataset t ORDER BY t.by_something ;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testSortValues_ByIsStrAscendingIsFalse(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse+'.'+af._dataset
        
        actual = af.sort_values('by_something', False)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('ORDER BY by_something', actual._schema)
        self.assertEqual('SELECT VALUE t FROM test_dataverse.test_dataset t ORDER BY t.by_something DESC;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testSortValues_ByIsListAscendingIsTrue(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        dataset = af._dataverse+'.'+af._dataset
        new_query = 'SELECT VALUE t FROM %s t ' % dataset
        
        actual = af.sort_values(['attri1', 'attri2', 'attri3'], True)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('ORDER BY [\'attri1\', \'attri2\', \'attri3\']', actual._schema)
        self.assertEqual('SELECT VALUE t FROM test_dataverse.test_dataset t ORDER BY t.attri2, t.attri3;', actual._query)

    @patch.object(AFrame, 'get_dataset')
    def testSortValues_ByIsListAscendingIsFalse(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        
        actual = af.sort_values(['attri1', 'attri2', 'attri3'], False)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual('ORDER BY [\'attri1\', \'attri2\', \'attri3\']', actual._schema)
        self.assertEqual('SELECT VALUE t FROM test_dataverse.test_dataset t ORDER BY t.attri2, t.attri3 DESC;', actual._query) ####query

    @patch.object(AFrame, 'get_dataset')
    def testSortValues_ByIsNdarrayAscendingIsTrue(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        by = np.array(['attr2', 'attr3', 'attr4'])
        schema = 'ORDER BY %s' % by
        
        actual = af.sort_values(by, True)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE t FROM test_dataverse.test_dataset t ORDER BY t.attr3, t.attr4;', actual._query) ##query

    @patch.object(AFrame, 'get_dataset')
    def testSortValues_ByIsNdarrayAscendingIsFalse(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        by = np.array(['attr3', 'attr4'])
        schema = 'ORDER BY %s' % by

        actual = af.sort_values(by, False)
        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(schema, actual._schema)
        self.assertEqual('SELECT VALUE t FROM test_dataverse.test_dataset t ORDER BY t.attr4 DESC;', actual._query) ##query

    @patch.object(AFrame, 'get_dataset')
    def testRolling_WindowIsNotNoneNotWindow(self, mock_init):
        window = 'something'
        on = None
        af = AFrame('test_dataverse', 'test_dataset')
        with self.assertRaises(ValueError):
            af.rolling(window, on)

    @patch.object(AFrame, 'get_dataset')
    def testRolling_WindowIsNoneOnIsNone(self, mock_init):
        window = None
        on = None
        af = AFrame('test_dataverse', 'test_dataset')
        with self.assertRaises(ValueError):
            af.rolling(window, on)

    @patch.object(AFrame, 'get_dataset')
    @patch('aframe.window.Window')
    def testRolling_NormalCase(self, mock_init, mock_class):
        af = AFrame('test_dataverse', 'test_dataset')
        window = Window(None, None, None)
        on = None
        actual = af.rolling(window, on)

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(af._columns, actual._columns)
        self.assertEqual(None, actual.on)
        self.assertEqual(af.query, actual.query)
        self.assertEqual(window, actual._window)

    @patch.object(AFrame, 'get_dataset')
    def testDrop(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        AFrame.send = MagicMock(return_value = 'something')

        actual = AFrame.drop(af)
        AFrame.send.assert_called_once_with('DROP DATASET test_dataverse.test_dataset;')
        self.assertEqual('something', actual)

    def testGetDataset_IsOpenIsTrue(self):
        response = [{'Derived': {'Record': {'IsOpen': True,
                                            'Fields': [{'FieldName': 'test_fieldName1',
                                                       'FieldType': 'test_fieldType1',
                                                       'IsNullable': True},
                                                      {'FieldName': 'test_fieldName2',
                                                       'FieldType': 'test_fieldType2',
                                                       'IsNullable': True}]}},
                    'DatatypeName': 'test_datatypeName'}]
        AFrame.send_request = MagicMock(return_value=response)
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value = response)
        expected_columns = [{'test_fieldName1': 'test_fieldType1'}, {'test_fieldName2': 'test_fieldType2'}]

        AFrame.send_request.assert_called_with('SELECT VALUE dt FROM Metadata.`Dataset` ds, Metadata.`Datatype` dt WHERE ds.DatasetName = \'test_dataset\' AND ds.DatatypeName = dt.DatatypeName;')
        self.assertEqual(af._datatype, 'open')
        self.assertEqual(af._datatype_name, 'test_datatypeName')
        self.assertEqual(af._columns, expected_columns)

    def testGetDataset_IsOpenIsFalse(self):
        response = [{'Derived': {'Record': {'IsOpen': False,
                                            'Fields': [{'FieldName': 'test_fieldName1',
                                                       'FieldType': 'test_fieldType1',
                                                       'IsNullable': True},
                                                      {'FieldName': 'test_fieldName2',
                                                       'FieldType': 'test_fieldType2',
                                                       'IsNullable': True}]}},
                    'DatatypeName': 'test_datatypeName'}]
        AFrame.send_request = MagicMock(return_value=response)
        
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value = response)

        expected_columns = [{'test_fieldName1': 'test_fieldType1'}, {'test_fieldName2': 'test_fieldType2'}]

        AFrame.send_request.assert_called_with('SELECT VALUE dt FROM Metadata.`Dataset` ds, Metadata.`Datatype` dt WHERE ds.DatasetName = \'test_dataset\' AND ds.DatatypeName = dt.DatatypeName;')
        self.assertEqual(af._datatype, 'close')
        self.assertEqual(af._datatype_name, 'test_datatypeName')
        self.assertEqual(af._columns, expected_columns)
        
    @patch.object(AFrame, 'get_dataset')
    def testDescribe(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        af._columns = [{'col1': 'int'}, {'col2': 'string'}]
        mock_response = [{'col1_min':0, 'col1_max':2, 'col1_count':3, 'col1_avg':1, 'col1_mean':1, 'col1_std':1,'col2_min':'test_min', 'col2_max':'test_max', 'col2_count':'test_cnt', 'col2_avg':'test_avg', 'col2_mean':'test_mean', 'col2_std':'test_std'}]
        af.send_request = MagicMock(return_value=mock_response)
        
        actual_res =  af.describe()
        expected_res = pd.DataFrame([['test_cnt', 3], ['test_mean', 1], [None, 1], ['test_min', 0], ['test_max', 2]], ['count', 'mean', 'std', 'min', 'max'], ['col2', 'col1'])
        
        query = 'SELECT count(t.col1) col1_count, min(t.col1) col1_min, max(t.col1) col1_max, avg(t.col1) col1_mean, count(t.col2) col2_count, min(t.col2) col2_min, max(t.col2) col2_max FROM test_dataverse.test_dataset AS t;'
        std_query = 'SELECT sqrt(avg(square.col1)) AS col1_std FROM (SELECT power(1 - t.col1, 2) AS col1 FROM test_dataverse.test_dataset t) square;'
        calls = [call(query), call(std_query)]
        af.send_request.assert_has_calls(calls)
        self.assertTrue(actual_res.equals(expected_res))
    
if __name__ == '__main__':
    unittest.main()
