import unittest
from unittest.mock import MagicMock
from aframe.aframe import AFrame
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
    def testLen(self,  mock_init):
        expected = 7
        af = AFrame('test_dataverse', 'test_dataset')
        mock_init.assert_called_once_with('test_dataset')

        af.send_request = MagicMock(return_value=[expected])
        actual_len = len(af)
        af.send_request.assert_called_once_with('SELECT VALUE count(*) FROM test_dataverse.test_dataset;')
        self.assertEqual(expected, actual_len)

    @patch.object(AFrame, 'get_dataset')
    def testSimpleHead(self, mock_init):

        # Arrange
        json_response = [{"attr1":1, "attr2":"str1"}, {"attr1":2, "attr2":"str2"},
                         {"attr1":3, "attr2":"str3"}, {"attr1":4, "attr2":"str4"},
                         {"attr1": 5, "attr2": "str5"}]
        af = AFrame('test_dataverse', 'test_dataset')
        af.send_request = MagicMock(return_value=json_response)

        # Act
        actual = af.head()

        # Assert
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
    def testGetItem_NormalCase1(self, mock_init):
        af = AFrame('test_dataverse', 'test_dataset')
        # aframe_obj = AFrameObj('test_dataverse', 'test_dataset', 't.id>0')
        dataset = af._dataverse + '.' + af._dataset
        key = 'col1'
        query = 'SELECT VALUE t.%s FROM %s t;' % (key, dataset)
        # expected = AFrameObj('test_dataverse', 'test_dataset', aframe_obj.schema, new_query)
        # actual = af.__getitem__(aframe_obj)
        actual = af['col1']

        self.assertEqual('test_dataverse', actual._dataverse)
        self.assertEqual('test_dataset', actual._dataset)
        self.assertEqual(query, actual.query)

        # self.assertEqual(expected._dataverse, actual._dataverse)
        # self.assertEqual(expected._dataset, actual._dataset)
        # self.assertEqual(expected._schema, actual._schema)
        # self.assertEqual(expected._query, actual._query)


if __name__ == '__main__':
    unittest.main()