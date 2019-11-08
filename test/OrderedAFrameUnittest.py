import unittest
from unittest.mock import MagicMock, Mock, call,patch
from aframe.aframe import AFrame, AFrameObj, OrderedAFrame, Window

class TestOrderedAFrame(unittest.TestCase):
    @patch.object(AFrame, "__init__")
    def testSimpleInit(self, mock_init):
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", "test_query", None)
        mock_init.assert_called_once() #???????????? when assert_called_once_with("test_dataverse", "test_dataset") will fail
        self.assertEqual(orderedAF._columns, "test_columns")
        self.assertEqual(orderedAF._data, None)
        self.assertEqual(orderedAF._dataverse, "test_dataverse")
        self.assertEqual(orderedAF._dataset, "test_dataset")
        self.assertEqual(orderedAF.on, "test_on")
        self.assertEqual(orderedAF.query, "test_query")

    @patch.object(AFrame, "__init__")
    def testGetWindow_WindowIsNone(self, mock_init):
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", "test_query", None)
        mock_init.assert_called_once()
        expected = 'OVER(ORDER BY t.test_on )'
        actual = orderedAF.get_window()
        self.assertEqual(expected, actual)

    @patch('aframe.window.Window')
    @patch.object(AFrame, "__init__")
    def testGetWindow_WindowIsNotNone_PartIsNone_OrdIsNone_RowsIsNone(self, mock_init, mock_class):
        window = Window(None, None, None)
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", "test_query", window)
        mock_init.assert_called_once()
        expected = "OVER()"
        actual = orderedAF.get_window()
        self.assertEqual(actual, expected)

    @patch('aframe.window.Window')
    @patch.object(AFrame, "__init__")
    def testGetWindow_WindowIsNotNone_PartIsNotNone_OrdIsNone_RowsIsNone(self, mock_init, mock_class):
        window = Window("test_part", None, None)
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", "test_query", window)
        mock_init.assert_called_once()
        expected = "OVER(PARTITION BY t.test_part )"
        actual = orderedAF.get_window()
        self.assertEqual(actual, expected)

    @patch('aframe.window.Window')
    @patch.object(AFrame, "__init__")
    def testGetWindow_WindowIsNotNone_PartIsNone_OrdIsNotNone_RowsIsNone(self, mock_init, mock_class):
        window = Window(None, "test_ord", None)
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", "test_query", window)
        mock_init.assert_called_once()
        expected = "OVER(ORDER BY t.test_ord )"
        actual = orderedAF.get_window()
        self.assertEqual(actual, expected)

    @patch('aframe.window.Window')
    @patch.object(AFrame, "__init__")
    def testGetWindow_WindowIsNotNone_PartIsNone_OrdIsNone_RowsIsNotNone(self, mock_init, mock_class):
        window = Window(None, None, "test_rows")
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", "test_query", window)
        mock_init.assert_called_once()
        expected = "OVER(test_rows)"
        actual = orderedAF.get_window()
        self.assertEqual(actual, expected)

    @patch('aframe.window.Window')
    @patch.object(AFrame, "__init__")
    def testGetWindow_WindowIsNotNone_PartIsNotNone_OrdIsNotNone_RowsIsNotNone(self, mock_init, mock_class):
        window = Window("test_part", "test_ord", "test_rows")
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", "test_query", window)
        mock_init.assert_called_once()
        expected = "OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)"
        actual = orderedAF.get_window()
        self.assertEqual(actual, expected)

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    def testValidateAggFunc_QueryIsNoneOnIsNoneWindowIsNone(self, mock_method, mock_init):
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", None, None, None)
        mock_init.assert_called_once()
        mock_over = 'OVER(ORDER BY t.test_on)'
        orderedAF.get_window = MagicMock(return_value=mock_over)
        with self.assertRaises(ValueError):
            orderedAF.validate_agg_func("test_func", None)
            mock_method.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    def testValidateAggFunc_QueryIsNotNoneOnIsNoneWindowIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", None, test_query, None)
        mock_init.assert_called_once()
        mock_over = 'OVER(ORDER BY t.test_on)'
        orderedAF.get_window = MagicMock(return_value=mock_over)
        with self.assertRaises(ValueError):
            orderedAF.validate_agg_func("test_func", None)
            mock_method.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    def testValidateAggFunc_QueryIsNotNoneOnIsNotNoneWindowIsNoneArgIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_over = 'OVER(ORDER BY t.test_on)'
        orderedAF.get_window = MagicMock(return_value=mock_over)

        actual = orderedAF.validate_agg_func("test_func", None)
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "test_func(t.test_on) OVER(ORDER BY t.test_on)")
        self.assertEqual(actual.query, "SELECT VALUE test_func(t.test_on) OVER(ORDER BY t.test_on) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")
        self.assertEqual(2, mock_init.call_count)
        orderedAF.get_window.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    def testValidateAggFunc_QueryIsNotNoneOnIsNotNoneWindowIsNoneArgIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_over = 'OVER(ORDER BY t.test_on)'
        orderedAF.get_window = MagicMock(return_value=mock_over)

        actual = orderedAF.validate_agg_func("test_func", "test_arg")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "test_func(t.test_arg) OVER(ORDER BY t.test_on)")
        self.assertEqual(actual.query,
                         "SELECT VALUE test_func(t.test_arg) OVER(ORDER BY t.test_on) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")
        self.assertEqual(2, mock_init.call_count)
        orderedAF.get_window.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    @patch('aframe.window.Window')
    def testValidateAggFunc_QueryIsNotNoneOnIsNoneWindowIsNotNoneArgIsNone(self, mock_class, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        window = Window("test_part", "test_ord", "test_rows")
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", None, test_query, window)
        mock_over = 'OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)'
        orderedAF.get_window = MagicMock(return_value=mock_over)

        actual = orderedAF.validate_agg_func("test_func", None)
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "test_func(t.test_ord) OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)")
        self.assertEqual(actual.query,
                         "SELECT VALUE test_func(t.test_ord) OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")
        self.assertEqual(2, mock_init.call_count)
        orderedAF.get_window.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    @patch('aframe.window.Window')
    def testValidateAggFunc_QueryIsNotNoneOnIsNoneWindowIsNotNoneArgIsNotNone(self, mock_class, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        window = Window("test_part", "test_ord", "test_rows")
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", None, test_query, window)
        mock_over = 'OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)'
        orderedAF.get_window = MagicMock(return_value=mock_over)

        actual = orderedAF.validate_agg_func("test_func", "test_arg")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "test_func(t.test_arg) OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)")
        self.assertEqual(actual.query,
                         "SELECT VALUE test_func(t.test_arg) OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")
        self.assertEqual(2, mock_init.call_count)
        orderedAF.get_window.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    @patch('aframe.window.Window')
    def testValidateAggFunc_QueryIsNotNoneOnIsNoneWindowIsNotNoneArgIsNotNone(self, mock_class, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        window = Window("test_part", "test_ord", "test_rows")
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", None, test_query, window)
        mock_over = 'OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)'
        orderedAF.get_window = MagicMock(return_value=mock_over)

        actual = orderedAF.validate_agg_func("test_func", "test_arg")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns,
                         "test_func(t.test_arg) OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)")
        self.assertEqual(actual.query,
                         "SELECT VALUE test_func(t.test_arg) OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")
        self.assertEqual(2, mock_init.call_count)
        orderedAF.get_window.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    @patch('aframe.window.Window')
    def testValidateAggFunc_QueryIsNoneOnIsNotNoneWindowIsNoneArgIsNone(self, mock_class, mock_method, mock_init):
        #test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        #window = Window("test_part", "test_ord", "test_rows")
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", None, None)
        mock_over = 'OVER(ORDER BY t.test_on)'
        orderedAF.get_window = MagicMock(return_value=mock_over)

        actual = orderedAF.validate_agg_func("test_func", None)
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns,
                         "test_func(t.test_on) OVER(ORDER BY t.test_on)")
        self.assertEqual(actual.query,
                         "SELECT VALUE test_func(t.test_on) OVER(ORDER BY t.test_on) FROM test_dataverse.test_dataset t;")
        self.assertEqual(2, mock_init.call_count)
        orderedAF.get_window.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    @patch('aframe.window.Window')
    def testValidateAggFunc_QueryIsNoneOnIsNotNoneWindowIsNoneArgIsNotNone(self, mock_class, mock_method, mock_init):
        # test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        # window = Window("test_part", "test_ord", "test_rows")
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", None, None)
        mock_over = 'OVER(ORDER BY t.test_on)'
        orderedAF.get_window = MagicMock(return_value=mock_over)

        actual = orderedAF.validate_agg_func("test_func", "test_arg")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns,
                         "test_func(t.test_arg) OVER(ORDER BY t.test_on)")
        self.assertEqual(actual.query,
                         "SELECT VALUE test_func(t.test_arg) OVER(ORDER BY t.test_on) FROM test_dataverse.test_dataset t;")
        self.assertEqual(2, mock_init.call_count)
        orderedAF.get_window.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    @patch('aframe.window.Window')
    def testValidateAggFunc_QueryIsNoneOnIsNoneWindowIsNotNoneArgIsNone(self, mock_class, mock_method, mock_init):
        #test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        window = Window("test_part", "test_ord", "test_rows")
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", None, None, window)
        mock_over = 'OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)'
        orderedAF.get_window = MagicMock(return_value=mock_over)

        actual = orderedAF.validate_agg_func("test_func", None)
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns,
                         "test_func(t.test_ord) OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)")
        self.assertEqual(actual.query,
                         "SELECT VALUE test_func(t.test_ord) OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows) FROM test_dataverse.test_dataset t;")
        self.assertEqual(2, mock_init.call_count)
        orderedAF.get_window.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "get_window")
    @patch('aframe.window.Window')
    def testValidateAggFunc_QueryIsNoneOnIsNoneWindowIsNotNoneArgIsNotNone(self, mock_class, mock_method, mock_init):
        #test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        window = Window("test_part", "test_ord", "test_rows")
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", None, None, window)
        mock_over = 'OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)'
        orderedAF.get_window = MagicMock(return_value=mock_over)

        actual = orderedAF.validate_agg_func("test_func", "test_arg")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns,
                         "test_func(t.test_arg) OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows)")
        self.assertEqual(actual.query,
                         "SELECT VALUE test_func(t.test_arg) OVER(PARTITION BY t.test_part ORDER BY t.test_ord test_rows) FROM test_dataverse.test_dataset t;")
        self.assertEqual(2, mock_init.call_count)
        orderedAF.get_window.assert_called_once()

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testSum_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "SUM(t.test_columns) OVER(ORDER BY t.test_on )", "test_on", "SELECT VALUE SUM(t.test_columns) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;", None)
        orderedAF.validate_agg_func = MagicMock(return_value = mock_oaf)
        actual = orderedAF.sum("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "SUM(t.test_columns) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE SUM(t.test_columns) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testSum_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "SUM(t.test_on) OVER(ORDER BY t.test_on )", "test_on", "SELECT VALUE SUM(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;", None)
        orderedAF.validate_agg_func = MagicMock(return_value = mock_oaf)
        actual = orderedAF.sum(None)
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "SUM(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE SUM(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testCount_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "COUNT(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE COUNT(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.count("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "COUNT(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE COUNT(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testCount_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "COUNT(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE COUNT(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.count("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "COUNT(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE COUNT(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testAvg_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "AVG(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE AVG(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.avg("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "AVG(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE AVG(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testAVG_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "AVG(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE AVG(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.avg("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "AVG(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE AVG(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testMean_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "AVG(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE AVG(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.mean("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "AVG(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE AVG(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testMean_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "AVG(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE AVG(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.mean("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "AVG(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE AVG(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testMin_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "MIN(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE MIN(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.min("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "MIN(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE MIN(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testMin_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "MIN(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE MIN(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.min("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "MIN(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE MIN(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testMax_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "MAX(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE MAX(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.max("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "MAX(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE MAX(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testMax_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "MAX(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE MAX(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.max("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "MAX(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE MAX(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testStddevSamp_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "STDDEV_SAMP(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE STDDEV_SAMP(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.stddev_samp("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "STDDEV_SAMP(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE STDDEV_SAMP  (t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testStddevSamp_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "STDDEV_SAMP(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE STDDEV_SAMP(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.stddev_samp("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "STDDEV_SAMP(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE STDDEV_SAMP(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testStddevPop_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "STDDEV_POP(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE STDDEV_POP(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.stddev_pop("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "STDDEV_POP(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE STDDEV_POP(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testStddevPop_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "STDDEV_POP(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE STDDEV_POP(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.stddev_pop("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "STDDEV_POP(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE STDDEV_POP(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testVarSamp_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "VAR_SAMP(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE VAR_SAMP(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.stddev_pop("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "VAR_SAMP(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE VAR_SAMP(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testVarSamp_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "VAR_SAMP(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE VAR_SAMP(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.var_samp("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "VAR_SAMP(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE VAR_SAMP(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testVarPop_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "VAR_POP(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE VAR_POP(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.stddev_pop("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "VAR_POP(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE VAR_POP(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testVarPop_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset",
                                 "VAR_POP(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE VAR_POP(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.var_samp("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "VAR_POP(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE VAR_POP(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testSkewness_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "SKEWNESS(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE SKEWNESS(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.stddev_pop("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "SKEWNESS(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE SKEWNESS(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testSkewness_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset",
                                 "SKEWNESS(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE SKEWNESS(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.var_samp("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "SKEWNESS(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE SKEWNESS(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testKurtosis_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "KURTOSIS(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE KURTOSIS(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.stddev_pop("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "KURTOSIS(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE KURTOSIS(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testKurtosis_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset",
                                 "KURTOSIS(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE KURTOSIS(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.var_samp("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "KURTOSIS(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE KURTOSIS(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    '''
    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testRowNumber_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once
        # mock_oaf = orderedAF.validate_agg_func("COUNT", None)
        # print(mock_oaf._columns)
        # print(mock_oaf.on)
        # print(mock_oaf.query)

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "ROW_NUMBER(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE ROW_NUMBER(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.stddev_pop("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "ROW_NUMBER(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE ROW_NUMBER(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testRowNumber_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once
        # mock_oaf = orderedAF.validate_agg_func("COUNT", None)
        # print(mock_oaf._columns)
        # print(mock_oaf.on)
        # print(mock_oaf.query)

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset",
                                 "ROW_NUMBER(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE ROW_NUMBER(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.var_samp("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "ROW_NUMBER(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE ROW_NUMBER(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testCumeDist_ColIsNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once
        # mock_oaf = orderedAF.validate_agg_func("COUNT", None)
        # print(mock_oaf._columns)
        # print(mock_oaf.on)
        # print(mock_oaf.query)

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset", "CUME_DIST(t.test_on) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE CUME_DIST(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.stddev_pop("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "CUME_DIST(t.test_on) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE CUME_DIST(t.test_on) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")

    @patch.object(AFrame, "__init__")
    @patch.object(OrderedAFrame, "validate_agg_func")
    def testCumeDist_ColIsNotNone(self, mock_method, mock_init):
        test_query = 'SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5;'
        orderedAF = OrderedAFrame("test_dataverse", "test_dataset", "test_columns", "test_on", test_query, None)
        mock_init.assert_called_once
        # mock_oaf = orderedAF.validate_agg_func("COUNT", None)
        # print(mock_oaf._columns)
        # print(mock_oaf.on)
        # print(mock_oaf.query)

        mock_oaf = OrderedAFrame("test_dataverse", "test_dataset",
                                 "CUME_DIST(t.test_cols) OVER(ORDER BY t.test_on )",
                                 "test_on",
                                 "SELECT VALUE CUME_DIST(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;",
                                 None)
        orderedAF.validate_agg_func = MagicMock(return_value=mock_oaf)
        actual = orderedAF.var_samp("test_columns")
        self.assertEqual(actual._dataverse, "test_dataverse")
        self.assertEqual(actual._dataset, "test_dataset")
        self.assertEqual(actual._columns, "CUME_DIST(t.test_cols) OVER(ORDER BY t.test_on )")
        self.assertEqual(actual.on, "test_on")
        self.assertEqual(actual.query,
                         "SELECT VALUE CUME_DIST(t.test_cols) OVER(ORDER BY t.test_on ) FROM (SELECT VALUE t FROM test_dataverse.test_dataset t LIMIT 5) t;")
    '''




if __name__ == '__main__':
    unittest.main()
