import unittest
from unittest.mock import MagicMock, Mock, call
from aframe.aframe import AFrame, AFrameObj, OrderedAFrame, Window

class TestWindowFunction(unittest.TestCase):

    def testInit(self):
        window = Window()
        self.assertEqual(window._part, None)
        self.assertEqual(window._ord, None)
        self.assertEqual(window._rows, None)

    def testOrd(self):
        window = Window(ord='test_ord')
        self.assertEqual(window.ord(), 'test_ord')

    def testRows(self):
        window = Window(rows='test_rows')
        self.assertEqual(window.rows(), 'test_rows')

    def testPart(self):
        window = Window(part='test_part')
        self.assertEqual(window.part(), 'test_part')

    def testPartitionBy_ColsIsStr(self):
        cols = 'test_cols'
        window = Window('test_part', 'test_ord', 'test_rows')
        actual = window.partitionBy(cols)

        self.assertEqual(actual._part, 'test_cols')
        self.assertEqual(actual._ord, 'test_ord')
        self.assertEqual(actual._rows, 'test_rows')
        
    def testPartitionBy_ColsIsList(self):
        cols = ['attr1', 'attr2', 'attr3']
        window = Window('test_part', 'test_ord', 'test_rows')
##        part_list = ''
##        for i in range(len(cols)):
##            if i > 0:
##                part_list += ', '
##            part_list += 't.%s' % cols[i]
##        print(part_list)
        actual = window.partitionBy(cols)

        self.assertEqual(actual._part, 't.attr1, t.attr2, t.attr3') ##according to original code, should be t.[attr1, attr2, attr3]
        self.assertEqual(actual._ord, 'test_ord')
        self.assertEqual(actual._rows, 'test_rows')

    def testOrderBy_ColsIsStrOrderIsASC(self):
        window = Window('test_part', 'test_ord', 'test_rows')
        cols = 'test_cols'
        #print(ord)

        actual = window.orderBy(cols, 'ASC')
        self.assertEqual(actual._part, 'test_part')
        self.assertEqual(actual._ord, 'test_cols ASC')
        self.assertEqual(actual._rows, 'test_rows')

    def testOrderBy_ColsIsStrOrderIsDESC(self):
        window = Window('test_part', 'test_ord', 'test_rows')
        cols = 'test_cols'
        #print(ord)

        actual = window.orderBy(cols, 'DESC')
        self.assertEqual(actual._part, 'test_part')
        self.assertEqual(actual._ord, 'test_cols DESC')
        self.assertEqual(actual._rows, 'test_rows')

    def testOrderBy_ColsIsListOrderIsASC(self):
        window = Window('test_part', 'test_ord', 'test_rows')
        cols = ['attr1', 'attr2', 'attr3']
##        ord_list = ''
##        for i in range(len(cols)):
##            if i > 0:
##                ord_list += ', '
##            ord_list += 't.%s' % cols[i]
##        ord = '%s %s' % (ord_list,'ASC')
        #print(ord)

        actual = window.orderBy(cols, 'ASC')
        self.assertEqual(actual._part, 'test_part')
        self.assertEqual(actual._ord, 't.attr1, t.attr2, t.attr3 ASC') ##similar to PartitionBy
        self.assertEqual(actual._rows, 'test_rows')

    def testOrderBy_ColsIsListOrderIsDESC(self):
        window = Window('test_part', 'test_ord', 'test_rows')
        cols = ['attr1', 'attr2', 'attr3']
##        ord_list = ''
##        for i in range(len(cols)):
##            if i > 0:
##                ord_list += ', '
##            ord_list += 't.%s' % cols[i]
##        ord = '%s %s' % (ord_list,'ASC')
        #print(ord)

        actual = window.orderBy(cols, 'DESC')
        self.assertEqual(actual._part, 'test_part')
        self.assertEqual(actual._ord, 't.attr1, t.attr2, t.attr3 DESC') ##similar to PartitionBy
        self.assertEqual(actual._rows, 'test_rows')

    def testRowsBetween_StartIsZeroEndIsZero(self):
        window = Window('test_part', 'test_ord', 'test_rows')

        actual = window.rowsBetween(0, 0)
        self.assertEqual(actual._part, 'test_part')
        self.assertEqual(actual._ord, 'test_ord')
        self.assertEqual(actual._rows, 'ROWS BETWEEN CURRENT ROW AND CURRENT ROW')

    def testRowsBetween_StartIsZeroEndIsNotZero(self):
        window = Window('test_part', 'test_ord', 'test_rows')

        actual = window.rowsBetween(0, 3)
        self.assertEqual(actual._part, 'test_part')
        self.assertEqual(actual._ord, 'test_ord')
        self.assertEqual(actual._rows, 'ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING')

    def testRowBetween_StartIsNotZeroEndIsZero(self):
        window = Window('test_part', 'test_ord', 'test_rows')

        actual = window.rowsBetween(3, 0)
        self.assertEqual(actual._part, 'test_part')
        self.assertEqual(actual._ord, 'test_ord')
        self.assertEqual(actual._rows, 'ROWS BETWEEN 3 PRECEDING AND CURRENT ROW')

    def testRowBetween_StartIsNotZeroEndIsNotZero(self):
        window = Window('test_part', 'test_ord', 'test_rows')

        actual = window.rowsBetween(3, 3)
        self.assertEqual(actual._part, 'test_part')
        self.assertEqual(actual._ord, 'test_ord')
        self.assertEqual(actual._rows, 'ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING')
            
if __name__ == '__main__':
    unittest.main()
