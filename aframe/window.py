class Window(object):

    def __init__(self,part=None,ord=None,rows=None):
        self._part = part
        self._ord = ord
        self._rows = rows

    def ord(self):
        return self._ord

    def rows(self):
        return self._rows

    def part(self):
        return self._part

    def partitionBy(self, cols):
        part_list = ''
        if isinstance(cols, str):
            part_list = cols
        elif isinstance(cols[0], list):
            for i in range(len(cols)):
                if i > 0:
                    part_list += ', '
                part_list += 't.%s' % cols[i]
        part = part_list
        return Window(part=part, ord=self.ord(), rows=self.rows())

    def orderBy(self, cols, order='ASC'):
        ord_list = ''
        if isinstance(cols, str):
            ord_list = cols
        elif isinstance(cols[0], list):
            for i in range(len(cols)):
                if i > 0:
                    ord_list += ', '
                ord_list += 't.%s' % cols[i]
        ord = '%s %s' % (ord_list,order)
        return Window(part=self.part(), ord=ord, rows=self.rows())

    def rowsBetween(self, start, end):
        start_str = ''
        end_str = ''
        if start == 0:
            start_str = 'CURRENT ROW'
        elif start != 0:
            start_str = '%d PRECEDING' % (start*-1)
        if end == 0:
            end_str = 'CURRENT ROW'
        elif end !=0:
            end_str = '%d FOLLOWING' % end
        rows = 'ROWS BETWEEN %s AND %s' %(start_str, end_str)
        return Window(part=self.part(), ord=self.ord(), rows=rows)