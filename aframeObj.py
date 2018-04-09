import aframe as af
import pandas as pd
import pandas.io.json as json


class AFrameObj:
    def __init__(self, dataverse, dataset, schema, query=None):
        self._schema = schema
        self._query = query
        self._data = None
        self._dataverse = dataverse
        self._dataset = dataset

    def __str__(self):
        return 'Column: '+str(self._schema)

    @property
    def schema(self):
        return self._schema

    @property
    def query(self):
        return str(self._query)

    def collect(self):
        results = af.AFrame.send_request(self._query)
        json_str = json.dumps(results)
        result = pd.DataFrame(data=json.read_json(json_str))
        return result

    def head(self, num=5):
        new_query = self.query[:-1]+' limit %d' %num
        results = af.AFrame.send_request(new_query)
        json_str = json.dumps(results)
        result = pd.DataFrame(data=json.read_json(json_str))
        return result

    def __add__(self, other):
        return self.add(other)

    def __sub__(self, other):
        return self.sub(other)

    def __mul__(self, other):
        return self.mul(other)

    def __mod__(self, other):
        return self.mod(other)

    def __pow__(self, power, modulo=None):
        return self.pow(power)

    def __truediv__(self, other):
        return self.div(other)

    def add(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, '+')

    def sub(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, '-')

    def div(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, '/')

    def mul(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, '*')

    def mod(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, '%')

    def pow(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, '^')

    def arithmetic_op(self, value, op):
        dataset = self._dataverse + '.' + self._dataset
        new_query = 'select value t.%s %s %s from %s t;' % (self.schema, op, str(value), dataset)
        schema = '%s + %s' % (self.schema, value)
        return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def __eq__(self, other):
        return self.binary_opt(other, '=')

    def __ne__(self, other):
        return self.binary_opt(other, '!=')

    def __gt__(self, other):
        return self.binary_opt(other, '>')

    def __lt__(self, other):
        return self.binary_opt(other, '<')

    def __ge__(self, other):
        return self.binary_opt(other, '>=')

    def __le__(self, other):
        return self.binary_opt(other, '<=')

    def binary_opt(self, other, opt):
        dataset = self._dataverse+'.'+self._dataset
        if isinstance(self, af.AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            schema = 't.%s %s %s' %(self.schema, opt, other)
            query = 'select value %s from %s t;' %(schema, dataset)
            return AFrameObj(self._dataverse, self._dataset, schema, query)

    def __and__(self, other):
        dataset = self._dataverse + '.' + self._dataset
        if isinstance(self, af.AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            if isinstance(other, AFrameObj):
                schema = self.schema+' and '+other.schema
                new_query = 'select value %s from %s t;' %(schema, dataset)
                return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def toAframe(self):
        dataverse, dataset = self.get_dataverse()
        return af.AFrame(dataverse, dataset)

    def get_dataverse(self):
        sub_query = self.query.split("from")
        data = sub_query[1][1:].split(".", 1)
        dataverse = data[0]
        dataset = data[1].split(" ")[0]
        return dataverse, dataset

    def map(self, func, *args, **kwargs):
        dataset = self._dataverse + '.' + self._dataset
        if not isinstance(func, str):
            raise TypeError('Function name must be string.')
        args_str = ''
        if args:
            for arg in args:
                if isinstance(arg, str):
                    args_str += ', \"%s\"' % arg
                else:
                    args_str += ', ' + str(arg)
        if kwargs:
            for key, value in kwargs.items():
                if isinstance(value, str):
                    args_str += ', %s = \"%s\"' % (key, value)
                else:
                    args_str += ', %s = %s' % (key, str(value))
        schema = func + '(' + self.schema + args_str + ')'
        new_query = 'select value %s(t.%s%s) from %s t;' % (func, self.schema, args_str, dataset)
        return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def persist(self, name=None, primary_key=None):
        if self.schema is None:
            raise ValueError('Cannot write to AsterixDB!')
        if name is None:
            raise ValueError('Need to provide a name for the new dataset.')
        data_type = self.get_dataType()

        if primary_key is None:
            primary = self.get_primary_key()
        else:
            primary = primary_key
        # print('primary: ', primary)
        new_q = 'create dataset %s.%s(%s) primary key %s;' % (self._dataverse, name, data_type, primary)
        new_q += '\n insert into %s.%s select value ((%s));' % (self._dataverse, name, self.query[:-1])
        # print(new_q)
        result = af.AFrame.send(new_q)
        # print('result :', result)
        return af.AFrame(self._dataverse, name)

    def get_dataType(self):
        query = 'select value t. DatatypeName from Metadata.`Dataset` t where' \
                ' t.DataverseName = \"%s\" and t.DatasetName = \"%s\"' % (self._dataverse, self._dataset)
        result = af.AFrame.send_request(query)
        return result[0]

    def get_primary_key(self):
        query = 'select value p from Metadata.`Dataset` t unnest t.InternalDetails.PrimaryKey p ' \
                'where t.DatasetName = \"%s\" and t.DataverseName=\"%s\" ;' %(self._dataset, self._dataverse)
        keys = af.AFrame.send_request(query)
        return keys[0][0]
