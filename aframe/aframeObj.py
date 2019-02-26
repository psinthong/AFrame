import aframe as af
import pandas as pd
import pandas.io.json as json
import numpy as np

class AFrameObj:
    def __init__(self, dataverse, dataset, schema, query=None):
        self._schema = schema
        self._query = query
        self._data = None
        self._dataverse = dataverse
        self._dataset = dataset

    def __str__(self):
        return 'Column: '+str(self._schema)

    def __getitem__(self, key):
        dataset = self._dataverse + '.' + self._dataset
        if isinstance(key, AFrameObj):
            raise NotImplemented

        if isinstance(key, str):
            if self._schema is not None:
                query = 'SELECT VALUE t.%s FROM %s t WHERE %s;' %(key, dataset, self._schema)
                return AFrameObj(self._dataverse, self._dataset, key, query)
            else:
                raise NotImplemented

        if isinstance(key, (np.ndarray, list)):
            fields = ''
            for i in range(len(key)):
                if i > 0:
                    fields += ', '
                fields += 't.%s' % key[i]
            if self._schema is not None:
                query = 'SELECT %s FROM %s t WHERE %s;' % (fields, dataset, self._schema)
                return AFrameObj(self._dataverse, self._dataset, key, query)


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
        if '_uuid' in result.columns:
            result.drop('_uuid', axis=1, inplace=True)
        return result

    def head(self, num=5):
        new_query = self.query[:-1]+' LIMIT %d;' % num
        results = af.AFrame.send_request(new_query)
        json_str = json.dumps(results)
        result = pd.DataFrame(data=json.read_json(json_str))
        if '_uuid' in result.columns:
            result.drop('_uuid', axis=1, inplace=True)
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

    def __len__(self):
        query = 'SELECT VALUE count(*) FROM (%s) t;' % self.query[:-1]
        return af.AFrame.send_request(query)[0]

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
        new_query = 'SELECT VALUE t.%s %s %s FROM %s t;' % (self.schema, op, str(value), dataset)
        schema = '%s + %s' % (self.schema, value)
        return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def max(self):
        dataset = self._dataverse + '.' + self._dataset
        new_query = 'SELECT max(t.%s) FROM %s t;' % (self.schema, dataset)
        schema = 'max(%s)' % self.schema
        return AFrameObj(self._dataverse, dataset, schema, new_query)

    def min(self):
        dataset = self._dataverse + '.' + self._dataset
        new_query = 'SELECT min(t.%s) FROM %s t;' % (self.schema, dataset)
        schema = 'min(%s)' % self.schema
        return AFrameObj(self._dataverse, dataset, schema, new_query)

    def avg(self):
        dataset = self._dataverse + '.' + self._dataset
        new_query = 'SELECT avg(t.%s) FROM %s t;' % (self.schema, dataset)
        schema = 'avg(%s)' % self.schema
        return AFrameObj(self._dataverse, dataset, schema, new_query)

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
            if type(other) == str:
                schema = 't.%s %s \"%s\"' %(self.schema, opt, other)
            else:
                schema = 't.%s %s %s' % (self.schema, opt, other)
            query = 'SELECT VALUE %s FROM %s t;' %(schema, dataset)
            return AFrameObj(self._dataverse, self._dataset, schema, query)

    def __and__(self, other):
        if isinstance(self, af.AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            if isinstance(other, AFrameObj):
                return self.boolean_op(other, 'AND')

    def __or__(self, other):
        if isinstance(self, af.AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            if isinstance(other, AFrameObj):
                return self.boolean_op(other, 'OR')

    def boolean_op(self, other, op):
        dataset = self._dataverse + '.' + self._dataset
        schema = '%s %s %s' % (self.schema , op, other.schema)
        new_query = 'SELECT VALUE %s FROM %s t;' % (schema, dataset)
        return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def toAframe(self):
        dataverse, dataset = self.get_dataverse()
        return af.AFrame(dataverse, dataset)

    def get_dataverse(self):
        sub_query = self.query.lower().split("from")
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
        # schema = func + '(' + self.schema + args_str + ')'
        schema = '%s(t.%s%s)' % (func, self.schema, args_str)
        new_query = 'SELECT VALUE %s(t.%s%s) FROM %s t;' % (func, self.schema, args_str, dataset)
        return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def persist(self, name=None, dataverse=None):
        if self.schema is None:
            raise ValueError('Cannot write to AsterixDB!')
        if name is None:
            raise ValueError('Need to provide a name for the new dataset.')

        self.create_tmp_dataverse(dataverse)
        if dataverse:
            new_q = 'create dataset %s.%s(TempType) primary key _uuid autogenerated;' % (dataverse, name)
            new_q += '\n insert into %s.%s select value ((%s));' % (dataverse, name, self.query[:-1])
            result = af.AFrame.send(new_q)
            return af.AFrame(dataverse, name)
        else:
            new_q = 'create dataset _Temp.%s(TempType) primary key _uuid autogenerated;' % name
            new_q += '\n insert into _Temp.%s select value ((%s));' % (name, self.query[:-1])
            result = af.AFrame.send(new_q)
            return af.AFrame('_Temp', name)

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

    @staticmethod
    def create_tmp_dataverse(name=None):
        if name:
            query = 'create dataverse %s if not exists; ' \
                    '\n create type %s.TempType if not exists as open{ _uuid: uuid};' % (name, name)
        else:
            query = 'create dataverse _Temp if not exists; ' \
                '\n create type _Temp.TempType if not exists as open{ _uuid: uuid};'
        result = af.AFrame.send(query)
        return result

    def withColumn(self, name, col):
        if not isinstance(name, str):
            raise ValueError('Must provide a string name for the appended column.')
        if not isinstance(col, AFrameObj):
            raise ValueError('A column must be of type \'AFrameObj\'')
        cnt = af.AFrame.get_column_count(col)
        if self.get_count() != cnt:
            # print(self.get_count(), cnt)
            raise ValueError('The appended column must have the same size as the original AFrame.')
        dataset = self._dataverse + '.' + self._dataset
        fields = ''
        if isinstance(self.schema, list):
            for i in range(len(self.schema)):
                if i == 0:
                    fields += 't.'+self.schema[i]
                else:
                    fields += ', t.' + self.schema[i]
            new_query = 'SELECT %s, %s %s FROM %s t;' % (fields, col.schema, name, dataset)
            self.schema.append(name)
            return AFrameObj(self._dataverse, self._dataset, self.schema, new_query)
        new_query = 'SELECT t.*, %s %s FROM %s t;' % (col.schema, name, dataset)
        schema = col.schema
        return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def get_count(self):
        query = 'SELECT VALUE count(*) FROM (%s) t;' % self.query[:-1]
        result = af.AFrame.send_request(query)[0]
        return result

    def sort_values(self, by, ascending=True):
        new_query = 'SELECT VALUE t FROM (%s) t ' % self.query[:-1]
        if isinstance(by, str):
            if ascending:
                new_query += 'ORDER BY t.%s ;' % by
            else:
                new_query += 'ORDER BY t.%s DESC;' % by

        if isinstance(by, (np.ndarray, list)):
            by_list = ''
            for i in range(len(by)):
                if i > 0:
                    by_list += ', '
                    by_list += 't.%s' % by[i]
            if ascending:
                new_query += 'ORDER BY %s;' % by_list
            else:
                new_query += 'ORDER BY %s DESC;' % by_list
        schema = 'ORDER BY %s' % by
        return AFrameObj(self._dataverse, self._dataset, schema, new_query)