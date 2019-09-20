import pandas as pd
import numpy as np
import urllib.parse
import urllib.request
import urllib.error
import pandas.io.json as json
from aframe.groupby import AFrameGroupBy
from pandas.io.json import json_normalize
from aframe.window import Window


class AFrame:

    def __init__(self, dataverse, dataset, schema=None, query=None, predicate=None, server_address='http://localhost:19002'):
        # load in dataset definition
        self._dataverse = dataverse
        self._dataset = dataset
        self._server_address = server_address
        self._columns = None
        self._datatype = None
        self._datatype_name = None
        self._info = dict()
        #initialize
        self.get_dataset(dataverse=dataverse,dataset=dataset)
        if query is not None:
            self.query = query
        else:
            self.query = self.get_initial_query()
        self._schema = schema
        self._predicate = predicate

    @property
    def schema(self):
        return self._schema

    def get_initial_query(self):
        dataset = self._dataverse + '.' + self._dataset
        return 'SELECT VALUE t FROM %s t;' %dataset

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        if isinstance(key, AFrame):
            new_query = self.create_query('SELECT VALUE t FROM (%s) t WHERE %s;' % (self.query[:-1], key.schema), selection=key.schema)
            return type(self)(self._dataverse, self._dataset, key.schema, new_query)

        if isinstance(key, str):
            query = self.create_query('SELECT VALUE t.%s FROM (%s) t;' % (key, self.query[:-1]), projection=key)
            return type(self)(self._dataverse, self._dataset, key, query)

        if isinstance(key, (np.ndarray, list)):
            fields = ''
            for i in range(len(key)):
                if i > 0:
                    fields += ', '
                fields += 't.%s' % key[i]
            query = self.create_query('SELECT %s FROM (%s) t;' % (fields, self.query[:-1]), projection=key)
            return type(self)(self._dataverse, self._dataset, key, query)

    def __setitem__(self, key, value):
        dataset = self._dataverse + '.' + self._dataset
        if not isinstance(key, str):
            raise ValueError('Must provide a string name for the appended column.')
        if isinstance(value, OrderedAFrame):
            new_query = 'SELECT t.*, %s %s FROM %s t;' % (value._columns, key, dataset)
            self.query = new_query
        fields = ''
        new_query = 'SELECT t.*, %s %s FROM (%s) t;' % (value.schema, key, self.query[:-1])
        schema = value.schema
        if self._schema is not None:
            self._schema.append(schema)
        else:
            self._schema = [schema]
        self.query = new_query

    def __len__(self):
        result = self.get_count()
        self._info['count'] = result
        return result

    def create_query(self, query, selection=None, projection=None):
        return query

    def get_count(self):
        query = 'SELECT VALUE count(*) FROM (%s) t;' % self.query[:-1]
        result = self.send_request(query)[0]
        return result

    def __str__(self):
        if self._columns:
            txt = 'AsterixDB DataFrame with the following pre-defined columns: \n\t'
            return txt + str(self._columns)
        else:
            return 'Empty AsterixDB DataFrame'

    def head(self, sample=5):
        from pandas.io import json
        dataset = self._dataverse + '.' + self._dataset
        new_query = self.query[:-1] + ' LIMIT %d;' % sample

        result = self.send_request(new_query)
        data = json.read_json(json.dumps(result))
        df = pd.DataFrame(data)
        if '_uuid' in df.columns:
            df.drop('_uuid', axis=1, inplace=True)
        return df

    def flatten(self):
        return NestedAFrame(self._dataverse, self._dataset, self.columns, self.query)

    @property
    def columns(self):
        return self._columns

    def toPandas(self, sample: int = 0):
        from pandas.io import json

        if self._dataset is None:
            raise ValueError('no dataset specified')
        else:
            dataset = self._dataverse+'.'+self._dataset
            if sample > 0:
                query = 'SELECT VALUE t FROM (%s) t LIMIT %d;' % (dataset, self.query[:-1])
            else:
                query = 'SELECT VALUE t FROM (%s) t;' % self.query[:-1]
            result = self.send_request(query)
            data = json.read_json(json.dumps(result))
            df = pd.DataFrame(data)
            if '_uuid' in df.columns:
                df.drop('_uuid', axis=1, inplace=True)
            return df

    def collect_query(self):
        if self._dataset is None:
            raise ValueError('no dataset specified')
        else:
            dataset = self._dataverse+'.'+self._dataset
            query = 'SELECT VALUE t FROM %s t;' % dataset
            return query

    @staticmethod
    def attach_row_id(result_lst):
        if len(result_lst) > 0 and len(result_lst[0]) == 2 and 'row_id' in result_lst[0] and 'data' in result_lst[0]:
            flatten_results = []
            for i in result_lst:
                i['data']['row_id'] = i['row_id']
                flatten_results.append(i['data'])
            return flatten_results
        return result_lst

    def unnest(self, col, meta=None, appended=False, name=None):
        dataset = self._dataverse + '.' + self._dataset
        if isinstance(col, str):
            schema = 'unnest(t.%s)' % col
            new_query = 'SELECT VALUE %s FROM %s t unnest t.%s %s;' % (col, dataset, col, col)
            if isinstance(meta, (np.ndarray, list)):
                fields = ''
                for i in range(len(meta)):
                    if i > 0:
                        fields += ', '
                    fields += 't.%s' % meta[i]
                schema = '%s, t.%s' % (fields, col)
                new_query = 'SELECT %s, %s FROM %s t unnest t.%s %s;' % (fields, col, dataset, col, col)
            return type(self)(self._dataverse, self._dataset, schema, new_query)
        # if not isinstance(col, AFrameObj):
        #     raise ValueError('A column must be of type \'AFrameObj\'')
        if isinstance(col, AFrame) and not appended:
            schema = 'unnest(%s)' % col.schema
            new_query = 'SELECT VALUE e FROM (%s) t unnest t e;' % col.query[:-1]
            return type(self)(self._dataverse, self._dataset, schema, new_query)
        elif isinstance(col, AFrame) and appended:
            if not name:
                raise ValueError('Must provide a string name for the appended column.')

            new_query = 'SELECT u %s, t.* FROM %s t unnest t.%s u;' % (name, dataset, col.schema)
            schema = col.schema
            return type(self)(self._dataverse, self._dataset, schema, new_query)

    def withColumn(self, name, col):
        if not isinstance(name, str):
            raise ValueError('Must provide a string name for the appended column.')
        if not isinstance(col, AFrame):
            raise ValueError('A column must be of type \'AFrame\'')
        # cnt = self.get_column_count(col)
        # if self.get_count() != cnt:
        #     # print(self.get_count(), cnt)
        #     raise ValueError('The appended column must have the same size as the original AFrame.')
        dataset = self._dataverse + '.' + self._dataset
        # new_query = 'select t.*, t.%s %s from %s t;' % (col.schema, name, dataset)
        new_query = 'SELECT t.*, %s %s FROM %s t;' % (col.schema, name, dataset)
        schema = col.schema
        # columns = self._columns
        # columns.append(schema)
        # new_af = AFrame(self._dataverse, self._dataset, columns)
        # new_af.query = new_query
        # return new_af
        return type(self)(self._dataverse, self._dataset, schema, new_query)

    def notna(self):
        query = 'SELECT VALUE t FROM (%s) AS t WHERE ' % self.query[:-1]
        if isinstance(self.schema, (np.ndarray, list)):
            fields = self.schema
            fields_str = ''
            for field in fields:
                fields_str += 't.%s IS KNOWN AND ' % field
            fields_str = fields_str[:-4]
            query = query + fields_str + ';'
            schema = fields_str
        if isinstance(self.schema, str):
            field_str = 't.%s IS KNOWN' % self.schema
            query = query + field_str + ';'
            schema = field_str
        return type(self)(dataverse=self._dataverse, dataset=self._dataset, schema=schema, query=query)

    def isna(self):
        query = 'SELECT VALUE t FROM (%s) AS t WHERE ' % self.query[:-1]
        if isinstance(self.schema, (np.ndarray, list)):
            fields = self.schema
            fields_str = ''
            for field in fields:
                fields_str += 't.%s IS UNKNOWN AND ' % field
            fields_str = fields_str[:-4]
            query = query + fields_str + ';'
            schema = fields_str
        if isinstance(self.schema, str):
            field_str = 't.%s IS UNKNOWN' % self.schema
            query = query + field_str + ';'
            schema = field_str
        return type(self)(dataverse=self._dataverse, dataset=self._dataset, schema=schema, query=query)

    def isnull(self):
        return self.isna()

    def notnull(self):
        return self.notna()

    @staticmethod
    def get_column_count(other):
        if not isinstance(other, AFrame):
            raise ValueError('A column must be of type \'AFrameObj\'')
        if isinstance(other, AFrame):
            query = 'SELECT VALUE count(*) FROM (%s) t;' % other.query[:-1]
            # print(query)
            return other.send_request(query)[0]

    def create(self, path:str):
        query = 'create %s;\n' % self._dataverse
        query += 'use %s;\n' % self._dataverse
        host = 'http://localhost:19002/query/service'
        data = {}
        query += 'CREATE TYPE Schema AS open{ \n' \
                 'id: int64};\n'
        query += 'CREATE DATASET %s(Schema) PRIMARY KEY id;\n' % self._dataset
        query += 'LOAD DATASET %s USING localfs\n ' \
                 '((\"path\"=\"127.0.0.1://%s\"),(\"format\"=\"adm\"));\n' % (self._dataset, path)

        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            result = json.loads(handler.read())
            ret_array = result['results']
            # return pd.DataFrame(json.read_json(json.dumps(ret_array)))

    def init_columns(self, columns=None):
        if columns is None:
            raise ValueError('no columns specified')

    def get_dataset(self, dataset,dataverse):
        query = 'SELECT VALUE dt FROM Metadata.`Dataset` ds, Metadata.`Datatype` dt ' \
                'WHERE ds.DatasetName = \"%s\" AND ds.DatatypeName = dt.DatatypeName ' \
                'AND dt. DataverseName = \"%s\";' % (dataset,dataverse)
        # print(query)
        result = self.send_request(query)

        if len(result) == 0:
            raise ValueError('Cannot find %s.%s' %(dataverse,dataset))
        else:
            result = result[0]

        is_open = result['Derived']['Record']['IsOpen']
        if is_open:
            self._datatype = 'open'
        else:
            self._datatype = 'close'
        self._datatype_name = result['DatatypeName']
        fields = result['Derived']['Record']['Fields']
        for field in fields:
            name = field['FieldName']
            type = field['FieldType']
            nullable = field['IsNullable']
            column = dict([(name, type)])
            if self._columns is None:
                self._columns = [column]
            else:
                self._columns.append(column)

    def join(self, other, left_on, right_on, how='inner', lsuffix='l', rsuffix='r'):

        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        if isinstance(other, AFrame):
            if left_on is None or right_on is None:
                raise ValueError('Missing join columns')
            if how not in join_types:
                raise NotImplementedError('Join type specified is not yet available')

            l_dataset = '(%s)' % self.query[:-1]
            r_dataset = other._dataverse + '.' + other._dataset
            if other.query is not None:
                r_dataset = '(%s)' % other.query[:-1]

            if left_on != right_on:
                query = 'SELECT VALUE object_merge(%s,%s) '% (lsuffix, rsuffix) + 'FROM %s %s ' %(l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' %(r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
            else:
                query = 'SELECT %s,%s '% (lsuffix, rsuffix) + 'from %s %s ' % (l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
            schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)

            return type(self)(self._dataverse, self._dataset, schema, query)

    def groupby(self, by):
        return AFrameGroupBy(self._dataverse, self._dataset, self._server_address, by)

    def apply(self, func, *args, **kwargs):
        if not isinstance(func, str):
            raise TypeError('Function name must be string.')
        dataset = self._dataverse + '.' + self._dataset
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
        schema = func + '(t' + args_str + ')'
        new_query = 'SELECT VALUE %s(t%s) FROM (%s) t;' % (func, args_str, self.query[:-1])
        return type(self)(self._dataverse, self._dataset, schema, new_query)

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
        return AFrame(self._dataverse, self._dataset, schema, new_query)

    def describe(self):
        num_cols = []
        str_cols = []
        numeric_types = ['int','int8','int16', 'int32', 'int64', 'double',
                         'integer', 'smallint', 'tinyint', 'bigint', 'float']
        index = ['count', 'mean', 'std', 'min', 'max']
        data = []
        dataset = self._dataverse + '.' + self._dataset

        fields = ''
        cols = self.columns
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
        # contains min,max,cnt,avg results of all attributes
        stats = self.send_request(query)[0]

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

        # contains standard deviation results of all numeric attributes
        stds = self.send_request(std_query)[0]

        all_cols = str_cols+num_cols

        # iterate over each row and add both numeric and string values from the json result
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

        res = pd.DataFrame(data, index=index, columns=all_cols)
        return res

    def rolling(self, window=None, on=None):
        if window is not None and not isinstance(window, Window):
            raise ValueError('window object must be of type Window')
        elif on is None and window is None:
            raise ValueError('Must provide at least \'on\' or \'window\' value')
        else:
            return OrderedAFrame(self._dataverse, self._dataset, self._columns, on, self.query, window)

    #------------------ migrate AFrameObj methods
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

    # def __len__(self):
    #     query = 'SELECT VALUE count(*) FROM (%s) t;' % self.query[:-1]
    #     return self.send_request(query)[0]

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
        new_query = 'SELECT VALUE t %s %s FROM (%s) t;' % (op, str(value), self.query[:-1])
        schema = '%s + %s' % (self.schema, value)
        return type(self)(self._dataverse, self._dataset, schema, new_query)

    def max(self):
        new_query = 'SELECT max(t.%s) FROM (%s) t;' % (self.schema, self.query[:-1])
        schema = 'max(%s)' % self.schema
        return type(self)(self._dataverse, self._dataset, schema, new_query)

    def min(self):
        new_query = 'SELECT min(t.%s) FROM (%s) t;' % (self.schema, self.query[:-1])
        schema = 'min(%s)' % self.schema
        return type(self)(self._dataverse, self._dataset, schema, new_query)

    def avg(self):
        new_query = 'SELECT avg(t.%s) FROM (%s) t;' % (self.schema, self.query[:-1])
        schema = 'avg(%s)' % self.schema
        return type(self)(self._dataverse, self._dataset, schema, new_query)

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
        if type(other) == str:
            schema = 't.%s %s \'%s\'' %(self.schema, opt, other)
        else:
            schema = 't.%s %s %s' % (self.schema, opt, other)
        query = 'SELECT VALUE %s FROM (%s) t;' %(schema, self.query[:-1])
        return type(self)(self._dataverse, self._dataset, schema, query)

    def __and__(self, other):
        return self.boolean_op(other, 'AND')

    def __or__(self, other):
        return self.boolean_op(other, 'OR')

    def boolean_op(self, other, op):
        schema = '%s %s %s' % (self.schema , op, other.schema)
        new_query = 'SELECT VALUE %s FROM (%s) t;' % (schema, self.query[:-1])
        return type(self)(self._dataverse, self._dataset, schema, new_query)

    def get_dataverse(self):
        sub_query = self.query.lower().split("from")
        data = sub_query[1][1:].split(".", 1)
        dataverse = data[0]
        dataset = data[1].split(" ")[0]
        return dataverse, dataset

    def map(self, func, *args, **kwargs):
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
        new_query = 'SELECT VALUE %s(t%s) FROM (%s) t;' % (func, args_str, self.query[:-1])

        predicate = None
        if self._predicate is not None:
            predicate = self._predicate
            new_query = 'SELECT VALUE %s(t.%s%s) FROM (%s) t WHERE %s;' % (func, self.schema, args_str, self.query[:-1], self._predicate)
        # elif self._query is not None:
        #     new_query = 'SELECT %s FROM (%s) t;' % (fields, self.query[:-1])
        # else:
        #     new_query = 'SELECT %s FROM %s t WHERE %s;' % (fields, dataset, self._schema)

        return AFrame(self._dataverse, self._dataset, schema, new_query, predicate)

    def persist(self, name=None, dataverse=None):
        if self.schema is None:
            raise ValueError('Cannot write to AsterixDB!')
        if name is None:
            raise ValueError('Need to provide a name for the new dataset.')

        self.create_tmp_dataverse(dataverse)
        if dataverse:
            new_q = 'create dataset %s.%s(TempType) primary key _uuid autogenerated;' % (dataverse, name)
            new_q += '\n insert into %s.%s select value ((%s));' % (dataverse, name, self.query[:-1])
            result = self.send(new_q)
            return AFrame(dataverse, name)
        else:
            new_q = 'create dataset _Temp.%s(TempType) primary key _uuid autogenerated;' % name
            new_q += '\n insert into _Temp.%s select value ((%s));' % (name, self.query[:-1])
            result = self.send(new_q)
            return AFrame('_Temp', name)

    def get_dataType(self):
        query = 'select value t. DatatypeName from Metadata.`Dataset` t where' \
                ' t.DataverseName = \"%s\" and t.DatasetName = \"%s\"' % (self._dataverse, self._dataset)
        result = self.send_request(query)
        return result[0]

    def get_primary_key(self):
        query = 'select value p from Metadata.`Dataset` t unnest t.InternalDetails.PrimaryKey p ' \
                'where t.DatasetName = \"%s\" and t.DataverseName=\"%s\" ;' %(self._dataset, self._dataverse)
        keys = self.send_request(query)
        return keys[0][0]

    def create_tmp_dataverse(self,name=None):
        if name:
            query = 'create dataverse %s if not exists; ' \
                    '\n create type %s.TempType if not exists as open{ _uuid: uuid};' % (name, name)
        else:
            query = 'create dataverse _Temp if not exists; ' \
                '\n create type _Temp.TempType if not exists as open{ _uuid: uuid};'
        result = self.send(query)
        return result

    # def __setitem__(self, key, value):
    #     if not isinstance(key, str):
    #         raise ValueError('Must provide a string name for the appended column.')
    #     if not isinstance(value, AFrame):
    #         raise ValueError('A column must be of type \'AFrameObj\'')
    #     dataset = self._dataverse + '.' + self._dataset
    #     fields = ''
    #     if isinstance(self.schema, list):
    #         for i in range(len(self.schema)):
    #             if i == 0:
    #                 fields += 't.' + self.schema[i]
    #             else:
    #                 fields += ', t.' + self.schema[i]
    #         new_query = 'SELECT %s, %s %s FROM (%s) t;' % (fields, value.schema, key, self.query[:-1])
    #         self.schema.append(key)
    #         self._query = new_query
    #     else:
    #         if self.query is None:
    #             new_query = 'SELECT t.*, %s %s FROM %s t;' % (value.schema, key, dataset)
    #         else:
    #             new_query = 'SELECT t.*, %s %s FROM (%s) t;' % (value.schema, key, self.query[:-1])
    #         schema = value.schema
    #         self._schema = schema
    #         self._query = new_query

    # def withColumn(self, name, col):
    #     if not isinstance(name, str):
    #         raise ValueError('Must provide a string name for the appended column.')
    #     # if not isinstance(col, AFrame):
    #     #     raise ValueError('A column must be of type \'AFrameObj\'')
    #     # cnt = af.AFrame.get_column_count(col)
    #     # if self.get_count() != cnt:
    #     #     # print(self.get_count(), cnt)
    #     #     raise ValueError('The appended column must have the same size as the original AFrame.')
    #     dataset = self._dataverse + '.' + self._dataset
    #     fields = ''
    #     if isinstance(self.schema, list):
    #         for i in range(len(self.schema)):
    #             if i == 0:
    #                 fields += 't.'+self.schema[i]
    #             else:
    #                 fields += ', t.' + self.schema[i]
    #         new_query = 'SELECT %s, %s %s FROM (%s) t;' % (fields, col.schema, name, self.query[:-1])
    #         self.schema.append(name)
    #         return AFrame(self._dataverse, self._dataset, self.schema, new_query)
    #     new_query = 'SELECT t.*, %s %s FROM (%s) t;' % (col.schema, name, self.query[:-1])
    #     schema = col.schema
    #     return AFrame(self._dataverse, self._dataset, schema, new_query)

    # def get_count(self):
    #     query = 'SELECT VALUE count(*) FROM (%s) t;' % self.query[:-1]
    #     result = AFrame.send_request(query)[0]
    #     return result


        #-------------migrate AFrameObj methods

    def send_request(self, query: str):
        host = self._server_address+'/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        try:
            handler = urllib.request.urlopen(host,data)
            result = json.loads(handler.read())
            return result['results']

        except urllib.error.URLError as e:
            raise Exception('The following error occured: %s. Please check if AsterixDB is running.' %str(e.reason))

    def send(self, query: str):
        host = self._server_address+'/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            result = json.loads(handler.read())
            return result['status']

    @staticmethod
    def drop(af=None, dataverse=None, dataset=None):
        if isinstance(af, AFrame):
            query = 'DROP DATASET %s.%s;' % (af._dataverse, af._dataset)
            result = af.send(query)
            return result
        if (dataverse is not None) and (dataset is not None):
            af = AFrame(dataverse, dataset)
            query = 'DROP DATASET %s.%s;' % (af._dataverse, af._dataset)
            result = af.send(query)
            return result

    @staticmethod
    def send_perf(query):
        host = 'http://localhost:19002/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            ret = handler.read()
            return ret


class NestedAFrame(AFrame):
    def __init__(self, dataverse, dataset, schema, query=None):
        self._schema = schema
        self._query = query
        self._data = None
        self._dataverse = dataverse
        self._dataset = dataset
        AFrame.__init__(self,dataverse,dataset)

    def head(self, sample=5):
        dataset = self._dataverse + '.' + self._dataset
        if self._query is not None:
            new_query = self._query[:-1]+' LIMIT %d;' % sample
            results = self.send_request(new_query)
            norm_result = json_normalize(results)
        else:
            self._query = 'SELECT VALUE t FROM %s t;' % dataset
            new_query = self._query[:-1] + ' LIMIT %d;' % sample
            results = self.send_request(new_query)
            norm_result = json_normalize(results)
        norm_cols = norm_result.columns.str.split('.', expand=True).values
        norm_result.columns = pd.MultiIndex.from_tuples([('', x[0]) if pd.isnull(x[1]) else x for x in norm_cols])

        if '_uuid' in norm_result.columns:
            norm_result.drop('_uuid', axis=1, inplace=True)
        return norm_result


class OrderedAFrame(AFrame):
    def __init__(self,dataverse, dataset, columns, on, query, window=None):
        AFrame.__init__(self, dataverse, dataset)
        self._window = window
        self._columns = columns
        self._data = None
        self._dataverse = dataverse
        self._dataset = dataset
        self.on = on
        self.query = query

    def get_window(self):
        over = ''
        if self._window is not None:
            if self._window.part() is not None:
                over += 'PARTITION BY t.%s ' % self._window._part
            if self._window.ord() is not None:
                over += 'ORDER BY t.%s ' % self._window._ord
            if self._window.rows() is not None:
                over += self._window._rows
        else:
            over += 'ORDER BY t.%s ' % self.on
        return 'OVER(%s)' % over

    def validate_agg_func(self, func, arg=None):
        over = self.get_window()
        dataset = '(%s)' % self.query[:-1]
        if self.on is not None:
            if arg is None:
                col = '%s(t.%s) %s' % (func, self.on, over)
                query = 'SELECT VALUE %s FROM %s t;' % (col, dataset)
            else:
                col = '%s(t.%s) %s' % (func, arg, over)
                query = 'SELECT VALUE %s FROM %s t;' % (col, dataset)
        elif self._window is not None:
            if arg is None:
                col = '%s(t.%s) %s' % (func, self._window.ord(), over)
                query = 'SELECT VALUE %s FROM %s t;' % (col, dataset)
            else:
                col = '%s(t.%s) %s' % (func, arg, over)
                query = 'SELECT VALUE %s FROM %s t;' % (col, dataset)
        else:
            raise ValueError('Must provide either on or window')
        return OrderedAFrame(self._dataverse, self._dataset, col, self.on, query, self._window)

    def sum(self,col=None):
        return self.validate_agg_func('SUM',col)

    def count(self, col=None):
        return self.validate_agg_func('COUNT', col)

    def avg(self, col=None):
        return self.validate_agg_func('AVG', col)

    mean = avg

    def min(self, col=None):
        return self.validate_agg_func('MIN', col)

    def max(self, col=None):
        return self.validate_agg_func('MAX', col)

    def stddev_samp(self, col=None):
        return self.validate_agg_func('STDDEV_SAMP', col)

    def stddev_pop(self, col=None):
        return self.validate_agg_func('STDDEV_POP', col)

    def var_samp(self, col=None):
        return self.validate_agg_func('VAR_SAMP', col)

    def var_pop(self, col=None):
        return self.validate_agg_func('VAR_POP', col)

    def skewness(self, col=None):
        return self.validate_agg_func('SKEWNESS', col)

    def kurtosis(self, col=None):
        return self.validate_agg_func('KURTOSIS', col)

    def row_number(self):
        return self.validate_window_function('ROW_NUMBER')

    def cume_dist(self):
        return self.validate_window_function('CUME_DIST')

    def dense_rank(self):
        return self.validate_window_function('DENSE_RANK')

    def first_value(self, expr, ignore_null=False):
        return self.validate_window_function_argument('FIRST_VALUE', expr, ignore_null)

    def lag(self, offset, expr, ignore_null=False):
        return self.validate_window_function_two_arguments('LAG', offset, expr, ignore_null)

    def last_value(self, expr, ignore_null=False):
        return self.validate_window_function_argument('LAST_VALUE', expr, ignore_null)

    def lead(self, offset, expr, ignore_null=False):
        return self.validate_window_function_two_arguments('LEAD', offset, expr, ignore_null)

    def nth_value(self, offset, expr, ignore_null=False):
        return self.validate_window_function_two_arguments('NTH_VALUE', offset, expr, ignore_null)

    def ntile(self, num_tiles):
        return self.validate_window_function_argument('NTILE', str(num_tiles), False)

    def percent_rank(self):
        return self.validate_window_function('PERCENT_RANK')

    def rank(self):
        return self.validate_window_function('RANK')

    def ratio_to_report(self, expr):
        return self.validate_window_function_argument('RATIO_TO_REPORT', expr, False)

    def collect(self):
        results = self.send_request(self.query)
        json_str = json.dumps(results)
        result = pd.DataFrame(data=json.read_json(json_str))
        if '_uuid' in result.columns:
            result.drop('_uuid', axis=1, inplace=True)
        return result

    def validate_window_function(self, func):
        dataset = '(%s)' % self.query[:-1]
        over = self.get_window()
        columns = '%s() %s' % (func, over)
        query = 'SELECT VALUE %s FROM %s t;' % (columns, dataset)
        return OrderedAFrame(self._dataverse, self._dataset, columns, self.on, query, self._window)

    def validate_window_function_argument(self, func, expr, ignore_null):
        if not isinstance(expr,str):
            raise ValueError('expr for first_value must be string')
        dataset = '(%s)' % self.query[:-1]
        over = self.get_window()
        columns = '%s(%s) %s' % (func, expr, over)
        if ignore_null:
            columns = '%s(%s) IGNORE NULLS %s' % (func, expr, over)
        query = 'SELECT VALUE %s FROM %s t;' % (columns, dataset)
        return OrderedAFrame(self._dataverse, self._dataset, columns, self.on, query, self._window)

    def validate_window_function_two_arguments(self, func, offset, expr, ignore_null=False):
        if not isinstance(expr,str):
            error_msg = 'expr for %s must be string' % func
            raise ValueError(error_msg)
        if not isinstance(offset,int):
            error_msg = 'offset for %s must be an integer' % func
            raise ValueError(error_msg)
        dataset = '(%s)' % self.query[:-1]
        over = self.get_window()
        columns = '%s(%s, %d) %s' % (func, expr, offset, over)
        if ignore_null:
            columns = '%s(%s,%d) IGNORE NULLS %s' % (func, expr, offset, over)
        query = 'SELECT VALUE %s FROM %s t;' % (columns, dataset)
        return OrderedAFrame(self._dataverse, self._dataset, columns, self.on, query, self._window)