import pandas as pd
import numpy as np
import urllib.parse
import urllib.request
import pandas.io.json as json
from aframe.aframeObj import AFrameObj
from aframe.groupby import AFrameGroupBy
from aframe.missing import notna
from aframe.nestedAFrame import NestedAFrame

class AFrame:

    def __init__(self, dataverse, dataset, columns=None, path=None):
        # if dataset doesn't exist -> create it
        # else load in its definition
        self._dataverse = dataverse
        self._dataset = dataset
        self._columns = columns
        self._datatype = None
        self._datatype_name = None
        self._info = dict()
        #initialize
        self.get_dataset(dataset)
        self.query = None

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        dataset = self._dataverse + '.' + self._dataset
        if isinstance(key, AFrameObj):
            new_query = 'SELECT VALUE t FROM %s t WHERE %s;' %(dataset, key.schema)
            return AFrameObj(self._dataverse, self._dataset, key.schema, new_query)

        if isinstance(key, str):
            query = 'SELECT VALUE t.%s FROM %s t;' % (key, dataset)
            return AFrameObj(self._dataverse, self._dataset, key, query)

        if isinstance(key, (np.ndarray, list)):
            fields = ''
            for i in range(len(key)):
                if i > 0:
                    fields += ', '
                fields += 't.%s' % key[i]
            query = 'SELECT %s FROM %s t;' % (fields, dataset)
            return AFrameObj(self._dataverse, self._dataset, key, query)

    def __len__(self):
        result = self.get_count()
        self._info['count'] = result
        return result

    def get_count(self):
        dataset = self._dataverse + '.' + self._dataset
        query = 'SELECT VALUE count(*) FROM %s;' % dataset
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
        if self.query is None:
            self.query = 'SELECT VALUE t FROM %s t;' % dataset
        new_query = self.query[:-1] + ' limit %d;' % sample
        result = self.send_request(new_query)
        data = json.read_json(json.dumps(result))
        df = pd.DataFrame(data)
        if '_uuid' in df.columns:
            df.drop('_uuid', axis=1, inplace=True)
        return df

    def normalize(self):
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
                query = 'SELECT VALUE t FROM %s t LIMIT %d;' % (dataset, sample)
            else:
                query = 'SELECT VALUE t FROM %s t;' % dataset
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

    def unnest(self, col, appended=False, name=None):
        if not isinstance(col, AFrameObj):
            raise ValueError('A column must be of type \'AFrameObj\'')
        if isinstance(col, AFrameObj) and not appended:
            schema = 'unnest(%s)' % col.schema
            new_query = 'SELECT VALUE e FROM (%s) t unnest t e;' % col.query[:-1]
            return AFrameObj(self._dataverse, self._dataset, schema, new_query)
        elif isinstance(col, AFrameObj) and appended:
            if not name:
                raise ValueError('Must provide a string name for the appended column.')
            dataset = self._dataverse + '.' + self._dataset
            new_query = 'SELECT u %s, t.* FROM %s t unnest t.%s u;' % (name, dataset, col.schema)
            schema = col.schema
            return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def withColumn(self, name, col):
        if not isinstance(name, str):
            raise ValueError('Must provide a string name for the appended column.')
        if not isinstance(col, AFrameObj):
            raise ValueError('A column must be of type \'AFrameObj\'')
        cnt = self.get_column_count(col)
        if self.get_count() != cnt:
            # print(self.get_count(), cnt)
            raise ValueError('The appended column must have the same size as the original AFrame.')
        dataset = self._dataverse + '.' + self._dataset
        # new_query = 'select t.*, t.%s %s from %s t;' % (col.schema, name, dataset)
        new_query = 'SELECT t.*, %s %s FROM %s t;' % (col.schema, name, dataset)
        schema = col.schema
        # columns = self._columns
        # columns.append(schema)
        # new_af = AFrame(self._dataverse, self._dataset, columns)
        # new_af.query = new_query
        # return new_af
        return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def toAFrameObj(self):
        if self.query:
            return AFrameObj(self._dataverse, self._dataset, None, self.query)

    def notna(self):
        return notna(self)


    @staticmethod
    def get_column_count(other):
        if not isinstance(other, AFrameObj):
            raise ValueError('A column must be of type \'AFrameObj\'')
        if isinstance(other, AFrameObj):
            query = 'SELECT VALUE count(*) FROM (%s) t;' % other.query[:-1]
            # print(query)
            return AFrame.send_request(query)[0]


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

    def get_dataset(self, dataset):
        query = 'SELECT VALUE dt FROM Metadata.`Dataset` ds, Metadata.`Datatype` dt ' \
                'WHERE ds.DatasetName = \'%s\' AND ds.DatatypeName = dt.DatatypeName;' % dataset
        # print(query)
        result = self.send_request(query)[0]

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
            if self._columns:
                self._columns.append(column)
            else:
                self._columns = [column]

    def join(self, other, left_on, right_on, how='inner', lsuffix='l', rsuffix='r'):

        join_types = {'inner': 'JOIN', 'left': 'LEFT OUTER JOIN'}
        if isinstance(other, AFrame):
            if left_on is None or right_on is None:
                raise ValueError('Missing join columns')
            if how not in join_types:
                raise NotImplementedError('Join type specified is not yet available')

            l_dataset = self._dataverse + '.' + self._dataset
            r_dataset = other._dataverse + '.' + other._dataset

            if left_on != right_on:
                query = 'SELECT VALUE object_merge(%s,%s) '% (lsuffix, rsuffix) + 'FROM %s %s ' %(l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s=%s.%s;' %(r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
            else:
                query = 'SELECT %s,%s '% (lsuffix, rsuffix) + 'from %s %s ' % (l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s=%s.%s;' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
            schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)

            return AFrameObj(self._dataverse, self._dataset, schema, query)

    def groupby(self, by):
        return AFrameGroupBy(self._dataverse, self._dataset, by)

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
        new_query = 'SELECT VALUE %s(t%s) FROM %s t;' % (func, args_str, dataset)
        return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def sort_values(self, by, ascending=True):
        dataset = self._dataverse + '.' + self._dataset
        new_query = 'SELECT VALUE t FROM %s t ' % dataset
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


    @staticmethod
    def send_request(query: str):
        host = 'http://localhost:19002/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            result = json.loads(handler.read())
            return result['results']

    @staticmethod
    def send(query: str):
        host = 'http://localhost:19002/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            result = json.loads(handler.read())
            return result['status']

    @staticmethod
    def drop(aframe):
        if isinstance(aframe, AFrame):
            dataverse = aframe._dataverse
            dataset = aframe._dataset
            query = 'DROP DATASET %s.%s;' % (dataverse, dataset)
            result = AFrame.send(query)
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
