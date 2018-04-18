import pandas as pd
import numpy as np
import urllib.parse
import urllib.request
import pandas.io.json as json
from aframeObj import AFrameObj
from groupby import AFrameGroupBy


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
            new_query = 'select value t from %s t where %s;' %(dataset, key.schema)
            return AFrameObj(self._dataverse, self._dataset, key.schema, new_query)

        if isinstance(key, str):
            query = 'select value t.%s from %s t;' % (key, dataset)
            return AFrameObj(self._dataverse, self._dataset, key, query)

        if isinstance(key, (np.ndarray, list)):
            fields = ''
            for i in range(len(key)):
                if i > 0:
                    fields += ', '
                fields += 't.%s' % key[i]
            query = 'select %s from %s t;' % (fields, dataset)
            return AFrameObj(self._dataverse, self._dataset, key, query)

    def __len__(self):
        result = self.get_count()
        self._info['count'] = result
        return result

    def get_count(self):
        dataset = self._dataverse + '.' + self._dataset
        query = 'select value count(*) from %s;' % dataset
        result = self.send_request(query)[0]
        return result

    def __str__(self):
        if self._columns:
            txt = 'AsterixDB DataFrame with the following known columns: \n\t'
            return txt + str(self._columns)
        else:
            return 'Empty AsterixDB DataFrame'

    @property
    def columns(self):
        return str(self._columns)

    def toPandas(self, sample: int = 0):
        from pandas.io import json

        if self._dataset is None:
            raise ValueError('no dataset specified')
        else:
            dataset = self._dataverse+'.'+self._dataset
            if sample > 0:
                query = 'select value t from %s t limit %d;' % (dataset, sample)
            else:
                query = 'select value t from %s t;' % dataset
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
            query = 'select value t from %s t;' % dataset
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
            new_query = 'select value e from (%s) t unnest t e;' % col.query[:-1]
            return AFrameObj(self._dataverse, self._dataset, schema, new_query)
        elif isinstance(col, AFrameObj) and appended:
            if not name:
                raise ValueError('Must provide a string name for the appended column.')
            dataset = self._dataverse + '.' + self._dataset
            new_query = 'select u %s, t.* from %s t unnest t.%s u;' % (name, dataset, col.schema)
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
        new_query = 'select t.*, t.%s %s from %s t;' % (col.schema, name, dataset)
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

    @staticmethod
    def get_column_count(other):
        if not isinstance(other, AFrameObj):
            raise ValueError('A column must be of type \'AFrameObj\'')
        if isinstance(other, AFrameObj):
            query = 'select value count(*) from (%s) t;' % other.query[:-1]
            # print(query)
            return AFrame.send_request(query)[0]


    def create(self, path:str):
        query = 'create %s;\n' % self._dataverse
        query += 'use %s;\n' % self._dataverse
        host = 'http://localhost:19002/query/service'
        data = {}
        query += 'create type Schema as open{ \n' \
                 'id: int64};\n'
        query += 'create dataset %s(Schema) primary key id;\n' % self._dataset
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
        query = 'select value dt from Metadata.`Dataset` ds, Metadata.`Datatype` dt ' \
                'where ds.DatasetName = \'%s\' and ds.DatatypeName = dt.DatatypeName;' % dataset
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
            column = dict([('name', name), ('type', type), ('nullable', nullable)])
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
                query = 'select value object_merge(%s,%s) '% (lsuffix, rsuffix) + 'from %s %s ' %(l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s=%s.%s;' %(r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
            else:
                query = 'select %s,%s '% (lsuffix, rsuffix) + 'from %s %s ' % (l_dataset, lsuffix) +\
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
        new_query = 'select value %s(t%s) from %s t;' % (func, args_str, dataset)
        return AFrameObj(self._dataverse, self._dataset, schema, new_query)

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
            query = 'drop dataset %s.%s;' % (dataverse, dataset)
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
