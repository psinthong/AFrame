import pandas as pd
import urllib.parse
import urllib.request
import pandas.io.json as json
import json as pyJson
import re
import decimal


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
        results = AFrame.send_request(self._query)
        result_flatten = AFrame.attach_row_id(results)
        if all(i is None for i in result_flatten):
            raise KeyError(self._schema)
        self._data = result_flatten
        # return pd.Series(self._data)
        result = pd.DataFrame(json.read_json(json.dumps(result_flatten)))
        result.set_index('row_id', inplace=True)
        return result

    def head(self, num=5):
        new_query = self._query[:-1]+' order by t.row_id limit %d;' % num
        results = AFrame.send_request(new_query)
        result_flatten = AFrame.attach_row_id(results)
        if all(i is None for i in result_flatten):
            raise KeyError(self._schema)
        # return pd.Series(results)
        json_str = json.dumps(result_flatten)
        result = pd.DataFrame(data=json.read_json(json_str))
        result.set_index('row_id', inplace=True)
        return result

    def __eq__(self, other):
        if isinstance(self, AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            old_query = self._query[:-1]
            new_query = 'with q as(%s) from q t select t.row_id, t.%s=%s %s;' % \
                   (old_query, self._schema, str(other), self._schema)
            self._query = new_query
            return AFrameObj(self._dataverse, self._dataset, self._schema, self._query)

    def __and__(self, other):
        if isinstance(self, AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            if isinstance(other, AFrameObj):
                left_q = self.query[:-1]
                right_q = other.query[:-1]
                new_q = 'with q1 as (%s), \n  q2 as(%s)\n ' \
                        'from q1 t, q2 t2 where t.row_id=t2.row_id ' \
                        'select t.row_id, t.%s and t2.%s as result;' \
                        % (left_q, right_q, self.schema, other.schema)
                return AFrameObj(self._dataverse, self._dataset, 'result', new_q)

    # def __getitem__(self, key):
    #     if isinstance(key, AFrameObj):
    #         return AFrameObj(key.schema, key.query)
    #     if self._columns:
    #         dataset = self._dataverse + '.' + self._dataset
    #         query = 'select value %s from %s;' % (key, dataset)
    #         for col in self._columns:
    #             if col['name'] == key:
    #                 query = 'select value %s from %s;' % (key, dataset)
    #                 return AFrameObj(col, query)
    #         return AFrameObj(key, query)

    def toAframe(self):
        dataverse, dataset = self.get_dataverse()
        return AFrame(dataverse, dataset)

    def get_dataverse(self):
        sub_query = self.query.split("from")
        data = sub_query[1][1:].split(".",1)
        dataverse = data[0]
        dataset = data[1].split(" ")[0]
        return (dataverse, dataset)

class AFrame:

    def __init__(self, dataverse, dataset, columns=[], path=None):
        # if dataset doesn't exist -> create it
        # else load in its definition
        self._dataverse = dataverse
        self._dataset = dataset+'_c'
        self._columns = columns
        self._datatype = None
        self._datatype_name = None
        self._info = dict()
        #initialize
        self.get_dataset()

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        if isinstance(key, AFrameObj):
            old_query = key.query[:-1]
            new_query = 'with q as('+old_query+')\n' \
                                               'from q t1 LEFT OUTER JOIN %s.%s t on t.row_id=t1.row_id ' \
                                               'where t1.%s ' \
                                               'select value t;' % (self._dataverse, self._dataset, key.schema)

            return AFrameObj(self._dataverse, self._dataset, key.schema, new_query)
        if self._columns:
            dataset = self._dataverse + '.' + self._dataset
            # query = 'select value t.%s from %s t;' % (key, dataset)
            query = 'from %s t select t.row_id, t.data.%s;' % (dataset, key)
            # for col in self._columns:
            #     if col['name'] == key:
            #         query = 'select %s from %s;' % (key, dataset)
            #         return AFrameObj(self._dataverse, self._dataset, col, query)
            return AFrameObj(self._dataverse, self._dataset, key, query)

    def __len__(self):
        dataset = self._dataverse+'.'+self._dataset
        query = 'select value count(*) from %s;' % dataset
        result = self.send_request(query)[0]
        self._info['count'] = result
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
                query = 'select value t from %s limit %d;' % (dataset, sample)
            else:
                query = 'select value t from %s t;' % dataset
            result = self.send_request(query)
            results = AFrame.attach_row_id(result)
            df = pd.DataFrame(json.read_json(json.dumps(results)))
            df.set_index('row_id', inplace=True)
            return df

    @staticmethod
    def attach_row_id(result_lst):
        if len(result_lst) > 0 and len(result_lst[0]) == 2 and 'row_id' in result_lst[0] and 'data' in result_lst[0]:
            flatten_results = []
            for i in result_lst:
                i['data']['row_id'] = i['row_id']
                flatten_results.append(i['data'])
            return flatten_results
        return result_lst


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

    def get_dataset(self):
        query = 'select value dt from Metadata.`Datatype` dt ' \
                'where dt. DataverseName = \'%s\';' % self._dataverse

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
            self._columns.append(column)

    @staticmethod
    def send_request(query: str):
        host = 'http://localhost:19002/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            result = json.loads(handler.read())
            return result['results']
