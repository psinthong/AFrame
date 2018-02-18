import pandas as pd
import urllib.parse
import urllib.request
import pandas.io.json as json
import re

class AFrameObj:
    def __init__(self, schema, query=None):
        self._schema = schema
        self._query = query
        self._data = None

    def __str__(self):
        return 'Column: '+str(self._schema)

    @property
    def schema(self):
        return self._schema

    @property
    def query(self):
        return self._query

    def collect(self):
        results = AFrame.send_request(self._query)
        if all(i is None for i in results):
            raise KeyError(self._schema)
        self._data = results
        return pd.Series(self._data)

    def head(self, num=5):
        new_query = self._query[:-1]+' limit %d;' % num
        results = AFrame.send_request(new_query)
        if all(i is None for i in results):
            raise KeyError(self._schema)
        return pd.Series(results)

    def __eq__(self, other):
        if isinstance(self, AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            tmp = re.sub(str(self._schema), "t", str(self._query))
            new_query = tmp[:-1] + ' t where %s = %s;' % (self._schema, other)
            self._query = new_query
            # print(new_query)
            return AFrameObj(self._schema, self._query)

    def __and__(self, other):
        if isinstance(self, AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            if isinstance(other, AFrameObj):
                sub_query = other.query.split("where")
                new_query = self.query[:-1] + ' and' + sub_query[1]
                self._query = new_query
                return AFrameObj(self._schema, self._query)

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
        self._dataset = dataset
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
            return AFrameObj(key.schema, key.query)
        if self._columns:
            dataset = self._dataverse + '.' + self._dataset
            query = 'select value t.%s from %s t;' % (key, dataset)
            for col in self._columns:
                if col['name'] == key:
                    query = 'select value %s from %s;' % (key, dataset)
                    return AFrameObj(col, query)
            return AFrameObj(key, query)

    def __len__(self):
        dataset = self._dataverse+'.'+self._dataset
        query = 'select value count(*) from %s;' % dataset
        result = self.send_request(query)[0]
        self._info['count'] = result
        return result

    def __str__(self):
        if self._columns:
            txt = 'AsterixDB DataFrame with the following \'%s\' columns: \n\t' % self._datatype
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
            query = 'select value t ' \
                    'from %s t limit %d;' % (dataset, sample)
            result = self.send_request(query)
            return pd.DataFrame(json.read_json(json.dumps(result)))


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
