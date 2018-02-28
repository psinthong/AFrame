import pandas as pd
import urllib.parse
import urllib.request
import pandas.io.json as json
from aframeObj import AFrameObj


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
        if isinstance(key, slice):
            step = 1
            start = 0
            stop = self.__len__()
            new_query = ''
            if key.step:
                step = key.step
            if key.start:
                start = key.start
            if key.stop:
                stop=key.stop

            dataset = self._dataverse + '.' + self._dataset
            row_ids = []
            if step != 1:
                for i in range(start, stop, step):
                    row_ids.append(i)
                new_query = 'select value t from %s t where t.row_id in %s;' % (dataset, row_ids)
            else:
                new_query = 'select value t from %s t where t.row_id >= %d and t.row_id < %d;' % (dataset, start, stop)

            return AFrameObj(self._dataverse, self._dataset, None, new_query)

        if self._columns:
            dataset = self._dataverse + '.' + self._dataset
            # query = 'select value t.%s from %s t;' % (key, dataset)
            query = 'from %s t select t.row_id, t.data.%s;' % (dataset, key)
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
