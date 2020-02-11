import pandas as pd
import numpy as np
import urllib.parse
import urllib.request
import urllib.error
import pandas.io.json as json
from aframe.groupby import AFrameGroupBy
from pandas.io.json import json_normalize
from aframe.window import Window
from aframe.connector import Connector
from aframe.connector import AsterixConnector
import configparser
import os
import re

class AFrame:

    def __init__(self, dataverse, dataset, schema=None, query=None, predicate=None, is_view=False, con=Connector()):
        # load in dataset definition
        self._dataverse = dataverse
        self._dataset = dataset
        self._columns = None
        self._datatype = None
        self._datatype_name = None
        self._info = dict()
        self._is_view = is_view
        self._connector = con

        # initialize
        con.get_collection(dataverse=dataverse,dataset=dataset)
        self._config_queries = con.get_config_queries()
        if not is_view and isinstance(con, AsterixConnector):
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

    @property
    def config_queries(self):
        return self._config_queries

    def get_initial_query(self):
        dataset = self._dataverse + '.' + self._dataset
        if self._is_view:
            return '{}();'.format(dataset)
        else:
            init_query = self.config_queries['q1']
            init_query = AFrame.rewrite(query=init_query, namespace=self._dataverse, collection=self._dataset)
            return init_query

    @staticmethod
    def rewrite(query, **kwargs):
        for key, value in kwargs.items():
            query = query.replace('$'+key, value)
        return query

    @staticmethod
    def get_server_address(file_path):
        config = configparser.ConfigParser()
        config.read(file_path)
        return config['SERVER']['address']

    @staticmethod
    def concat_statements(attr_format, attr_separator, values):
        if not isinstance(values, list):
            raise ValueError("Expecting a list of attribute values")
        if len(values) == 1:
            return AFrame.rewrite(attr_format, attribute=values[0])
        else:
            condition = ''
            for i in range(len(values) - 1):
                left = condition if len(condition) > 0 else AFrame.rewrite(attr_format, attribute=values[i])
                right = AFrame.rewrite(attr_format, attribute=values[i + 1])
                condition = AFrame.rewrite(attr_separator, left=left, right=right)
            return condition

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, key):
        if isinstance(key, AFrame):
            new_query = self.config_queries['q3']
            new_query = AFrame.rewrite(new_query, subquery=self.query, statement=key.schema)
            return AFrame(self._dataverse, self._dataset, key.schema, new_query, is_view=self._is_view, con=self._connector)

        if isinstance(key, str):
            attr = self.config_queries['attribute_project']
            attr = AFrame.rewrite(attr, attribute=key)
            query = self.config_queries['q2']
            query = AFrame.rewrite(query, attribute_value=attr, subquery=self.query)
            return AFrame(self._dataverse, self._dataset, key, query,is_view=self._is_view, con=self._connector)

        if isinstance(key, (np.ndarray, list)):
            query = self.config_queries['q2']
            attr_format = self.config_queries['attribute_project']
            attr_separator = self._config_queries['attribute_separator']
            attributes = self.concat_statements(attr_format, attr_separator, key)

            query = self.rewrite(query, attribute_value=attributes, alias='', subquery=self.query)
            return AFrame(self._dataverse, self._dataset, attributes, query, is_view=self._is_view, con=self._connector)

    def __setitem__(self, key, value):
        dataset = self._dataverse + '.' + self._dataset
        if not isinstance(key, str):
            raise ValueError('Must provide a string name for the appended column.')
        # if isinstance(value, OrderedAFrame):
        #     new_query = 'SELECT t.*, %s %s FROM %s t;' % (value._columns, key, dataset)
        #     self.query = new_query
        new_query = self.config_queries['q9']
        new_query = self.rewrite(new_query, statement=value.schema, alias=key, subquery=self.query)
        schema = value.schema
        if self._schema is not None:
            if isinstance(self._schema, list):
                self._schema.append(schema)
            elif isinstance(self._schema, str):
                self._schema = self._schema+','+schema
        else:
            self._schema = [schema]
        self.query = new_query

    def __len__(self):
        result = self.get_count()
        self._info['count'] = result
        return result

    def get_count(self):
        query = self.config_queries['q4']
        query = AFrame.rewrite(query, subquery=self.query)

        result = self.send_request(query).iloc[0]
        return int(result)

    def __str__(self):
        if self._columns:
            txt = 'AsterixDB DataFrame with the following pre-defined columns: \n\t'
            return txt + str(self._columns)
        else:
            return 'Empty AsterixDB DataFrame'

    def head(self, sample=5, query=False):
        limit_query = self.config_queries['limit']
        new_query = AFrame.rewrite(limit_query, num=str(sample), subquery=self.query)
        # new_query = self.query[:-1] + ' LIMIT %d;' % sample

        if query:
            return new_query

        result = self.send_request(new_query)
        if '_uuid' in result.columns:
            result.drop('_uuid', axis=1, inplace=True)
        return result

    def flatten(self):
        return NestedAFrame(self._dataverse, self._dataset, self.columns, self.query)

    @property
    def columns(self):
        return self._columns

    def toPandas(self, sample: int = 0):
        if sample > 0:
            query = '{} LIMIT {};'.format(self.query, sample)
        else:
            query = self.query
        result = self.send_request(query)
        if '_uuid' in result.columns:
            result.drop('_uuid', axis=1, inplace=True)
        return result


    def collect(self):
        results = self.send_request(self.query)
        if '_uuid' in results.columns:
            results.drop('_uuid', axis=1, inplace=True)
        return results

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
            return AFrame(self._dataverse, self._dataset, schema, new_query, con=self._connector)
        # if not isinstance(col, AFrameObj):
        #     raise ValueError('A column must be of type \'AFrameObj\'')
        if isinstance(col, AFrame) and not appended:
            schema = 'unnest(%s)' % col.schema
            new_query = 'SELECT VALUE e FROM (%s) t unnest t e;' % col.query
            return type(self)(self._dataverse, self._dataset, schema, new_query)
        elif isinstance(col, AFrame) and appended:
            if not name:
                raise ValueError('Must provide a string name for the appended column.')

            new_query = 'SELECT u %s, t.* FROM %s t unnest t.%s u;' % (name, dataset, col.schema)
            schema = col.schema
            return AFrame(self._dataverse, self._dataset, schema, new_query, con=self._connector)

    def withColumn(self, name, col):
        if not isinstance(name, str):
            raise ValueError('Must provide a string name for the appended column.')
        if not isinstance(col, AFrame):
            raise ValueError('A column must be of type \'AFrame\'')
        dataset = self._dataverse + '.' + self._dataset
        new_query = 'SELECT t.*, %s %s FROM %s t;' % (col.schema, name, dataset)
        schema = col.schema
        return AFrame(self._dataverse, self._dataset, schema, new_query, con=self._connector)

    def notna(self):
        query = 'SELECT VALUE t FROM (%s) AS t WHERE ' % self.query
        if isinstance(self.schema, (np.ndarray, list)):
            fields = self.schema
            fields_str = ''
            for field in fields:
                fields_str += '%s IS KNOWN AND ' % field
            fields_str = fields_str[:-4]
            query = query + fields_str + ';'
            schema = fields_str
        if isinstance(self.schema, str):
            field_str = '%s IS KNOWN' % self.schema
            query = query + field_str + ';'
            schema = field_str
        return AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=schema, query=query, is_view=self._is_view, con=self._connector)

    def isna(self):
        query = 'SELECT VALUE t FROM (%s) AS t WHERE ' % self.query
        schema = self.schema
        if isinstance(self.schema, (np.ndarray, list)):
            fields = self.schema
            fields_str = ''
            for field in fields:
                fields_str += '%s IS UNKNOWN AND ' % field
            fields_str = fields_str[:-4]
            query = query + fields_str + ';'
            schema = fields_str
        if isinstance(self.schema, str):
            field_str = '%s IS UNKNOWN' % self.schema
            query = query + field_str + ';'
            schema = field_str
        return AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=schema, query=query, is_view=self._is_view, con=self._connector)

    def isnull(self):
        return self.isna()

    def notnull(self):
        return self.notna()

    @staticmethod
    def get_column_count(other):
        if not isinstance(other, AFrame):
            raise ValueError('A column must be of type \'AFrameObj\'')
        if isinstance(other, AFrame):
            query = 'SELECT VALUE count(*) FROM (%s) t;' % other.query
            # print(query)
            return other.send_request(query).iloc[0]

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
        # config = configparser.ConfigParser()
        # config.read(self._config)
        # if config['SERVER']['asterixdb'] == 'True':
        query = 'SELECT VALUE dt FROM Metadata.`Dataset` ds, Metadata.`Datatype` dt ' \
                'WHERE ds.DatasetName = \"%s\" AND ds.DatatypeName = dt.DatatypeName ' \
                'AND dt. DataverseName = \"%s\";' % (dataset,dataverse)
        # print(query)
        result = self.send_request(query)

        if len(result) == 0:
            raise ValueError('Cannot find %s.%s' %(dataverse,dataset))
        else:
            result = result.iloc[0]

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
            # nullable = field['IsNullable']
            column = dict([(name, type)])
            # column = name
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

            l_dataset = '(%s)' % self.query
            r_dataset = '(%s)' % other.query
            if other.query is not None:
                r_dataset = '(%s)' % other.query

            if left_on != right_on:
                query = 'SELECT VALUE object_merge(%s,%s) '% (lsuffix, rsuffix) + 'FROM %s %s ' %(l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' %(r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
            else:
                query = 'SELECT %s.*,%s.* '% (lsuffix, rsuffix) + 'from %s %s ' % (l_dataset, lsuffix) +\
                        join_types[how] + ' %s %s on %s.%s /*+ indexnl */ = %s.%s;' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)
            schema = '%s %s ' % (l_dataset, lsuffix) + join_types[how] + \
                     ' %s %s on %s.%s=%s.%s' % (r_dataset, rsuffix, lsuffix, left_on, rsuffix, right_on)

            return AFrame(self._dataverse, self._dataset, schema, query, is_view=self._is_view, con=self._connector)

    def groupby(self, by):
        return AFrameGroupBy(self._dataverse, self._dataset, self.query, self._config_queries, self._connector, by)

    def apply(self, func, *args, **kwargs):
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
        schema = func + '(t' + args_str + ')'
        new_query = 'SELECT VALUE %s(t%s) FROM (%s) t;' % (func, args_str, self.query)
        return AFrame(self._dataverse, self._dataset, schema, new_query, is_view=self._is_view, con=self._connector)

    def sort_values(self, by, ascending=True):

        by_attrs = []
        if isinstance(by, str):
            by_attrs.append(by)
        elif isinstance(by, (np.ndarray, list)):
            by_attrs = by

        if ascending:
            sort_order = 'sort_asc_attr'
            new_query = self.config_queries['q6']
            attr_format = self.config_queries[sort_order]
            attr_separator = self._config_queries['attribute_separator']
            attributes = self.concat_statements(attr_format, attr_separator, by_attrs)
            new_query = AFrame.rewrite(new_query, subquery=self.query, sort_asc_attr=attributes)
        else:
            sort_order = 'sort_desc_attr'
            new_query = self.config_queries['q5']
            attr_format = self.config_queries[sort_order]
            attr_separator = self._config_queries['attribute_separator']
            attributes = self.concat_statements(attr_format, attr_separator, by_attrs)
            new_query = AFrame.rewrite(new_query, subquery=self.query, sort_desc_attr=attributes)

        schema = new_query
        return AFrame(self._dataverse, self._dataset, schema, new_query, con=self._connector)

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
        stats = self.send_request(query).iloc[0]

        std_query = 'SELECT '
        sqr_query = '(SELECT '
        for key in num_cols:
            attr_std = 'sqrt(avg(square.%s)) AS %s_std,' % (key, key)
            attr_sqr = 'power(%s - t.%s, 2) AS %s,' % (stats[key+'_mean'], key, key)
            sqr_query += attr_sqr
            std_query += attr_std
        std_query = std_query
        sqr_query = sqr_query
        std_query += ' FROM '
        std_query += sqr_query
        std_query += ' FROM %s t) square;' % dataset

        # contains standard deviation results of all numeric attributes
        stds = self.send_request(std_query).iloc[0]

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

    def get_bin_size(self, attr, bins):
        if isinstance(attr, AFrame):
            query = "SELECT VALUE " \
                    "duration_from_ms(" \
                    "to_bigint(" \
                    "ms_from_day_time_duration(" \
                    "duration_from_interval(" \
                    "interval(min(t), max(t))))/%d)) " \
                    "FROM (%s) t;" % (bins, attr.query)
            result = self.send_request(query).iloc[0]
            return result

    @staticmethod
    def is_datetime(text):
        if isinstance(text, str):
            if 'T' in text:
                return True
            return False

    def get_min_date(self,query):
        new_query = 'SELECT VALUE MIN(t) FROM (%s) t;' % query
        date_str = str(self.send_request(new_query).iloc[0])
        if AFrame.is_datetime(date_str):
            return 'datetime(\"%s\")' %date_str
        else:
            return 'date(\"%s\")' % date_str

    def format_datetime_bin(self, min_date, duration, bin_attribute='t'):
        start = "\n\tget_interval_start_datetime(" \
                "interval_bin(%s , %s, get_day_time_duration(duration(\"%s\"))))" %(bin_attribute, min_date,duration)
        end = "\n\tget_interval_end_datetime(" \
                "interval_bin(%s , %s, get_day_time_duration(duration(\"%s\"))))" % (bin_attribute, min_date,duration)
        bin_query = "OBJECT_MERGE({\"Start\":%s}, \n\t{\"End\":%s})" % (start, end)
        return bin_query

    def format_date_bin(self, min_date, duration, bin_attribute='t'):
        start = "\nget_interval_start_date(" \
                "interval_bin(%s , %s, get_day_time_duration(duration(\"%s\"))))" % (bin_attribute, min_date, duration)
        end = "\nget_interval_end_date(" \
              "interval_bin(%s , %s, get_day_time_duration(duration(\"%s\"))))" % (bin_attribute, min_date, duration)
        bin_query = "OBJECT_MERGE({\"Start\":%s}, {\"End\":%s})" % (start, end)
        return bin_query

    def drop(self, attrs, axis=1):
        if axis != 1:
            raise ValueError('drop() currently only supports dropping columns')
        if isinstance(attrs, str):
            remove_list = "\"%s\"" % attrs
        elif isinstance(attrs, list):
            attrs = ['\"%s\"'% i for i in attrs]
            sep = ','
            remove_list = sep.join(attrs)
        else:
            raise ValueError('drop() takes a list of column names')
        schema = 'OBJECT_REMOVE_FIELDS(t, [%s])' % remove_list
        new_query = 'SELECT VALUE OBJECT_REMOVE_FIELDS(t, [%s]) FROM (%s) t;' % (remove_list, self.query)
        return AFrame(self._dataverse, self._dataset, schema, new_query, con=self._connector)

    def unique(self, sample=0):
        if sample == 0:
            new_query = 'SELECT DISTINCT VALUE t FROM (%s) t;' % self.query
        elif sample > 0:
            new_query = 'SELECT DISTINCT VALUE t FROM (%s) t LIMIT %d;' % (self.query, sample)
        else:
            raise ValueError('Number of returning records must be > 0.')
        result = self.send_request(new_query)
        return result

    @staticmethod
    def get_dummies(af,columns=None):
        if isinstance(af, AFrame):
            if columns is None:
                cols = af.unique()
                conditions, schemas = AFrame.get_conditions(af, cols)
                query = 'SELECT %s FROM (%s) t;' % (conditions, af.query)
                return AFrame(af._dataverse, af._dataset, schemas, query, is_view=af._is_view, con=af._connector)
            else:
                if isinstance(columns,list):
                    conditions = ""
                    schemas = ""
                    for col in columns:
                        cols = af[col].unique()
                        cons_i, schemas_i = AFrame.get_conditions(af, cols, str(col))
                        conditions += cons_i+','
                        schemas += schemas_i+','
                    conditions = conditions[:-1]
                    schemas = schemas[:-1]
                    query = 'SELECT t.*, %s FROM (%s) t;' % (conditions, af.query)
                    return AFrame(af._dataverse, af._dataset, schemas, query, is_view=af._is_view, con=af._connector)

        else:
            raise ValueError("must be an AFrame object")

    @staticmethod
    def get_conditions(af, cols, prefix=None):
        conditions = ''
        schemas = ''
        for col in cols:
            col_name = str(col).replace(" ", "_").replace("/", "_").replace(",", "_").replace("-", "_")
            if isinstance(col, str):
                if prefix is None:
                    condition = "to_number(t = \'%s\') %s," % (col, col_name)
                    schema = "to_number(%s = \'%s\') %s," % (af.schema, col, col_name)
                else:
                    condition = "to_number(t.%s = \'%s\') %s," % (prefix, col, prefix+'_'+col_name)
                    schema = "to_number(%s = \'%s\') %s," % (prefix, col, prefix+'_'+col_name)
            elif isinstance(col, (float, int)):
                if prefix is None:
                    condition = "to_number(t = {}) `{}`,".format(col, col_name)
                    schema = "to_number({} = {}) `{}`,".format(af.schema, col, col_name)
                else:
                    condition = "to_number(t.{} = {}) `{}`,".format(prefix, col, prefix+'_'+col_name)
                    schema = "to_number({} = {}) `{}`,".format(prefix, col, prefix+'_'+col_name)
            conditions += condition
            schemas += schema
        conditions = conditions[:-1]
        schemas = schemas[:-1]
        return conditions, schemas

    @staticmethod
    def concat(objs, axis=0):
        if axis == 0:
            raise NotImplementedError('Currently only supports concatenating columns (axis=1)')
        if isinstance(objs, list):
            first_obj = objs[0]
            conditions = ''
            for obj in objs[1:]:
                if isinstance(obj, AFrame):
                    condition = '%s,' % obj.schema
                    conditions += condition
                else:
                    raise ValueError("list elements must be AFrame objects")
            conditions = conditions[:-1]
            if isinstance(first_obj,AFrame):
                query = 'SELECT t.*, %s FROM (%s) t;' %(conditions, first_obj.query)
                return AFrame(first_obj._dataverse, first_obj._dataset, 't.*,%s' % conditions, query, is_view=first_obj._is_view, con=first_obj._connector)

    @staticmethod
    def cut_date(af,bins):
        if not isinstance(af, AFrame):
            raise ValueError('Input data has to be an AFrame object')
        else:
            if not isinstance(bins, int):
                raise ValueError('Input bins has to be an integer')
            else:
                min_date = af.get_min_date(af.query)
                bin_size = af.get_bin_size(af, bins)

                if 'datetime' in min_date:
                    new_schema = af.format_datetime_bin(min_date, bin_size, af.schema)
                    bin_query = af.format_datetime_bin(min_date, bin_size)
                else:
                    new_schema = af.format_date_bin(min_date, bin_size, af.schema)
                    bin_query = af.format_date_bin(min_date, bin_size)

                new_query = 'SELECT VALUE \n' \
                            '%s \n' \
                            'FROM (%s) t;' % (bin_query, af.query)
                return AFrame(af._dataverse, af._dataset, new_schema, new_query, con=af._connector)



    @staticmethod
    def cut(af, bins, labels=None):
        use_label = False
        if isinstance(labels, list):
            if isinstance(bins, int) and len(labels) != bins:
                raise ValueError('insufficient labels')
            elif isinstance(bins, list) and len(labels) != len(bins) - 1:
                raise ValueError('insufficient labels')
            else:
                use_label = True
        if not isinstance(af, AFrame):
            raise ValueError('Input data has to be an AFrame object')
        else:
            new_query = 'SELECT VALUE CASE\n'
            schema = 'CASE\n'
            if isinstance(bins, int):
                data_min = af.min()
                data_max = af.max()
                bin_size = (data_max-data_min)/bins
                new_min = data_min - ((data_max-data_min)*0.1/100)
                new_max = data_max + ((data_max-data_min)*0.1/100)

                lower_bound = new_min
                for i in range(bins):
                    upper_bound = round(lower_bound+bin_size,3)

                    if i+1 == bins:
                        if use_label:
                            label = str(labels[i])
                        else:
                            label = '(%s,%s]' % (str(lower_bound), str(float(data_max)))
                        case = 'WHEN (%s<t and t<=%s) THEN \"%s\"\n' % (str(lower_bound), str(float(data_max)), label)
                        case_schema = 'WHEN (%s<t.%s and t.%s<=%s) THEN \"%s\"\n' % (
                            str(lower_bound), af.schema, af.schema, str(float(data_max)), label)
                    else:
                        if use_label:
                            label = str(labels[i])
                        else:
                            label = '(%s,%s]' % (str(lower_bound),str(upper_bound))
                        case = 'WHEN (%s<t and t<=%s) THEN \"%s\"\n' %(str(lower_bound),str(upper_bound), label)
                        case_schema = 'WHEN (%s<t.%s and t.%s<=%s) THEN \"%s\"\n' % (
                            str(lower_bound), af.schema, af.schema, str(upper_bound), label)
                    new_query += case
                    schema += case_schema
                    lower_bound = upper_bound
                new_query += 'END FROM (%s) t;' %af.query
                schema += 'END'
                return AFrame(af._dataverse, af._dataset, schema,query=new_query, con=af._connector)
            if isinstance(bins, list) & len(bins) > 0:
                lower_bound = bins[0] # first element
                for i in range(len(bins)-1):
                    upper_bound = bins[i+1]
                    if use_label:
                        label = str(labels[i])
                    else:
                        label = '(%s,%s]' % (str(lower_bound),str(upper_bound))
                    case = 'WHEN (%s<t and t<=%s) THEN \"%s\"\n' %(str(lower_bound),str(upper_bound), label)
                    case_schema = 'WHEN (%s<t.%s and t.%s<=%s) THEN \"%s\"\n' % (
                        str(lower_bound), af.schema, af.schema, str(upper_bound), label)
                    new_query += case
                    schema += case_schema
                    lower_bound = upper_bound
                new_query += 'END FROM (%s) t;' % af.query
                schema += 'END'
                return AFrame(af._dataverse, af._dataset, schema, query=new_query, con=af._connector)


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

    def add(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, 'add')

    def sub(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, 'sub')

    def div(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, 'div')

    def mul(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, 'mul')

    def mod(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, 'mod')

    def pow(self, value):
        if not isinstance(value, int) and not isinstance(value, float):
            raise ValueError('parameter must be numerical')
        return self.arithmetic_op(value, 'pow')

    def arithmetic_op(self, value, op):
        # new_query = 'SELECT VALUE t %s %s FROM (%s) t;' % (op, str(value), self.query)
        arithmetic_statement = self.config_queries[op]
        condition = AFrame.rewrite(arithmetic_statement, left=self.schema, right=str(value))
        new_query = self.config_queries['q2']
        col_alias = self.config_queries['attribute_value']
        new_query = self.rewrite(new_query, attribute_value=col_alias)

        escape_chars = self.config_queries['escape']
        alias = re.sub(escape_chars, '', str(condition))

        new_query = self.rewrite(new_query, subquery=self.query, attribute=condition, alias=alias)
        return AFrame(self._dataverse, self._dataset, condition, new_query, is_view=self._is_view, con=self._connector)

    def max(self):
        return self.agg_function('max')

    def min(self):
        return self.agg_function('min')

    def avg(self):
        return self.agg_function('avg')

    def count(self):
        return self.agg_function('count')

    def agg_function(self, func):
        new_query = 'SELECT VALUE %s(t) FROM (%s) t;' % (func, self.query)
        result = self.send_request(new_query)
        return result.iloc[0]

    def __eq__(self, other):
        return self.binary_opt(other, 'eq')

    def __ne__(self, other):
        return self.binary_opt(other, 'ne')

    def __gt__(self, other):
        return self.binary_opt(other, 'gt')

    def __lt__(self, other):
        return self.binary_opt(other, 'lt')

    def __ge__(self, other):
        return self.binary_opt(other, 'ge')

    def __le__(self, other):
        return self.binary_opt(other, 'le')

    def binary_opt(self, other, opt):
        comparison_statement = self.config_queries[opt]

        if type(other) == str:
            comparison = AFrame.rewrite(comparison_statement, left=self.schema, right="'"+other+"'")
        else:
            comparison = AFrame.rewrite(comparison_statement, left=self.schema, right=str(other))
        # query = 'SELECT VALUE %s FROM (%s) t;' %(selection, self.query)

        query = self.config_queries['q2']
        col_alias = self.config_queries['attribute_value']
        query = self.rewrite(query, attribute_value=col_alias)
        escape_chars = self.config_queries['escape']
        alias = re.sub(escape_chars, '', str(comparison))
        query = self.rewrite(query, alias=alias, attribute=comparison, subquery=self.query)
        return AFrame(self._dataverse, self._dataset, comparison, query, is_view=self._is_view, con=self._connector)

    def __and__(self, other):
        return self.boolean_op(other, 'and')

    def __or__(self, other):
        return self.boolean_op(other, 'or')

    def boolean_op(self, other, op):
        logical_statement = self.config_queries[op]
        logical_statement = AFrame.rewrite(logical_statement, left=self.schema, right=other.schema)

        new_query = self.config_queries['q2']
        col_alias = self.config_queries['attribute_value']
        new_query = AFrame.rewrite(new_query, attribute_value=col_alias)
        escape_chars = self.config_queries['escape']
        alias = re.sub(escape_chars, '', str(logical_statement))
        new_query = AFrame.rewrite(new_query, subquery=self.query, attribute=logical_statement, alias=alias)
        return AFrame(self._dataverse, self._dataset, logical_statement, new_query, is_view=self._is_view, con=self._connector)

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
        schema = '%s(%s%s)' % (func, self.schema, args_str)
        new_query = 'SELECT VALUE %s(t%s) FROM (%s) t;' % (func, args_str, self.query)
        return AFrame(self._dataverse, self._dataset, schema, new_query, None, con=self._connector)

    def persist(self, name=None, dataverse=None, is_view=False):
        if name is None:
            raise ValueError('Need to provide a name for the new dataset.')

        if is_view:
            function_name = '%s.%s' %(dataverse, name)
            function_query = 'CREATE FUNCTION %s(){%s};' % (function_name, self.query)
            self.send(function_query)
            new_q = 'SELECT VALUE t FROM (%s()) t;' % function_name
            return AFrame(dataverse=dataverse, dataset=name, query=new_q, is_view=True, con=self._connector)
        else:
            self.create_tmp_dataverse(dataverse)
            if dataverse:
                new_q = 'create dataset %s.%s(TempType) primary key _uuid autogenerated;' % (dataverse, name)
                new_q += '\n insert into %s.%s select value ((%s));' % (dataverse, name, self.query)
                self.send(new_q)
                return AFrame(dataverse, name, con=self._connector)
            else:
                new_q = 'create dataset _Temp.%s(TempType) primary key _uuid autogenerated;' % name
                new_q += '\n insert into _Temp.%s select value ((%s));' % (name, self.query)
                self.send(new_q)
                return AFrame('_Temp', name, con=self._connector)

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

        #-------------migrate AFrameObj methods

    def send_request(self, query: str):
        return self._connector.send_request(query)

    def send(self, query: str):
        host = self._connector.server_address+'/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            result = json.loads(handler.read())
            return result['status']

    @staticmethod
    def drop_dataset(af=None, dataverse=None, dataset=None):
        if isinstance(af, AFrame):
            if af._is_view:
                query = 'DROP FUNCTION %s.%s@0;' % (af._dataverse, af._dataset)
            else:
                query = 'DROP DATASET %s.%s;' % (af._dataverse, af._dataset)
            result = af.send(query)
            return result
        if (dataverse is not None) and (dataset is not None):
            if af._is_view:
                query = 'DROP FUNCTION %s.%s@0;' % (af._dataverse, af._dataset)
            else:
                query = 'DROP DATASET %s.%s;' % (dataverse, dataset)
            result = af.send(query)
            return result


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
            new_query = self._query+' LIMIT %d;' % sample
            results = self.send_request(new_query)
            norm_result = json_normalize(results)
        else:
            self._query = 'SELECT VALUE t FROM %s t;' % dataset
            new_query = self._query + ' LIMIT %d;' % sample
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
        dataset = '(%s)' % self.query
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
        dataset = '(%s)' % self.query
        over = self.get_window()
        columns = '%s() %s' % (func, over)
        query = 'SELECT VALUE %s FROM %s t;' % (columns, dataset)
        return OrderedAFrame(self._dataverse, self._dataset, columns, self.on, query, self._window)

    def validate_window_function_argument(self, func, expr, ignore_null):
        if not isinstance(expr,str):
            raise ValueError('expr for first_value must be string')
        dataset = '(%s)' % self.query
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
        dataset = '(%s)' % self.query
        over = self.get_window()
        columns = '%s(%s, %d) %s' % (func, expr, offset, over)
        if ignore_null:
            columns = '%s(%s,%d) IGNORE NULLS %s' % (func, expr, offset, over)
        query = 'SELECT VALUE %s FROM %s t;' % (columns, dataset)
        return OrderedAFrame(self._dataverse, self._dataset, columns, self.on, query, self._window)