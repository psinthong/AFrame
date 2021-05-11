import pandas as pd
import numpy as np
import urllib.parse
import urllib.request
import urllib.error
import pandas.io.json as json
from aframe.groupby import AFrameGroupBy
from aframe.window import Window
from aframe.connector import Connector
from aframe.connector import AsterixConnector
import configparser
import os
import re
import copy

class AFrame:

    def __init__(self, dataverse, dataset, schema=None, query=None, predicate=None, is_view=False, connector=None):
        # load in dataset definition
        self._dataverse = dataverse
        self._dataset = dataset
        self._columns = None
        self._datatype = None
        self._datatype_name = None
        self._info = dict()
        self._is_view = is_view
        self._connector = connector

        # initialize
        self._config_queries = connector.get_config_queries()

        # if not is_view and isinstance(connector, AsterixConnector):
        #     self.get_dataset(dataverse=dataverse,dataset=dataset)

        if query is not None and query != '':
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
        if self._is_view:
            # return '{}();'.format(dataset)
            return self._connector.get_view(dataverse=self._dataverse, dataset=self._dataset)
        else:
            self._connector.get_collection(dataverse=self._dataverse, dataset=self._dataset)
            init_query = self.config_queries['q1']
            init_query = AFrame.rewrite(query=init_query, namespace=self._dataverse, collection=self._dataset)
            return init_query

    @staticmethod
    def rewrite(query, **kwargs):
        for key, value in kwargs.items():
            query = query.replace('$'+key, str(value))
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
            return AFrame(self._dataverse, self._dataset, key.schema, new_query, is_view=self._is_view, connector=self._connector)

        if isinstance(key, str):
            attr = self.config_queries['attribute_project']
            attr = AFrame.rewrite(attr, attribute=key)

            if isinstance(self, NestedAFrame):
                nested_fields = self._nested_fields
                if '.' in key and key.split('.')[0] in nested_fields:
                    nested_attribute = key.split('.')[0]
                    alias = key.split('.')[1]
                    attr = self.config_queries['attribute_project_nested']
                    attr = AFrame.rewrite(attr, attribute=key, nested_attribute=nested_attribute, alias=alias)

            query = self.config_queries['q2']
            query = AFrame.rewrite(query, attribute_value=attr, subquery=self.query)
            return AFrame(self._dataverse, self._dataset, key, query, is_view=self._is_view, connector=self._connector)

        if isinstance(key, (np.ndarray, list)):
            query = self.config_queries['q2']
            attr_separator = self._config_queries['attribute_separator']

            if isinstance(self, NestedAFrame):
                nested_fields = self._nested_fields
                attributes = ''
                for k in key:
                    if '.' in str(k) and k.split('.')[0] in nested_fields:
                        nested_attribute = k.split('.')[0]
                        alias = k.split('.')[1]
                        attr_format = self.config_queries['attribute_project_nested']
                        attr = AFrame.rewrite(attr_format, attribute=k, nested_attribute=nested_attribute, alias=alias)
                    else:
                        attr_format = self.config_queries['attribute_project']
                        attr = AFrame.rewrite(attr_format, attribute=k)
                    if attributes == '':
                        attributes = self.rewrite(attr_format, attribute=attr)
                    else:
                        attributes = self.rewrite(attr_separator, left=attributes, right=attr)
            else:
                attr_format = self.config_queries['attribute_project']
                attributes = self.concat_statements(attr_format, attr_separator, key)

            query = self.rewrite(query, attribute_value=attributes, alias='', subquery=self.query)
            return AFrame(self._dataverse, self._dataset, key, query, is_view=self._is_view, connector=self._connector)

    def __setitem__(self, key, value):

        if not isinstance(key, str):
            raise ValueError('Must provide a string name for the appended column.')
        # if isinstance(value, OrderedAFrame):
        #     new_query = 'SELECT t.*, %s %s FROM %s t;' % (value._columns, key, dataset)
        #     self.query = new_query
        if not isinstance(value, AFrame):
            raise ValueError('Must provide an AFrame object as a value.')

        new_query = self.config_queries['q9']
        new_field_format = self.config_queries['attribute_value']
        attribute_format = self.config_queries['single_attribute']
        new_field_format = self.rewrite(new_field_format, attribute=attribute_format)
        new_field_format = self.rewrite(new_field_format, attribute=value.schema, alias=key)
        new_query = self.rewrite(new_query, attribute_value=new_field_format, subquery=self.query)

        if self._schema is not None:
            if isinstance(self._schema, list):
                self._schema.append(new_field_format)
            elif isinstance(self._schema, str):
                self._schema = [self._schema]
                self._schema.append(new_field_format)
        else:
            self._schema = [new_field_format]
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
        txt = 'AFrame: a DataFrame interface for databases'
        return txt

    def head(self, sample=5, query=False):
        limit_query = self.config_queries['limit']
        new_query = AFrame.rewrite(limit_query, num=str(sample), subquery=self.query)

        if query:
            return new_query

        result = self.send_request(new_query)
        if '_uuid' in result.columns:
            result.drop('_uuid', axis=1, inplace=True)
        return result

    def flatten(self, columns=None):
        return NestedAFrame(self._dataverse, self._dataset, self.columns, self.query, self._is_view, self._connector, columns)

    @property
    def columns(self):
        if self._columns:
            return self._columns
        else:
            n = self.config_queries['sample_size']
            samples = self.head(n)
            return samples.columns

    @property
    def dtypes(self):
        n = self.config_queries['sample_size']
        samples = self.head(n)
        return samples.dtypes

    @property
    def shape(self):
        n = self.config_queries['sample_size']
        samples = self.head(n)
        column_count = len(samples.columns)
        row_count = len(self)
        return (row_count, column_count)

    @property
    def ndim(self):
        return 2


    def toPandas(self, sample: int = 0):
        if sample > 0:
            return self.head(sample)
        query = self.config_queries['return_all']
        query = self.rewrite(query, subquery=self.query)
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

    def explode(self, column):
        # add exploded attribute as a new column
        new_query = self.config_queries['q18']
        new_column = column+'_alias'
        new_query = self.rewrite(new_query, subquery=self.query, attribute=column, alias=new_column)
        tmp_af = AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=self.schema, query=new_query,
                        is_view=self._is_view, connector=self._connector)

        # drop the original column
        drop_af = tmp_af.drop(attrs=column)

        # rename the exploded attribute to the original column name
        result = drop_af.rename({new_column: column})

        return result

    def withColumn(self, name, col):
        if not isinstance(name, str):
            raise ValueError('Must provide a string name for the appended column.')
        if not isinstance(col, AFrame):
            raise ValueError('A column must be of type \'AFrame\'')
        dataset = self._dataverse + '.' + self._dataset
        new_query = 'SELECT t.*, %s %s FROM %s t;' % (col.schema, name, dataset)
        schema = col.schema
        return AFrame(self._dataverse, self._dataset, schema, new_query, connector=self._connector)

    def notna(self, cols=None):
        comparison_statement = self.config_queries['notna']
        return_col_alias = 'notna'
        comparison_statements, new_query = self.check_na(cols, comparison_statement, return_col_alias)
        return AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=comparison_statements, query=new_query,
                      is_view=self._is_view, connector=self._connector)

    def isna(self, cols=None):
        comparison_statement = self.config_queries['isna']
        return_col_alias = 'isna'
        comparison_statements, new_query = self.check_na(cols, comparison_statement, return_col_alias)
        return AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=comparison_statements, query=new_query, is_view=self._is_view, connector=self._connector)

    def check_na(self, cols, comparison_statement, return_col_alias):
        query = self._config_queries['q2']
        col_alias = self.config_queries['attribute_value']
        attr_separator = self.config_queries['attribute_separator']
        logic_and = self.config_queries['and']
        if cols is None:
            if isinstance(self.schema, list):
                fields = self.schema
            elif isinstance(self.schema, str):
                fields = [self.schema]
            else:
                raise ValueError('Must provide either field(s) to check or pre-select the field(s)')
        else:
            if isinstance(cols, list):
                fields = cols
            else:
                fields = [cols]
        attribute_vals = ''
        comparison_statements = ''
        for field in fields:
            single_attribute = self.config_queries['single_attribute']
            format_field = self.rewrite(single_attribute, attribute=field)
            comparison = self.rewrite(comparison_statement, left=format_field)
            alias = '{}({})'.format(return_col_alias, field)
            statement = self.rewrite(col_alias, alias=alias, attribute=comparison)
            if attribute_vals == '':
                attribute_vals = statement
                comparison_statements = comparison
            else:
                attribute_vals = self.rewrite(attr_separator, left=attribute_vals, right=statement)
                comparison_statements = self.rewrite(logic_and, left=comparison_statements, right=comparison)
        new_query = self.rewrite(query, subquery=self.query, attribute_value=attribute_vals)
        return comparison_statements, new_query

    def isnull(self):
        return self.isna()

    def notnull(self):
        return self.notna()

    def dropna(self, cols=None, axis=1):
        if axis != 1:
            raise ValueError("Currently only support droping columns")
        return self[self.notna(cols=cols)]

    def fillna(self, values, cols=None):
        query = self.config_queries['q9']
        col_alias = self.config_queries['attribute_value']
        attr_separator = self.config_queries['attribute_separator']
        fillna_field_format = self.config_queries['fillna']
        str_format = self.config_queries['str_format']
        single_attribute = self.config_queries['single_attribute']
        attribute_vals = ''
        new_schema =[]
        if isinstance(values, dict):
            for key in values.keys():
                value = values[key]
                if isinstance(value,str):
                    value = self.rewrite(str_format, value=value)
                formatted_key = self.rewrite(single_attribute, attribute=key)
                fillna_field = self.rewrite(fillna_field_format, attribute=formatted_key, value=value)
                col_statement = self.rewrite(col_alias, attribute=fillna_field, alias=key)
                new_schema.append(col_statement)
                if attribute_vals == '':
                    attribute_vals = col_statement
                else:
                    attribute_vals = self.rewrite(attr_separator, left=attribute_vals, right=col_statement)
        else:
            if cols is None:
                if isinstance(self.schema, list):
                    fields = self.schema
                elif isinstance(self.schema, str):
                    fields = [self.schema]
                else:
                    raise ValueError('Must provide either field(s) to check or pre-select the field(s)')
            else:
                if isinstance(cols, list):
                    fields = cols
                else:
                    fields = [cols]
            value = values
            if isinstance(value, str):
                value = self.rewrite(str_format, value=value)
            for key in fields:
                formatted_key = self.rewrite(single_attribute, attribute=key)
                fillna_field = self.rewrite(fillna_field_format, attribute=formatted_key, value=value)
                col_statement = self.rewrite(col_alias, attribute=fillna_field, alias=key)
                new_schema.append(col_statement)
                if attribute_vals == '':
                    attribute_vals = col_statement
                else:
                    attribute_vals = self.rewrite(attr_separator, left=attribute_vals, right=col_statement)
        new_query = self.rewrite(query, attribute_value=attribute_vals, subquery=self.query)
        return AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=new_schema, query=new_query, is_view=self._is_view, connector=self._connector)

    def rename(self, mapper, axis='columns'):
        if axis != 'columns':
            raise ValueError('Only support renaming columns')
        tmp = copy.copy(self)
        if isinstance(mapper, dict):
            for key in mapper.keys():
                old_val = key
                new_val = mapper[key]

                new_query = self.config_queries['q9']
                new_field_format = self.config_queries['rename']
                single_attr = self.config_queries['single_attribute']
                new_field_format = self.rewrite(new_field_format, old_attribute=single_attr)
                new_field_format = self.rewrite(new_field_format, attribute=old_val, new_attribute=new_val)
                new_query = self.rewrite(new_query, attribute_value=new_field_format, subquery=tmp.query)

                if tmp._schema is not None:
                    if isinstance(tmp._schema, list):
                        tmp._schema.append(new_field_format)
                    elif isinstance(tmp._schema, str):
                        tmp._schema = [tmp._schema]
                        tmp._schema.append(new_field_format)
                else:
                    tmp._schema = [new_field_format]
                tmp.query = new_query
                tmp = tmp.drop(old_val)

            return tmp
        else:
            raise ValueError('mapper must be a dictionary')

    def replace(self, to_replace, value=None, columns=None):
        new_query = self.config_queries['q9']
        col_alias = self.config_queries['attribute_value']
        attr_separator = self.config_queries['attribute_separator']
        replace_field_format = self.config_queries['replace']
        str_format = self.config_queries['str_format']
        single_attribute = self.config_queries['single_attribute']
        eq = self.config_queries['eq']
        tmp_query=self.query
        new_schema = []
        drop_attrs = []
        if columns is not None or self._schema is not None:
            if columns is not None:
                if not isinstance(columns, list):
                    columns = [columns]
            if self._schema is not None:
                if not isinstance(self.schema, list):
                    columns = [self.schema]
                else:
                    columns = self.schema
            if value is not None:
                # df.replace(0, 5)
                if isinstance(value, str):
                    value = self.rewrite(str_format, value=value)
                to_replace_dict = {to_replace: value}
            else:
                if isinstance(to_replace, dict):
                    to_replace_dict = to_replace
                else:
                    raise ValueError('Must provide a dictionary for column and values to replace')
                # df.replace({0: 10, 1: 100})
            keys = to_replace_dict.keys()
            values = to_replace_dict.values()
            if set(keys) & set(values):
                raise ValueError("Replacement not allowed with overlapping keys and values")
            for to_replace_key in to_replace_dict.keys():
                replace_val = to_replace_dict[to_replace_key]
                for col in columns:
                    drop_attrs.append(col)
                    alias = col+'_alias'
                    formatted_key = self.rewrite(single_attribute, attribute=col)
                    if isinstance(to_replace_key, str):
                        to_replace_key = self.rewrite(str_format, value=to_replace_key)
                    if isinstance(replace_val, str):
                        replace_val = self.rewrite(str_format, value=replace_val)

                    eq_statement = self.rewrite(eq, left=formatted_key, right=str(to_replace_key))
                    replace_field = self.rewrite(replace_field_format, statement=eq_statement, attribute=formatted_key,
                                                 to_replace=str(replace_val))
                    col_statement = self.rewrite(col_alias, attribute=replace_field, alias=alias)
                    new_schema.append(col_statement)
                    tmp_query = self.rewrite(new_query, attribute_value=col_statement, subquery=tmp_query)
                    new_af = AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=self.schema,
                                    query=tmp_query, is_view=self._is_view, connector=self._connector)
                    new_af = new_af.drop(col)
                    new_af = new_af.rename({col + '_alias': col})
                    tmp_query = new_af.query

        else:
            attributes = ''
            if value is not None:
                if isinstance(value, str):
                    value = self.rewrite(str_format, value=value)
                if isinstance(to_replace, dict):
                    for to_replace_key in to_replace.keys():
                        drop_attrs.append(str(to_replace_key))
                        alias = str(to_replace_key) + '_alias'
                        replace_val = to_replace[to_replace_key]
                        if isinstance(replace_val,str):
                            replace_val = self.rewrite(str_format, value=replace_val)

                        formatted_key = self.rewrite(single_attribute, attribute=to_replace_key)
                        eq_statement = self.rewrite(eq, left=formatted_key, right=str(replace_val))
                        replace_field = self.rewrite(replace_field_format, statement=eq_statement,
                                                     attribute=formatted_key,
                                                     to_replace=str(value))
                        col_statement = self.rewrite(col_alias, attribute=replace_field, alias=alias)
                        new_schema.append(col_statement)
                        if attributes == '':
                            attributes = col_statement
                        else:
                            attributes = self.rewrite(attr_separator, left=attributes, right=col_statement)
                    tmp_query = self.rewrite(new_query, attribute_value=attributes, subquery=tmp_query)
                    new_af = AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=self.schema,
                                    query=tmp_query, is_view=self._is_view, connector=self._connector)
                    for attr in drop_attrs:
                        new_af = new_af.drop(attr)
                        new_af = new_af.rename({attr + '_alias': attr})
                else:
                    raise ValueError('Must provide a dictionary for column and values to replace')
                # df.replace({'A': 0, 'B': 5}, 100)
            else:
                if isinstance(to_replace, dict):
                    for to_replace_key in to_replace.keys():
                        replace_dict = to_replace[to_replace_key]
                        alias = str(to_replace_key) + '_alias'
                        if isinstance(replace_dict, dict):
                            keys = replace_dict.keys()
                            values = replace_dict.values()
                            if set(keys) & set(values):
                                raise ValueError("Replacement not allowed with overlapping keys and values")
                            for condition_key in keys:
                                replace_val = replace_dict[condition_key]
                                if isinstance(condition_key, str):
                                    condition_key = self.rewrite(str_format, value=condition_key)

                                if isinstance(replace_val, str):
                                    replace_val = self.rewrite(str_format, value=str(replace_val))

                                drop_attrs.append(str(to_replace_key))

                                formatted_key = self.rewrite(single_attribute, attribute=to_replace_key)
                                eq_statement = self.rewrite(eq, left=formatted_key, right=str(condition_key))
                                replace_field = self.rewrite(replace_field_format, statement=eq_statement,
                                                                 attribute=formatted_key,
                                                                 to_replace=str(replace_val))
                                col_statement = self.rewrite(col_alias, attribute=replace_field, alias=alias)
                                new_schema.append(col_statement)
                                tmp_query = self.rewrite(new_query, attribute_value=col_statement, subquery=tmp_query)
                                new_af = AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=self.schema,
                                                query=tmp_query, is_view=self._is_view, connector=self._connector)
                                new_af = new_af.drop(to_replace_key)
                                new_af = new_af.rename({alias: to_replace_key})
                                tmp_query = new_af.query
                        else:
                            raise ValueError('Must provide a dictionary for condition and value to be replaced')

                else:
                    raise ValueError('Must provide a dictionary for key and values to be replaced')
                # df.replace({'A': {0: 100, 4: 400}})

        return new_af

    def clip(self, lower=None, upper=None, columns=None):
        original_query = self.query
        project_query = self.config_queries['q2']
        attribute_value = self.config_queries['attribute_value']
        single_attribute = self.config_queries['single_attribute']
        replace_statement = self.config_queries['replace']

        attribute_value = self.rewrite(attribute_value, attribute=replace_statement)
        project_query = self.rewrite(project_query, attribute_value=attribute_value)

        le_format = self.config_queries['le']
        ge_format = self.config_queries['ge']

        if self.schema is not None:
            columns = self.schema if isinstance(self.schema, list) else [self.schema]
        else:
            if columns is None:
                raise ValueError('Must provide column names')

        new_query = ''
        for col in columns:
            formatted_key = self.rewrite(single_attribute, attribute=col)
            if lower is not None:
                le_statement = self.rewrite(le_format, left= formatted_key, right=lower)
                new_query =self.rewrite(project_query, subquery=original_query, alias=col, statement=le_statement,
                                        attribute=formatted_key, to_replace=lower)
                original_query = new_query
            if upper is not None:
                ge_statement = self.rewrite(ge_format, left= formatted_key, right=upper)
                new_query =self.rewrite(project_query, subquery=original_query, alias=col, statement=ge_statement,
                                        attribute=formatted_key, to_replace=upper)
                original_query = new_query
        return AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=columns, query=new_query,
                      is_view=self._is_view, connector=self._connector)

    def drop_duplicates(self, subset, keep='first'):

        grp_by_attr = self.config_queries['grp_by_attribute']
        attr_separator = self.config_queries['attribute_separator']
        grp_attrs = ''
        new_q = ''
        if not isinstance(subset, list):
            subset = [subset]

        if str(keep).lower() in ['first','last']:
            new_q = self.config_queries['q16']
        elif not keep:
            new_q = self.config_queries['q17']
        for attr in subset:
            if grp_attrs == '':
                grp_attrs = self.rewrite(grp_by_attr, attribute=attr)
            else:
                grp_attr = self.rewrite(grp_by_attr, attribute=attr)
                grp_attrs = self.rewrite(attr_separator, left=grp_attrs, right=grp_attr)
        new_q = self.rewrite(new_q, grp_by_attribute=grp_attrs, subquery= self.query)
        return AFrame(dataverse=self._dataverse, dataset=self._dataset, schema=self.schema,
                      query=new_q, is_view=self._is_view, connector=self._connector)


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
        query = 'SELECT VALUE ds FROM Metadata.`Dataset` ds' \
                ' WHERE ds.DatasetName = \"%s\" AND ds.DataverseName = \"%s\";' % (dataset,dataverse)
        # print(query)
        result = self.send_request(query)

        if len(result) == 0:
            raise ValueError('Cannot find %s.%s' %(dataverse,dataset))
        else:
            pass

    def merge(self, other, left_on=None, right_on=None, how='inner', l_alias='l', r_alias='r', hint=None):
        join_types = {'inner': 'q12', 'left': 'q13', 'inner_hint': 'q12_hint', 'left_hint': 'q13_hint'}
        if isinstance(other, AFrame):
            # if (left_on is None or right_on is None) and (hint is None):
            #     raise ValueError('Missing join columns')
            if how not in join_types:
                raise NotImplementedError('Join type specified is not yet available')

            if hint is not None:
                how = how+'_hint'
                left_on = left_on if left_on else ''
                right_on = right_on if right_on else ''
                new_query = self.config_queries[join_types[how]]
                new_query = self.rewrite(new_query, left_on=left_on, right_on=right_on, other=other._dataset, subquery=self.query,
                                         r_alias=r_alias, l_alias=l_alias, right_query=other.query.replace('\r\n',''), hint=hint)
            else:
                new_query = self.config_queries[join_types[how]]
                new_query = self.rewrite(new_query, left_on=left_on, right_on=right_on, other=other._dataset, subquery=self.query,
                                         r_alias=r_alias, l_alias=l_alias, right_query=other.query.replace('\r\n', ''))
            return AFrame(self._dataverse, self._dataset, self.schema, new_query, is_view=self._is_view, connector=self._connector)

    def groupby(self, by):
        return AFrameGroupBy(self._dataverse, self._dataset, self.query, self._config_queries, self._connector, by, self._is_view)

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
        return AFrame(self._dataverse, self._dataset, schema, new_query, connector=self._connector)

    @staticmethod
    def _return_element_list(item):
        if isinstance(item, list):
            return item
        else:
            return [item]

    def value_counts(self, subset=None, normalize=False, sort=True, ascending=False):
        if self.schema is None and subset is None:
            raise ValueError('Must supply column(s) to get value_counts on')
        cols = AFrame._return_element_list(subset) if subset else AFrame._return_element_list(self.schema)

        func = 'count'

        agg_query = self._config_queries['q8']
        agg_statement = self._config_queries['agg_value']
        grp_statement = self._config_queries['grp_value']
        grp_attr_format = self._config_queries['grp_by_attribute']
        attr_separator = self._config_queries['attribute_separator']
        grp_attributes = self.concat_statements(grp_attr_format, attr_separator, cols)
        grp_val_str = self.concat_statements(grp_statement, attr_separator, cols)

        func_format = self._config_queries[func]
        agg_func_format = agg_statement.replace('$agg_func_$attribute', func, 1)
        agg_func_format = self.rewrite(agg_func_format, func=func_format)

        agg_val_str = self.rewrite(agg_func_format, agg_func=func, attribute=cols[0])

        agg_query = self.rewrite(agg_query, subquery=self.query, grp_by_attribute=grp_attributes,
                                      agg_value=agg_val_str, grp_value=grp_val_str)
        return_all = self._config_queries['return_all']
        agg_query = self.rewrite(return_all, subquery=agg_query)

        tmp = AFrame(self._dataverse, self._dataset, self._schema, agg_query, is_view=self._is_view, connector=self._connector)

        if normalize:
            total_cnt = len(self)
            norm_col = tmp[func]/total_cnt
            tmp[func] = norm_col

        if sort:
            tmp = tmp.sort_values(by=func, ascending=ascending)
        return tmp

    def nunique(self, axis=0, query=False):
        if axis != 0:
            raise ValueError('This operation is not supported on rows')
        if not self.schema:
            raise ValueError('Select columns to get nunique values')
        unique_keys = self._return_element_list(self.schema)

        unique_format = self.config_queries['q10']
        results = dict()
        queries = []
        cnt = 0
        for key in unique_keys:
            unique = self.rewrite(unique_format, attribute=key, subquery=self.query)
            count_query = self.config_queries['q4']
            count_query = self.rewrite(count_query, subquery=unique)
            if query:
                cnt += 1
                if cnt != len(unique_keys):
                    queries.append(count_query)
                else:
                    queries.append(count_query)
                    return queries
            count = self.send_request(count_query).values[0][0]
            results[key] = count

        return pd.Series(results)

    def nlargest(self, n, columns, query=False):
        return self.sort_values(columns, ascending=False).head(n, query)

    def nsmallest(self, n, columns, query=False):
        return self.sort_values(columns, ascending=True).head(n, query)

    def describe(self, query=False):
        funcs = ['avg', 'std', 'min', 'max', 'count']
        data = []

        new_query = self._config_queries['q14']
        attribute_format = self._config_queries['agg_value']
        attr_separator = self._config_queries['attribute_separator']
        n = self.config_queries['sample_size']
        samples = self.head(n)

        if self.schema:
            if isinstance(self.schema,str):
                separator = self.rewrite(attr_separator, left='', right='').strip()
                selected_cols = [col.strip() for col in self.schema.split(separator)]
            elif isinstance(self.schema, list):
                selected_cols = self.schema
            cols = samples[selected_cols].select_dtypes([np.number]).columns.to_list()
        else:
            cols = samples.select_dtypes([np.number]).columns.to_list()

        all_func_str = ''
        for col in cols:
            for func in funcs:
                func_format = self._config_queries[func]
                func_alias = self.rewrite(attribute_format, func=func_format, agg_func=func, attribute=col)
                if all_func_str == '':
                    all_func_str = func_alias
                else:
                    left = all_func_str
                    right = func_alias
                    all_func_str = self.rewrite(attr_separator, left=left, right=right)

        new_query = self.rewrite(new_query, agg_value=all_func_str, subquery=self.query)
        return_all = self._config_queries['return_all']
        new_query = self.rewrite(return_all, subquery=new_query)

        if query:
            return new_query
        else:
            stats = self.send_request(new_query).iloc[0]
            for func in funcs:
                row_values = []
                for col in cols:
                    key = func+'_'+col
                    row_values.append(stats[key])
                data.append(row_values)
            res = pd.DataFrame(data, index=funcs, columns=cols)
            return res

    def create_str_attributes(self, columns, template):
        attr_separator = self._config_queries['attribute_separator']
        str_attributes = ''
        for col in columns:
            attr_format = self.rewrite(template, attribute=col)
            str_attributes = attr_format if str_attributes == '' else self.rewrite(attr_separator, left=str_attributes, right=attr_format)
        return str_attributes

    def diff(self, subset=None, inplace=True, order_by=None):
        tmp = AFrame(self._dataverse, self._dataset, copy.deepcopy(self.schema), copy.deepcopy(self.query),
                     is_view=copy.deepcopy(self._is_view),
                     connector=self._connector)
        if subset:
            columns = self._return_element_list(subset)
        else:
            if not self.schema:
                raise ValueError('Must provide a subset of column names')
            columns = self._return_element_list(self.schema)
        col_name = 'org_{}'

        diff_cols = ''

        new_query = self._config_queries['diff']
        attribute_name = self._config_queries['attribute_name']
        attr_separator = self._config_queries['attribute_separator']

        if 'window_order_by' in self._config_queries and order_by is None:
            raise ValueError('order_by is required when using a window function')
        else:
            order_by_list = self._return_element_list(order_by)
            order_by_format = self._config_queries['window_order_by']
            order_by_formatted = self.create_str_attributes(order_by_list, order_by_format)

        for col in columns:
            tmp[col_name.format(col)] = tmp[col]
            diff_col = self.rewrite(attribute_name, attribute=col)
            diff_cols = diff_col if diff_cols == '' else self.rewrite(attr_separator, left=diff_cols, right=diff_col)

        if not inplace:
            new_query = self.rewrite(new_query, attribute_name=diff_cols, window_order_by=order_by_formatted, subquery=tmp.query)
            tmp = AFrame(tmp._dataverse, tmp._dataset, tmp.schema, new_query, connector=tmp._connector)

            for col in columns:
                tmp['diff_{}'.format(col)] = tmp[col]
                tmp[col] = tmp[col_name.format(col)]
                tmp = tmp.drop(col_name.format(col))
            return tmp

        new_query = self.rewrite(new_query, attribute_name=diff_cols, window_order_by=order_by_formatted, subquery=self.query)

        return AFrame(self._dataverse, self._dataset, self.schema, new_query, connector=self._connector)

    def tail(self, sample=5, query=False):
        tail_query = self.config_queries['tail']
        new_query = AFrame.rewrite(tail_query, num=str(sample), subquery=self.query)

        if query:
            return new_query

        result = self.send_request(new_query)
        if '_uuid' in result.columns:
            result.drop('_uuid', axis=1, inplace=True)
        return result

    def rolling(self, window=None, on=None):
        if isinstance(window, int):
            window = Window(ord=on, rows=(-1*(window-1), 0))
        elif isinstance(window, str):
            window = Window(ord=on, rows=window)
        elif window is None:
            window = Window(ord=on)
        if on is None:
            raise ValueError('Must provide \'on\'')
        else:
            return RollingAFrame(self._dataverse, self._dataset, self._schema, self._connector, self._columns, on, self.query, window)
            # return AFrame(self._dataverse, self._dataset, self._columns, on, self.query, window)

    def expanding(self, on=None):
        total_count = len(self)
        return self.rolling(total_count, on=on)

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
        remove_list = ''
        attr_format = self.config_queries['attribute_remove']
        if isinstance(attrs, str):
            attr_format = self.rewrite(attr_format, attribute=attrs)
            remove_list = attr_format
        elif isinstance(attrs, list):
            new_af = AFrame(self._dataverse, self._dataset, self.schema, self.query, connector=self._connector)
            for attr in attrs:
                new_af = new_af.drop(attr)
            return new_af
            # attr_separator = self.config_queries['attribute_separator']
            # remove_list = self.concat_statements(attr_format, attr_separator, attrs)
        else:
            raise ValueError('drop() takes a list of column names')
        # schema = 'OBJECT_REMOVE_FIELDS(t, [%s])' % remove_list
        new_query = self.config_queries['q11']
        new_query = self.rewrite(new_query, attribute_remove=remove_list, subquery=self.query)
        return AFrame(self._dataverse, self._dataset, self.schema, new_query, connector=self._connector)

    def pop(self, item):
        if not isinstance(item, str):
            raise ValueError('name of column to pop must be string')

        pop_df = self[item]
        new_df = self.drop(item)
        self.query = new_df.query
        self._schema = new_df._schema
        return pop_df


    def astype(self, type_name, columns=None):
        new_type_cols = ''
        if not columns:
            if isinstance(self.schema, list):
                columns = self.schema
            else:
                columns = [self.schema]
        for col in columns:
                escape_chars = self.config_queries['escape']
                format_col = re.sub(escape_chars, '', str(col))
                single_attribute = self.config_queries['single_attribute']
                attr = self.rewrite(single_attribute, attribute=format_col)
               #cast to specified type
                new_type = self.config_queries['to_'+type_name]
                new_type = self.rewrite(new_type, statement=col)
                cast_type_format = self.config_queries['to_{}_field'.format(type_name)]
                cast_type_format = self.rewrite(cast_type_format, attribute=attr)

                attr_format = self.config_queries['attribute_value']
                attr_format = self.rewrite(attr_format, attribute=cast_type_format, alias=format_col)

                new_type_cols = attr_format

        new_query = self.config_queries['q2']
        new_query = self.rewrite(new_query, attribute_value=new_type_cols, subquery=self.query)
        return AFrame(self._dataverse, self._dataset, new_type, new_query, is_view=self._is_view, connector=self._connector)

    def unique(self, sample=0, query=False):
        unique_key = self.schema
        # single_attr_format = self.config_queries['single_attribute']
        # single_attr_format = self.rewrite(single_attr_format, attribute=unique_key)

        new_query = self.config_queries['q10']
        new_query = self.rewrite(new_query, attribute=unique_key, subquery=self.query)
        if sample > 0:
            new_query = self.config_queries['limit']
            new_query = self.rewrite(new_query, num=sample, subquery=self.query)
        if query:
            return new_query
        result = self.send_request(new_query)
        result = result.values.flatten().tolist()
        return result

    @staticmethod
    def get_dummies(af, prefix=False):
        added_cols = []
        tmp_af = copy.copy(af)
        if isinstance(tmp_af, AFrame):
            encoded_col = af.schema
            cols = tmp_af.unique()
            for col in cols:
                is_col = af == col
                cast_col = is_col.astype('int32', columns=[is_col.schema])
                if prefix:
                    col = encoded_col + '_' + str(col)
                tmp_af[str(col)] = cast_col
                if isinstance(tmp_af.schema, list):
                    added_schema = tmp_af.schema[-1]
                elif isinstance(tmp_af.schema, str):
                    added_schema = tmp_af.schema.split(',')[-1]
                added_cols.append(added_schema)
            if 'q11' in af.config_queries:
                tmp_af = tmp_af.drop(encoded_col)
        else:
            raise ValueError("must be an AFrame object")

        tmp_af._schema = added_cols
        return tmp_af

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
            appended_cols = ''
            new_schema = []
            for obj in objs[1:]:
                if isinstance(obj, AFrame):
                    if isinstance(obj.schema, list):
                        new_schema.extend(obj.schema)

                        if len(obj.schema) == 1:
                            appended_cols += obj.schema[0]
                        else:
                            attributes = obj.schema
                            # num_attributes = len(obj.schema)
                            if appended_cols == '':
                                appended_cols = obj.schema[0]
                                attributes = obj.schema[1:]
                                # num_attributes -= 1

                            for i in range(len(attributes)):
                                format_col = obj.config_queries['attribute_separator']
                                # if appended_cols == '':
                                #     left = obj.schema[i]
                                # else:
                                #     left = appended_cols
                                # right = obj.schema[i+1]
                                left = appended_cols
                                right = attributes[i]

                                appended_cols = obj.rewrite(format_col, left=left, right=right)
                    else:
                        print()

                else:
                    raise ValueError("list elements must be AFrame objects")

            if isinstance(first_obj,AFrame):
                if isinstance(first_obj.schema, list):
                    new_schema.extend(first_obj.schema)
                elif first_obj.schema is not None:
                    new_schema.append(first_obj.schema)

                new_query = first_obj.config_queries['q9']
                new_query = first_obj.rewrite(new_query, attribute_value=appended_cols, subquery=first_obj.query)
                return AFrame(first_obj._dataverse, first_obj._dataset, new_schema, new_query, is_view=first_obj._is_view, connector=first_obj._connector)

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
                return AFrame(af._dataverse, af._dataset, new_schema, new_query, connector=af._connector)



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
                return AFrame(af._dataverse, af._dataset, schema, query=new_query, connector=af._connector)
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
                return AFrame(af._dataverse, af._dataset, schema, query=new_query, connector=af._connector)


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

    def __radd__(self, other):
        return self.radd(other)

    def __rmul__(self, other):
        return self.rmul(other)

    def __rsub__(self, other):
        return self.rsub(other)

    def __rdiv__(self, other):
        return self.rdiv(other)

    def __rmod__(self, other):
        return self.rmod(other)

    def add(self, value):
        return self.arithmetic_op(value, 'add')

    def sub(self, value, reverse=False):
        return self.arithmetic_op(value, 'sub', reverse)

    def div(self, value, reverse=False):
        return self.arithmetic_op(value, 'div', reverse)

    def mul(self, value):
        return self.arithmetic_op(value, 'mul')

    def mod(self, value, reverse=False):
        return self.arithmetic_op(value, 'mod', reverse)

    def pow(self, value, reverse=False):
        return self.arithmetic_op(value, 'pow', reverse)

    def floordiv(self, value, reverse=False):
        return self.div(int(value), reverse)

    def truediv(self, value, reverse=False):
        return self.div(value, reverse)

    radd = add
    rmul = mul

    def rdiv(self, other):
        return self.div(other, reverse=True)

    def rsub(self, other):
        return self.sub(other, reverse=True)

    def rpow(self, other):
        return self.pow(other, reverse=True)

    def rmod(self, other):
        return self.mod(other, reverse=True)

    def rtruediv(self, value):
        return self.div(value, True)

    def rfloordiv(self, value):
        return self.floordiv(value, True)

    def arithmetic_op(self, value, op, reverse=False):
        original_query = copy.deepcopy(self.query)
        single_attr_format = self.config_queries['single_attribute']
        attr_sep_format = self.config_queries['attribute_separator']
        attr_project_format = self.config_queries['attribute_project']

        if not isinstance(value, int) and not isinstance(value, float):
            if isinstance(value, AFrame):
                original_proj_attr = self.rewrite(attr_project_format, attribute=self.schema)
                added_attr_formatted = self.rewrite(attr_project_format, attribute=value.schema)
                added_attribute = self.rewrite(attr_sep_format, left=original_proj_attr, right=added_attr_formatted)
                self.query = self.query.replace(original_proj_attr, added_attribute, 1)
                value = self.rewrite(single_attr_format, attribute=value.schema)
            else:
                raise ValueError('parameter must be numerical')

        single_attr_format = self.rewrite(single_attr_format, attribute=self.schema)

        arithmetic_statement = self.config_queries[op]
        if not reverse:
            condition = AFrame.rewrite(arithmetic_statement, left=single_attr_format, right=str(value))
        else:
            condition = AFrame.rewrite(arithmetic_statement, left=str(value), right=single_attr_format)

        new_query = self.config_queries['q2']
        col_alias = self.config_queries['attribute_value']
        new_query = self.rewrite(new_query, attribute_value=col_alias)

        escape_chars = self.config_queries['escape']
        alias = re.sub(escape_chars, '', str(condition))

        new_query = self.rewrite(new_query, subquery=copy.deepcopy(self.query), attribute=condition, alias=alias)
        self.query = original_query
        return AFrame(self._dataverse, self._dataset, condition, new_query, is_view=self._is_view, connector=self._connector)

    def max(self, query=False):
        return self.agg_function('max', query)

    def min(self, query=False):
        return self.agg_function('min', query)

    def avg(self, query=False):
        return self.agg_function('avg', query)

    def count(self, query=False):
        return self.agg_function('count', query)

    def sum(self, query=False):
        return self.agg_function('sum', query)

    def std(self, query=False):
        return self.agg_function('std', query)

    def var(self, query=False):
        return self.agg_function('var', query)

    mean=avg

    def agg_function(self, func, query=False):
        if self.schema is None:
            raise ValueError('Require to select at least one attribute')

        query_templat = self._config_queries['q14']
        attribute_format = self._config_queries['agg_value']
        attribute_separator = self.config_queries['attribute_separator']
        return_all = self._config_queries['return_all']
        func_format = self._config_queries[func]

        columns = self.schema if isinstance(self.schema, list) else [self.schema]

        attribute_str = ''
        for col in columns:
            col_func = self.rewrite(func_format, attribute=col)
            formatted_attr = self.rewrite(attribute_format, func=col_func, agg_func=func, attribute=col)
            if attribute_str == '':
                attribute_str = formatted_attr
            else:
                attribute_str = self.rewrite(attribute_separator, left=attribute_str, right=formatted_attr)
        formatted_query = self.rewrite(query_templat, agg_value=attribute_str, subquery=self.query)
        new_query = self.rewrite(return_all, subquery=formatted_query)
        if query:
            return new_query

        stats = self.send_request(new_query).iloc[0]

        return stats

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

    def isin(self, values):
        if not isinstance(values, list):
            raise ValueError('Values must be a list')
        return self.binary_opt(values, 'isin')

    def binary_opt(self, other, opt):
        comparison_statement = self.config_queries[opt]

        single_attr_format = self.config_queries['single_attribute']
        single_attr_format = self.rewrite(single_attr_format, attribute=self.schema)
        attr_separator = self.config_queries['attribute_separator']
        string_format = self.config_queries['str_format']

        other = other if isinstance(other, list) else [other]

        other_str = ''
        for item in other:
            if type(item) == str:
                item = self.rewrite(string_format, value=item)
            if other_str == '':
                other_str = str(item)
            else:
                other_str = self.rewrite(attr_separator, left=other_str, right=item)

        comparison = AFrame.rewrite(comparison_statement, left=single_attr_format, right=other_str)

        query = self.config_queries['q2']
        col_alias = self.config_queries['attribute_value']
        query = self.rewrite(query, attribute_value=col_alias)
        escape_chars = self.config_queries['escape']
        alias = re.sub(escape_chars, '', AFrame.rewrite(comparison_statement, left=self.schema, right=str(other_str)))
        query = self.rewrite(query, alias=alias, attribute=comparison, subquery=self.query)
        return AFrame(self._dataverse, self._dataset, comparison, query, is_view=self._is_view, connector=self._connector)

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
        return AFrame(self._dataverse, self._dataset, logical_statement, new_query, is_view=self._is_view, connector=self._connector)

    def get_dataverse(self):
        sub_query = self.query.lower().split("from")
        data = sub_query[1][1:].split(".", 1)
        dataverse = data[0]
        dataset = data[1].split(" ")[0]
        return dataverse, dataset

    def map(self, func, *args, **kwargs):
        project_query = self.config_queries['q2']
        attribute_value = self.config_queries['attribute_value']
        project_query = self.rewrite(project_query, attribute_value=attribute_value)
        single_attribute = self.config_queries['single_attribute']
        function_format = self.config_queries['function_format']
        attribute_separator = self.config_queries['attribute_separator']
        kwarg_format = self.config_queries['kwarg']
        string_format = self.config_queries['str_format']
        escape_chars = self.config_queries['escape']

        if not isinstance(func, str):
            raise TypeError('Function name must be string.')
        args_str = ''
        if args:
            function_format = self.config_queries['function_arg_format']
            for arg in args:
                if isinstance(arg, str):
                    arg = self.rewrite(string_format, value=arg)
                args_str = arg if args_str == '' else self.rewrite(attribute_separator, left=args_str, right=arg)
        if kwargs:
            function_format = self.config_queries['function_arg_format']
            for key, value in kwargs.items():
                if isinstance(value, str):
                    value = self.rewrite(string_format, value=value)
                    # args_str += ', %s = \"%s\"' % (key, value)
                arg = self.rewrite(kwarg_format, key=key, value=value)
                args_str = args_str if args_str == '' else self.rewrite(attribute_separator, left=args_str, right=arg)

        attribute_str=''
        attributes = self.schema if isinstance(self.schema, list) else [self.schema]

        for attr in attributes:
            attr = self.rewrite(single_attribute, attribute=attr)
            attribute_str = attr if attribute_str == '' else self.rewrite(attribute_separator, left=attribute_str, right=attr)

        function = self.rewrite(function_format, function=func, attribute=attribute_str, argument=args_str)
        alias = re.sub(escape_chars, '', str(function))
        new_query = self.rewrite(project_query, alias=alias, attribute=function, subquery=self.query)
        return AFrame(self._dataverse, self._dataset, alias, new_query, is_view=self._is_view, connector=self._connector)

    def apply(self, func, namespace=None):
        dataverse = namespace if namespace else self._dataverse
        new_query = self.config_queries['q19']
        # project_query = self.config_queries['q2']
        # attribute_value = self.config_queries['attribute_value']
        # project_query = self.rewrite(project_query, attribute_value=attribute_value)
        function_format = self.config_queries['record_function_format']
        # escape_chars = self.config_queries['escape']

        if not isinstance(func, str):
            raise TypeError('Function name must be string.')

        function = self.rewrite(function_format, function=func, namespace=dataverse)
        # alias = re.sub(escape_chars, '', str(function))
        new_query = self.rewrite(new_query, attribute=function, subquery=self.query)
        return AFrame(self._dataverse, self._dataset, function, new_query, is_view=self._is_view,
                      connector=self._connector)


    def to_collection(self, name, query=False):
        if query:
            return self._connector.to_collection(self.query, self._dataverse, self._dataset, name, True)
        new_con = self._connector.to_collection(self.query, self._dataverse, self._dataset, name)
        return AFrame(dataverse=self._dataverse, dataset=name, connector=new_con)

    def to_view(self, name, query=False):

        if query:
            return self._connector.to_view(self.query, self._dataverse, self._dataset, name, True)

        new_query = self.config_queries['q15']
        new_query = self.rewrite(new_query, namespace=self._dataverse, view=name)
        new_con = self._connector.to_view(self.query, self._dataverse, self._dataset, name)
        new_view = AFrame(dataverse=self._dataverse, dataset=name, query=new_query, connector=new_con, is_view=True)
        new_view._connector.get_view(dataverse=self._dataverse, dataset=name)
        return new_view

    def to_transformation(self, name, namespace=None, query=False):
        if query:
            dataverse = namespace if namespace else self._dataverse
            return self._connector.to_transformation(self.query, dataverse, self._dataset, name, True)
        else:
            self._connector.to_transformation(self.query, self._dataverse, self._dataset, name)

    def persist(self, name, mode='collection', namespace=None, query=False):

        if str.lower(mode) == 'collection':
            return self.to_collection(name, query)
        elif str.lower(mode) == 'view':
            return self.to_view(name, query)
        elif str.lower(mode) == 'transformation':
            self.to_transformation(name, namespace, query)
        else:
            raise ValueError('The mode indicated is not available.')

    @staticmethod
    def read_json(connector, filepath, name=None, namespace=None, orient='records', lines=True, query=False):
        if query:
            return connector.read_json(filepath, name, namespace, orient, lines, query)
        initial_query = connector.read_json(filepath, name, namespace, orient, lines, query)
        return AFrame(namespace, name, query=initial_query, connector=connector)

    @staticmethod
    def read_csv(connector, filepath, sep=',', name=None, namespace=None, header=0, query=False):
        if query:
            return connector.read_csv(filepath, sep, name, namespace, header, query)
        initial_query = connector.read_csv(filepath, sep, name, namespace, header, query)
        return AFrame(namespace, name, query=initial_query, connector=connector)

    def drop_transformation(self, name, namespace=None):
        dataverse = namespace if namespace else self._dataverse
        return self._connector.drop_transformation(namespace=dataverse, name=name)

    def get_dataType(self):
        query = 'select value t. DatatypeName from Metadata.`Dataset` t where' \
                ' t.DataverseName = \"%s\" and t.DatasetName = \"%s\"' % (self._dataverse, self._dataset)
        result = self.send_request(query)
        return result[0]

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

    def drop_dataset(self):
        if self._is_view:
            return self._connector.drop_view(namespace=self._dataverse, collection=self._dataset)
        else:
            return self._connector.drop_collection(namespace=self._dataverse, collection=self._dataset)

    def drop_collection(self):
        return self._connector.drop_collection(namespace=self._dataverse, collection=self._dataset)

    def drop_view(self):
        return self._connector.drop_view(namespace=self._dataverse, collection=self._dataset)


class NestedAFrame(AFrame):
    def __init__(self, dataverse, dataset, schema, query, is_view, connector, attributes=None):
        self._schema = schema
        self._query = query
        self._data = None
        self._dataverse = dataverse
        self._dataset = dataset
        self._nested_fields = attributes
        AFrame.__init__(self, dataverse, dataset, schema, query, is_view=is_view, connector=connector)
        self.get_base_query(attributes)

    def get_base_query(self, attributes):
        query = self.config_queries['get_json']
        if attributes:
            for attr in attributes:
                new_query = self.rewrite(query, subquery=self.query, attribute=attr)
                self.query = new_query
        else:
            new_query = self.rewrite(query, subquery=self.query)
            self.query = new_query

    def head(self, sample=5, query=False):
        limit_query = self.config_queries['limit']
        new_query = AFrame.rewrite(limit_query, num=str(sample), subquery=self.query)

        if query:
            return new_query

        result = self.send_request(new_query)
        if '_uuid' in result.columns:
            result.drop('_uuid', axis=1, inplace=True)
        norm_result = pd.json_normalize(json.loads(result.to_json(orient='records')))
        norm_cols = norm_result.columns.str.split('.', expand=True).values
        norm_result.columns = pd.MultiIndex.from_tuples([('', x[0]) if pd.isnull(x[1]) else x for x in norm_cols])

        if '_uuid' in norm_result.columns:
            norm_result.drop('_uuid', axis=1, inplace=True)
        return norm_result


class RollingAFrame(AFrame):
    def __init__(self, dataverse, dataset, schema, connector, columns, on, query, window=None):
        AFrame.__init__(self, dataverse, dataset, schema=schema, query=query, connector=connector)
        self._window = window
        self._columns = columns
        self._data = None
        self._dataverse = dataverse
        self._dataset = dataset
        self.on = on
        self.query = query

    def build_window(self):
        print()

    def get_window(self):
        return self._connector.get_window(self._window)

    def validate_agg_func(self, func, arg=None):
        over = self.get_window()
        query_format = self.config_queries['q20']
        window_format = self.config_queries['window']
        func_format = self.config_queries['window_'+func]
        agg_window_format = self.config_queries['agg_window']

        window = AFrame.rewrite(window_format, over=over)

        if self.on is not None:
            if arg is None:
                func = AFrame.rewrite(func_format, attribute=self.on)
            else:
                func = AFrame.rewrite(func_format, attribute=arg)
            agg_window = AFrame.rewrite(agg_window_format, func=func, window=window)
            query = AFrame.rewrite(query_format, agg_window=agg_window, subquery=self.query)

        return RollingAFrame(self._dataverse, self._dataset, agg_window, self._connector, window, self.on, query, self._window)

    def sum(self, col=None):
        return self.validate_agg_func('sum',col)

    def count(self, col=None):
        return self.validate_agg_func('count', col)

    def avg(self, col=None):
        return self.validate_agg_func('avg', col)

    mean = avg

    def min(self, col=None):
        return self.validate_agg_func('min', col)

    def max(self, col=None):
        return self.validate_agg_func('max', col)

    def stddev_samp(self, col=None):
        return self.validate_agg_func('stddev_samp', col)

    def stddev_pop(self, col=None):
        return self.validate_agg_func('stddev_pop', col)

    def var_samp(self, col=None):
        return self.validate_agg_func('var_samp', col)

    def var_pop(self, col=None):
        return self.validate_agg_func('var_pop', col)

    def skewness(self, col=None):
        return self.validate_agg_func('skewness', col)

    def kurtosis(self, col=None):
        return self.validate_agg_func('kurtosis', col)

    def row_number(self):
        return self.validate_window_function('row_number')

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
        over = self.get_window()

        query_format = self.config_queries['q20']
        window_format = self.config_queries['window']
        func_format = self.config_queries['window_' + func]
        agg_window_format = self.config_queries['agg_window']

        window = AFrame.rewrite(window_format, over=over)

        func = func_format
        agg_window = AFrame.rewrite(agg_window_format, func=func, window=window)
        query = AFrame.rewrite(query_format, agg_window=agg_window, subquery=self.query)

        return RollingAFrame(self._dataverse, self._dataset, agg_window, self._connector, agg_window, self.on, query,
                             self._window)

    def validate_window_function_argument(self, func, expr, ignore_null):
        if not isinstance(expr,str):
            raise ValueError('expr for first_value must be string')

        over = self.get_window()

        if ignore_null:
            columns = '%s(%s) IGNORE NULLS %s' % (func, expr, over)

        query_format = self.config_queries['q20']
        window_format = self.config_queries['window']
        func_format = self.config_queries['window_' + func.lower()]
        agg_window_format = self.config_queries['agg_window']

        window = AFrame.rewrite(window_format, over=over)
        func = AFrame.rewrite(func_format, expr=expr)
        agg_window = AFrame.rewrite(agg_window_format, func=func, window=window)
        query = AFrame.rewrite(query_format, agg_window=agg_window, subquery=self.query)

        return RollingAFrame(self._dataverse, self._dataset, agg_window, self._connector, agg_window, self.on, query, self._window)

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
        query = 'SELECT VALUE %s FROM %s t' % (columns, dataset)
        # return RollingAFrame(self._dataverse, self._dataset, columns, self.on, query, self._window)
        return RollingAFrame(self._dataverse, self._dataset, columns, self._connector, columns, self.on, query,
                             self._window)
