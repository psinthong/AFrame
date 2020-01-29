import pandas as pd
import pandas.io.json as json
import urllib.parse
import urllib.request
import urllib.error
import aframe as af
from aframe.connector import Connector


class AFrameGroupBy:

    def __init__(self, dataverse, dataset, old_query, config_queries, connector=Connector(), by=None):
        self._dataverse = dataverse
        self._dataset = dataset
        self._con = connector
        self._schema = None
        self._base_query = old_query
        self._config_queries = config_queries
        self._by = self.get_by_attributes(by)
        self._query = self.get_initial_query(old_query,by)


    @property
    def query(self):
        return str(self._query)

    def get_by_attributes(self, by):
        if isinstance(by, list):
            return by
        else:
            return [by]

    def get_dataverse(self):
        sub_query = self.query.split("from")
        data = sub_query[1][1:].split(".", 1)
        dataverse = data[0]
        dataset = data[1].split(" ")[0]
        return dataverse, dataset

    def get_initial_query(self, old_query, by):
        by_lst = ','.join(by)
        # query = 'SELECT * FROM (%s) t GROUP BY %s ' \
        #         'GROUP AS grps(t AS grp);' % (old_query[:-1], by_lst)
        query = self._config_queries['q7']
        query = af.AFrame.rewrite(query, subquery=old_query[:-1], grp_by_attribute=by_lst)
        if self._schema:
            query = 'SELECT %s FROM (%s) t GROUP BY %s AS %s ' \
                    'GROUP AS grps(t AS grp);' % (by_lst, self._schema, old_query[:-1], by_lst)
        self._schema = 'GROUP BY %s GROUP AS grps(t AS grp)' % by_lst
        return query

    def get_group(self, key):
        # new_query = 'SELECT VALUE t.grps FROM (%s) t WHERE %s=%s;' % (self.query[:-1], self._by, str(key))
        new_query = self._config_queries['q3']
        key_lst = []
        if isinstance(key, (list, tuple)):
            key_lst = [k for k in key]
        else:
            key_lst.append(key)

        for i in range(len(key_lst)):
            k = key_lst[i]
            if isinstance(k, str):
                k = '"{}"'.format(k)
                key_lst[i] = str(self._by[i]) + " = " + k
            else:
                key_lst[i] = str(self._by[i]) + " = " + str(k)
        # condition = ' AND '.join(key_lst)
        # condition = ''
        and_statement = self._config_queries['and']
        condition = af.AFrame.concat_statements(and_statement, key_lst)
        new_query = af.AFrame.rewrite(new_query, subquery=self._base_query[:-1], statement=condition)
        results = json.dumps(self.send_request(new_query))
        df = pd.DataFrame(data=json.read_json(results))
        return df

    def count(self):
        columns = [self._by, 'count']
        dataset = self._dataverse + '.' + self._dataset
        new_query = 'SELECT %s, array_count(grps) AS count FROM %s t ' \
                    'GROUP BY t.%s GROUP AS grps(t AS grp);' % (self._by, dataset, self._by)
        # new_query = 'SELECT VALUE count(*) FROM (%s) t;' % self.query[:-1]
        results = pd.DataFrame(self.send_request(new_query))
        return results

    def agg(self, attr, func):
        by_lst = ','.join(self._by)

        functions = ['count', 'min', 'max', 'avg', 'sum', 'stddev_samp', 'stddev_pop', 'var_samp', 'var_pop']
        if str(func).lower() in functions:
            agg_statement = self._config_queries['agg_value']
            query = self._config_queries['q8']
            query = af.AFrame.rewrite(query, agg_value=agg_statement)
            query = af.AFrame.rewrite(query, subquery=self._base_query[:-1], grp_by_attribute=by_lst, agg_func=func, attribute=attr)
            # query = 'SELECT %s, %s(%s) FROM (%s) t ' % (by_lst, func, attr,func,self._base_query[:-1])
            results = json.dumps(self.send_request(query))
            df = pd.DataFrame(data=json.read_json(results))
            return df
        else:
            raise ValueError('Aggregate function %s is not available' %func)

    def send_request(self, query: str):
        return self._con.send_request(query)