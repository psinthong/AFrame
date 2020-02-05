import pandas as pd
import pandas.io.json as json
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
        query = self._config_queries['q7']
        query = af.AFrame.rewrite(query, subquery=old_query[:-1], grp_by_attribute=by_lst)
        if self._schema:
            query = 'SELECT %s FROM (%s) t GROUP BY %s AS %s ' \
                    'GROUP AS grps(t AS grp);' % (by_lst, self._schema, old_query[:-1], by_lst)
        self._schema = 'GROUP BY %s GROUP AS grps(t AS grp)' % by_lst
        return query

    def get_group(self, key):
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
        and_statement = self._config_queries['and']
        attr_format = self._config_queries['attribute_value']
        condition = af.AFrame.concat_statements(attr_format, and_statement, key_lst)
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

    def agg(self, func, query=False):
        if not isinstance(func, dict):
            raise ValueError("Currently only support a dictionary of attribute:func or [funcs]")

        by_lst = ','.join(self._by)
        agg_query = self._config_queries['q8']
        agg_statement = self._config_queries['agg_value']
        agg_values = []

        functions = ['count', 'min', 'max', 'avg', 'sum', 'stddev_samp', 'stddev_pop', 'var_samp', 'var_pop']
        for key in func.keys():
            if isinstance(func[key], list):
                for func_val in func[key]:
                    if str(func_val).lower() in functions:
                        agg_values.append(af.AFrame.rewrite(agg_statement, agg_func=func_val, attribute=key))
                    else:
                        raise ValueError('Aggregate function %s is not available' % func)
            else:
                if str(func[key]).lower() in functions:
                    agg_values.append(af.AFrame.rewrite(agg_statement, agg_func=func[key], attribute=key))
                else:
                    raise ValueError('Aggregate function %s is not available' % func)

        agg_val_str = ','.join(agg_values)
        agg_query = af.AFrame.rewrite(agg_query, subquery=self._base_query[:-1], grp_by_attribute=by_lst, agg_value=agg_val_str)

        if query:
            return agg_query

        results = json.dumps(self.send_request(agg_query))
        df = pd.DataFrame(data=json.read_json(results))
        return df

    def send_request(self, query: str):
        return self._con.send_request(query)