import pandas as pd
import pandas.io.json as json
import urllib.parse
import urllib.request
import urllib.error


class AFrameGroupBy:

    def __init__(self, dataverse, dataset, server_address, by=None):
        self._dataverse = dataverse
        self._dataset = dataset
        self._server_address = server_address
        self._by = by
        self._schema = None
        self._query = self.get_initial_query(by)


    @property
    def query(self):
        return str(self._query)

    # def toAframe(self):
    #     dataverse, dataset = self.get_dataverse()
    #     return af.AFrame(dataverse, dataset)

    def get_dataverse(self):
        sub_query = self.query.split("from")
        data = sub_query[1][1:].split(".", 1)
        dataverse = data[0]
        dataset = data[1].split(" ")[0]
        return dataverse, dataset

    def get_initial_query(self, by):
        dataset = self._dataverse + '.' + self._dataset
        query = 'SELECT * FROM %s t GROUP BY t.%s AS grp_id ' \
                'GROUP AS grps(t AS grp);' % (dataset, by)
        if self._schema:
            query = 'SELECT %s FROM %s t GROUP BY t.%s AS grp_id ' \
                    'GROUP AS grps(t AS grp);' % (self._schema, dataset, by)
        self._schema = 'GROUP BY t.%s AS grp_id GROUP AS grps(t AS grp)' % by
        return query

    def get_group(self, key):
        new_query = 'SELECT VALUE t.grps FROM (%s) t WHERE grp_id=%s;' % (self.query[:-1], str(key))
        results = json.dumps(self.send_request(new_query)[0])
        grp = json.read_json(results)['grp']
        df = pd.DataFrame(grp.tolist())
        return df

    def count(self):
        columns = [self._by, 'count']
        dataset = self._dataverse + '.' + self._dataset
        new_query = 'SELECT grp_id AS %s, array_count(grps) AS count FROM %s t ' \
                    'GROUP BY t.%s AS grp_id GROUP AS grps(t AS grp);' % (self._by, dataset, self._by)
        # new_query = 'SELECT VALUE count(*) FROM (%s) t;' % self.query[:-1]
        results = pd.DataFrame(self.send_request(new_query))
        return results

    # def agg(self, func):
    #     dataset = self._dataverse + '.' + self._dataset
    #     query = 'select t.%s, array_%s(grp) as %s from %s t %s;' % (self._by, func, func, dataset, self._schema)
    #     new_g = AFrameGroupBy(self._dataverse, self._dataset, self._by)
    #     new_g._query = query
    #     return new_g

    def agg(self, attr, func):
        dataset = self._dataverse + '.' + self._dataset
        functions = ['count', 'min', 'max', 'avg', 'sum', 'stddev_samp', 'stddev_pop', 'var_samp', 'var_pop']
        if str(func).lower() in functions:
            query = 'SELECT grp_id, %s(%s) AS %s FROM %s t ' % (func, attr,func,dataset)
            query += self._schema + ';'
            results = json.dumps(self.send_request(query))
            df = pd.DataFrame(data=json.read_json(results), columns=['grp_id', func])
            return df
        else:
            raise ValueError('Aggregate function %s is not available' %func)

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
            raise Exception('The following error occured: %s.' %str(e.reason))