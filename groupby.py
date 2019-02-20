import pandas as pd
import pandas.io.json as json
import aframe as af


class AFrameGroupBy:

    def __init__(self, dataverse, dataset, by=None):
        self._dataverse = dataverse
        self._dataset = dataset
        self._by = by
        self._schema = None
        self._query = self.get_initial_query(by)


    @property
    def query(self):
        return str(self._query)

    def toAframe(self):
        dataverse, dataset = self.get_dataverse()
        return af.AFrame(dataverse, dataset)

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
        results = json.dumps(af.AFrame.send_request(new_query)[0])
        grp = json.read_json(results)['grp']
        df = pd.DataFrame(grp.tolist())
        return df

    def count(self):
        new_query = 'SELECT VALUE count(*) FROM (%s) t;' % self.query[:-1]
        result = af.AFrame.send_request(new_query)[0]
        return result

    # def agg(self, func):
    #     dataset = self._dataverse + '.' + self._dataset
    #     query = 'select t.%s, array_%s(grp) as %s from %s t %s;' % (self._by, func, func, dataset, self._schema)
    #     new_g = AFrameGroupBy(self._dataverse, self._dataset, self._by)
    #     new_g._query = query
    #     return new_g

    def agg(self, attr, func):
        dataset = self._dataverse + '.' + self._dataset
        if func == 'count':
            query = 'SELECT grp_id, count(%s) AS %s FROM %s t ' % (attr, func, dataset)
            query += self._schema + ';'
            results = json.dumps(af.AFrame.send_request(query))
            df = pd.DataFrame(data=json.read_json(results), columns=['grp_id', func])
            return df
        if func == 'max':
            query = 'SELECT grp_id, max(t.%s) AS %s FROM %s t ' % (attr, func, dataset)
            query += self._schema+';'
            results = json.dumps(af.AFrame.send_request(query))
            df = pd.DataFrame(data=json.read_json(results), columns=['grp_id', func])

            return df
