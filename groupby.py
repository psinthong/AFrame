import pandas as pd
import pandas.io.json as json
import aframe as af


class AFrameGroupBy:

    def __init__(self, dataverse, dataset, by=None):
        self._dataverse = dataverse
        self._dataset = dataset
        self._by = by
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
        query = 'select * from %s t GROUP BY t.%s as grp_id ' \
                'GROUP AS grps(t AS grp);' %(dataset, by)
        return query

    def get_group(self, key):
        new_query = 'select value t.grps from (%s) t where grp_id=%s;' % (self.query[:-1], str(key))
        results = json.dumps(af.AFrame.send_request(new_query)[0])
        grp = json.read_json(results)['grp']
        df = pd.DataFrame(grp.tolist())
        return df

