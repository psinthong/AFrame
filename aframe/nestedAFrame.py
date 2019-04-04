import aframe as af
import pandas as pd
from pandas.io.json import json_normalize
import pandas.io.json as json

class NestedAFrame:
    def __init__(self, dataverse, dataset, schema, query=None):
        self._schema = schema
        self._query = query
        self._data = None
        self._dataverse = dataverse
        self._dataset = dataset

    def head(self, sample=5):
        dataset = self._dataverse + '.' + self._dataset
        if self._query is not None:
            new_query = self._query[:-1]+' LIMIT %d;' % sample
            results = af.AFrame.send_request(new_query)
            norm_result = json_normalize(results)
        else:
            self._query = 'SELECT VALUE t FROM %s t;' % dataset
            new_query = self._query[:-1] + ' LIMIT %d;' % sample
            results = af.AFrame.send_request(new_query)
            norm_result = json_normalize(results)
        norm_cols = norm_result.columns.str.split('.', expand=True).values
        norm_result.columns = pd.MultiIndex.from_tuples([('', x[0]) if pd.isnull(x[1]) else x for x in norm_cols])

        if '_uuid' in norm_result.columns:
            norm_result.drop('_uuid', axis=1, inplace=True)
        return norm_result

