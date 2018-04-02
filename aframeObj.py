import aframe as af
import pandas as pd
import pandas.io.json as json


class AFrameObj:
    def __init__(self, dataverse, dataset, schema, query=None):
        self._schema = schema
        self._query = query
        self._data = None
        self._dataverse = dataverse
        self._dataset = dataset

    def __str__(self):
        return 'Column: '+str(self._schema)

    @property
    def schema(self):
        return self._schema

    @property
    def query(self):
        return str(self._query)

    def collect(self):
        results = af.AFrame.send_request(self._query)
        json_str = json.dumps(results)
        result = pd.DataFrame(data=json.read_json(json_str))
        return result

    def head(self, num=5):
        new_query = self.query[:-1]+' limit %d' %num
        results = af.AFrame.send_request(new_query)
        json_str = json.dumps(results)
        result = pd.DataFrame(data=json.read_json(json_str))
        return result

    def __eq__(self, other):
        return self.binary_opt(other, '=')

    def __ne__(self, other):
        return self.binary_opt(other, '!=')

    def __gt__(self, other):
        return self.binary_opt(other, '>')

    def __lt__(self, other):
        return self.binary_opt(other, '<')

    def __ge__(self, other):
        return self.binary_opt(other, '>=')

    def __le__(self, other):
        return self.binary_opt(other, '<=')

    def binary_opt(self, other, opt):
        dataset = self._dataverse+'.'+self._dataset
        if isinstance(self, af.AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            schema = 't.%s %s %s' %(self.schema, opt, other)
            query = 'select value %s from %s t;' %(schema, dataset)
            return AFrameObj(self._dataverse, self._dataset, schema, query)

    def __and__(self, other):
        dataset = self._dataverse + '.' + self._dataset
        if isinstance(self, af.AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            if isinstance(other, AFrameObj):
                schema = self.schema+' and '+other.schema
                new_query = 'select value %s from %s t;' %(schema, dataset)
                return AFrameObj(self._dataverse, self._dataset, schema, new_query)

    def toAframe(self):
        dataverse, dataset = self.get_dataverse()
        return af.AFrame(dataverse, dataset)

    def get_dataverse(self):
        sub_query = self.query.split("from")
        data = sub_query[1][1:].split(".", 1)
        dataverse = data[0]
        dataset = data[1].split(" ")[0]
        return dataverse, dataset