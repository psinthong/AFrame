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
        result_flatten = af.AFrame.attach_row_id(results)
        if all(i is None for i in result_flatten):
            raise KeyError(self._schema)
        self._data = result_flatten
        # return pd.Series(self._data)
        result = pd.DataFrame(json.read_json(json.dumps(result_flatten)))
        result.set_index('row_id', inplace=True)
        return result

    def head(self, num=5):
        new_query = self._query[:-1]+' order by t.row_id limit %d;' % num
        results = af.AFrame.send_request(new_query)
        result_flatten = af.AFrame.attach_row_id(results)
        if all(i is None for i in result_flatten):
            raise KeyError(self._schema)
        # return pd.Series(results)
        json_str = json.dumps(result_flatten)
        result = pd.DataFrame(data=json.read_json(json_str))
        result.set_index('row_id', inplace=True)
        return result

    def __eq__(self, other):
        return self.boolean_exp(other, '=')

    def __gt__(self, other):
        return self.boolean_exp(other, '>')

    def __lt__(self, other):
        return self.boolean_exp(other, '<')

    def boolean_exp(self, other, exp):
        if isinstance(self, af.AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            old_query = self._query[:-1]
            new_query = 'with q as(%s) from q t select t.row_id, t.%s %s %s %s;' % \
                   (old_query, self._schema, exp, str(other), self._schema)
            self._query = new_query
            return AFrameObj(self._dataverse, self._dataset, self._schema, self._query)

    def __and__(self, other):
        if isinstance(self, af.AFrame):
            print('aframe instance!!')
        elif isinstance(self, AFrameObj):
            if isinstance(other, AFrameObj):
                left_q = self.query[:-1]
                right_q = other.query[:-1]
                new_q = 'with q1 as (%s), \n  q2 as(%s)\n ' \
                        'from q1 t, q2 t2 where t.row_id=t2.row_id ' \
                        'select t.row_id, t.%s and t2.%s as result;' \
                        % (left_q, right_q, self.schema, other.schema)
                return AFrameObj(self._dataverse, self._dataset, 'result', new_q)


    def toAframe(self):
        dataverse, dataset = self.get_dataverse()
        return af.AFrame(dataverse, dataset)

    def get_dataverse(self):
        sub_query = self.query.split("from")
        data = sub_query[1][1:].split(".",1)
        dataverse = data[0]
        dataset = data[1].split(" ")[0]
        return dataverse, dataset