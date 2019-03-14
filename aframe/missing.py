import aframe as af
import numpy as np


def notna(obj):

    if isinstance(obj, af.AFrame):
        raise NotImplementedError
    elif isinstance(obj, af.AFrameObj):
        query = 'SELECT VALUE t FROM (%s) AS t WHERE ' % obj.query[:-1]
        if isinstance(obj.schema, (np.ndarray, list)):
            fields = obj.schema
            fields_str = ''
            for field in fields:
                fields_str += 't.%s IS KNOWN AND ' % field
            fields_str = fields_str[:-4]
            query = query + fields_str+';'
            schema = fields_str
        if isinstance(obj.schema, str):
            field_str = 't.%s IS KNOWN' % obj.schema
            query = query + field_str + ';'
            schema = field_str
        return af.AFrameObj(obj._dataverse, obj._dataset, schema, query)



