from aframe.aframe import AFrame
# from aframe.aframeObj import AFrameObj
import subprocess
import ast
import pandas as pd
import json
import numpy as np


class PartiQL(AFrame):
    def __init__(self, dataverse='', dataset='', schema=None, query=None, shell='/Users/gift/github/partiql-cli-0.1.0/bin/partiql',file_path='/Users/gift/github/Data/employee.env'):
        self._shell = shell
        self._path = file_path
        AFrame.__init__(self, dataverse, dataset,schema,query)


    # def create_query(self, query, selection=None, projection=None):
        # dataset = self._dataverse+'.'+self._dataset
        # if isinstance(projection, str):
        #     if self.query is None:
        #         query = 'SELECT t.%s FROM %s t;' % (projection, dataset)
        # if isinstance(projection, list):
        #     fields = ''
        #     for i in range(len(projection)):
        #         if i > 0:
        #             fields += ', '
        #         fields += 't.%s' % projection[i]
        #     if self.query is None:
        #         query = 'SELECT %s FROM %s t;' % (fields, dataset)
        #     else:
        #         query = 'SELECT %s FROM (%s) t;' % (fields, self.query[:-1])
        # return query

    # def __getitem__(self, key):
    #     dataset = self._dataverse + '.' + self._dataset
    #     if isinstance(key, AFrameObj):
    #         if self.query is None:
    #             new_query = 'SELECT t FROM %s t WHERE %s;' %(dataset, key.schema)
    #         else:
    #             new_query = 'SELECT t FROM (%s) t WHERE %s;' % (self.query[:-1], key.schema)
    #         return AFrameObj(self._dataverse, self._dataset, key.schema, new_query)
    #
    #     if isinstance(key, str):
    #         if self.query is None:
    #             query = self.create_query(None, projection=key)
    #         else:
    #             query = self.create_query(None, projection=key)
    #         return AFrameObj(self._dataverse, self._dataset, key, query)
    #
    #     if isinstance(key, (np.ndarray, list)):
    #         fields = ''
    #         for i in range(len(key)):
    #             if i > 0:
    #                 fields += ', '
    #             fields += 't.%s' % key[i]
    #         if self.query is None:
    #             query = self.create_query(None, projection=key)
    #         else:
    #             query = self.create_query(None, projection=key)
    #         return AFrameObj(self._dataverse, self._dataset, key, query)

    def head(self, sample=5):
        dataset = self._dataverse + '.' + self._dataset
        if self.query is not None:
            new_query = self.query[:-1] + ' LIMIT %d;' % sample
        else:
            new_query = 'SELECT VALUE t FROM %s t LIMIT 5;' % dataset
        output = self.send_request(new_query)
        json_data = json.loads(output)
        df = pd.DataFrame(json_data)
        return df

    def send_request(self, query: str):
        HOST = "127.0.0.1"
        ssh = subprocess.Popen(["ssh",
                                "%s" % HOST],
                               stdin=subprocess.PIPE,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               universal_newlines=True,
                               bufsize=0)
        # send ssh commands to stdin
        ssh.stdin.write("%s -e %s\n" %(self._shell,self._path))
        ssh.stdin.write("%s\n\n" %query)
        ssh.stdin.close()

        cnt = 0
        output = '['
        for line in ssh.stdout:
            if line.strip() == '>>':
                break
            if cnt > 2:
                output += line.strip()
            cnt += 1
        output = output.replace('\'', '\"')
        output += ']'
        return output

    def __str__(self):
        txt = 'PartiQL DataFrame'
        return txt

    def __repr__(self):
        return self.__str__()
