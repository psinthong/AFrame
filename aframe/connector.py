import pandas as pd
import configparser
import os
import json


class Connector:

    def __init__(self, server_address=None, config_file_path=None, *args):
        self._server_address = server_address
        self._config_file_path = config_file_path

    @property
    def server_address(self):
        return self._server_address

    def connect(self, config_file):
        return None

    def send_request(self, query):
        return pd.DataFrame()

    def get_config_queries(self):
        queries = {}
        config = configparser.ConfigParser()
        config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'conf', self._config_file_path))
        for section in config.sections():
            for (key, value) in config.items(section):
                queries[key] = value
        return queries

    def get_collection(self, dataverse, dataset):
        pass


class AsterixConnector(Connector):
    def __init__(self, server_address="http://localhost:19002", config_file_path="sql_pp.ini"):
        Connector.__init__(self, server_address=server_address, config_file_path=config_file_path)
        self._db = self.connect(server_address)

    def connect(self, server_address):
        return server_address

    def send_request(self, query):
        import urllib.parse
        import urllib.request
        import urllib.error
        from pandas.io import json

        host = self._server_address + '/query/service'
        data = dict()
        data['statement'] = query + ';'
        data = urllib.parse.urlencode(data).encode('utf-8')
        try:
            handler = urllib.request.urlopen(host, data)
            result = json.loads(handler.read())
            result = result['results']
            data = json.read_json(json.dumps(result))
            df = pd.DataFrame(data)
            return df

        except urllib.error.URLError as e:
            raise Exception('The following error occured: %s. Please check if AsterixDB is running.' % str(e.reason))


class SQLConnector(Connector):

    def __init__(self, server_address=None, config_file_path="sql.ini", engine=None):
        import sqlalchemy
        Connector.__init__(self, server_address, config_file_path)
        if isinstance(engine, sqlalchemy.engine.Engine):
            self._db = engine
        else:
            self._db = self.connect(db_str=self.server_address)

    def connect(self, db_str):
        from sqlalchemy import create_engine
        return create_engine(db_str)

    def send_request(self, query):
        result_obj = self._db.execute(query)
        results = result_obj.fetchall()
        return pd.DataFrame(results, columns=result_obj.keys())


class MongoConnector(Connector):

    def __init__(self, server_address=None, config_file_path="mongo.ini", engine=None):
        from pymongo import MongoClient
        Connector.__init__(self, server_address, config_file_path)
        if isinstance(engine, MongoClient):
            self._db = engine
        else:
            self._db = self.connect(db_str=self.server_address)

    def connect(self, db_str):
        from pymongo import MongoClient
        return MongoClient(db_str)

    def send_request(self, query):
        p_lst = [json.loads(s) for s in query.split('\n')]
        results = list(self._db.aggregate(pipeline=p_lst, allowDiskUse=True))
        return pd.DataFrame(results)

    def get_collection(self, dataverse, dataset):
        from pymongo.collection import Collection
        if not isinstance(self._db, Collection):
            self._db = self._db[dataverse][dataset]