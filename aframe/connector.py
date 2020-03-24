import pandas as pd
import configparser
import os
import json


class Connector:

    def __init__(self, server_address=None, config_file_path=None, **kwargs):
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
                queries[key] = value.replace('\n', '\r\n')
        return queries

    def get_collection(self, **kwargs):
        pass

    def to_collection(self, subquery, namespace, collection, name, query=False):
        return Connector

    def to_view(self, subquery, namespace, collection, name, query=False):
        return Connector

    def drop_collection(self, namespace, collection):
        pass

    def drop_view(self, namespace, collection):
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
            if '_uuid' in df.columns:
                df.drop('_uuid', axis=1, inplace=True)
            return df

        except urllib.error.URLError as e:
            raise Exception('The following error occured: %s. Please check if AsterixDB is running.' % str(e.reason))

    def submit(self, query: str):
        import urllib.parse
        import urllib.request
        import urllib.error
        host = self.server_address+'/query/service'
        data = dict()
        data['statement'] = query
        data = urllib.parse.urlencode(data).encode('utf-8')
        with urllib.request.urlopen(host, data) as handler:
            result = json.loads(handler.read())
            return result['status']

    def to_collection(self, subquery, namespace, collection, name, query=False):
        new_query = 'CREATE TYPE $namespace._internalType IF NOT EXISTS AS OPEN{ _uuid: uuid};\n' \
                    'CREATE DATASET $namespace.$name(_internalType) PRIMARY KEY _uuid autogenerated;\n' \
                    'INSERT INTO $namespace.$name SELECT VALUE ($subquery);'
        new_query = new_query.replace('$namespace', namespace).replace('$name', name).replace('$subquery', subquery)
        if query:
            return new_query
        self.submit(new_query)
        return AsterixConnector(self.server_address)

    def to_view(self, subquery, namespace, collection, name, query=False):
        new_query = 'CREATE FUNCTION $namespace.$name(){\n$subquery};'
        new_query = new_query.replace('$namespace', namespace).replace('$name', name).replace('$subquery', subquery)
        if query:
            return new_query
        self.submit(new_query)
        return AsterixConnector(self.server_address)

    def drop_collection(self, namespace, collection):
        drop_query = 'DROP DATASET {}.{};'.format(namespace, collection)
        return self.submit(drop_query)

    def drop_view(self, namespace, collection):
        drop_query = 'DROP FUNCTION {}.{}@0;'.format(namespace, collection)
        return self.submit(drop_query)


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
        p_lst = [json.loads(s.strip(',')) for s in query.split('\r\n')]
        results = list(self._db.aggregate(pipeline=p_lst, allowDiskUse=True))
        return pd.DataFrame(results)

    def get_collection(self, dataverse, dataset):
        from pymongo.collection import Collection
        if not isinstance(self._db, Collection):
            self._db = self._db[dataverse][dataset]

    def to_collection(self, subquery, namespace, collection, name, query=False):
        new_query = '$subquery,\r\n{ "$out": "$name" }'
        new_query = new_query.replace('$name', name).replace('$subquery', subquery)
        if query:
            return new_query
        self.send_request(new_query)
        return MongoConnector(self.server_address)

    def to_view(self, subquery, namespace, collection, name, query=False):
        from pymongo import MongoClient

        pipeline = [json.loads(s.strip(',')) for s in subquery.split('\r\n')]
        db = MongoClient(self.server_address)
        create_view = {}
        create_view['create'] = "{}".format(name)
        create_view['viewOn'] = "{}".format(collection)
        create_view['pipeline'] = pipeline
        if query:
            return create_view
        db[namespace].command(create_view)
        return MongoConnector(self.server_address)

    def drop_collection(self, namespace, collection):
        self._db.drop()
        return 'success'

    drop_view = drop_collection


class CypherConnector(Connector):

    def __init__(self, uri='http://localhost:7474', config_file_path="cypher.ini", username=None, password=None):
        Connector.__init__(self, uri, config_file_path)
        self._db = self.connect(username=username,password=password)

    def connect(self, username, password):
        from py2neo import Graph
        return Graph(self.server_address, username=username, password=password)

    def send_request(self, query):
        import pandas as pd
        formatted_q = query.replace('\r\n','\n')
        results = self._db.run(formatted_q).to_data_frame()
        if len(results['t']) > 0:
            if isinstance(results['t'][0], dict):
                df = pd.DataFrame.from_records(results['t'])
            else:
                df = pd.DataFrame(results['t'])
            return df
