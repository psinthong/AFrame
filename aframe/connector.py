import pandas as pd


class Connector:

    def __init__(self, *args):
        pass

    def connect(self, config_file):
        return None

    def send_request(self,query):
        return pd.DataFrame()


class AsterixConnector(Connector):
    def __init__(self, server_address="http://localhost:19002"):
        self._server_address = server_address
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
        data['statement'] = query
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

    def __init__(self, server_address):
        self._server_address = server_address
        # db_str = "postgres://gift:pass@localhost:5432/TinySocial"
        self._db = self.connect(self._server_address)
        Connector.__init__(self, server_address)

    def connect(self, db_str):
        from sqlalchemy import create_engine
        return create_engine(db_str)

    def send_request(self, query):
        result_obj = self._db.execute(query)
        results = result_obj.fetchall()
        return pd.DataFrame(results, columns=result_obj.keys())
