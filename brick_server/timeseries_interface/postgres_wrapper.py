import psycopg2
from psycopg2.extras import execute_values
import pdb
from datetime import datetime

class BrickTimeseries(object):
    def __init__(self, dbname, user, pw, host, port=5601):
        self.DB_NAME = dbname
        self.TABLE_NAME = 'brick_data'
        self.conn = psycopg2.connect(
            "dbname='{dbname}' user='{user}' host='{host}' port='{port}' password='{pw}'".format(
                dbname=self.DB_NAME, user=user, pw=pw, host=host, port=port))
        self.cur = self.conn.cursor()

    def _get_cursor(self):
        return self.conn.cursor()

    def get_all_data(self, query=''):
        cur = self._get_cursor()
        cur.execute("""SELECT (uuid, time, value) FROM {0}""".format(self.TABLE_NAME))
        res = cur.fetchall()
        pdb.set_trace()

    def query(self, begin_time=None, end_time=None, uuids=[]):
        if not (begin_time or end_time or uuids):
            return self.get_all_data()

    def add_data(self, data):
        """
        - input
            - uuid (str): a unique id of one sensor
            - data (list(tuple)): timeseries data. E.g., [(1055151, 70.0), 1055153, 70.1)]
        """
        if not data:
            raise Exception('Empty data to insert')
        sql = """
              INSERT INTO {0}(uuid, time, value)
              VALUES %s
              """.format(self.TABLE_NAME)
        encoded_data = [(datum[0], datetime.fromtimestamp(datum[1]), datum[2])
                        for datum in data]
        cur = self._get_cursor()
        execute_values(cur, sql, encoded_data)
        self.conn.commit()
        pdb.set_trace()

if __name__ == '__main__':
    dbname = 'brick'
    user = 'bricker'
    pw = 'brick-demo'
    host = 'localhost'
    port = 6001
    brick_ts = BrickTimeseries(dbname, user, pw, host, port)

    data = [
        ['id1', 1524436788, 70.0],
        ['id2', 1524436788, 900],
        ['id21', 1524437788, 70.5],
    ]
    brick_ts.add_data(data)
    brick_ts.query_data()
