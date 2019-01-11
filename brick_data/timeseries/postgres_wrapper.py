import pdb
from datetime import datetime
import pandas as pd

from shapely.geometry import LineString, Point
from shapely import wkb
import psycopg2
from psycopg2.extras import execute_values
from geoalchemy2.shape import from_shape
#from postgis.psycopg import register
#from postgis import Point

class PostgresInterface(object):
    def __init__(self, dbname, table_name, user, pw, host, port=5601):
        self.DB_NAME = dbname
        self.TABLE_NAME = table_name
        conn_str = "dbname='{dbname}' host='{host}' port='{port}' " \
            .format(dbname=self.DB_NAME, host=host, port=port) + \
            "password='{pw}' user='{user}'".format(user=user, pw=pw)
        self.conn = psycopg2.connect(conn_str)

    def _get_cursor(self):
        return self.conn.cursor()

    def _exec_query(self, qstr):
        cur = self._get_cursor()
        cur.execute(qstr)
        raw_res = cur.fetchall()
        return raw_res

    def raw_query(self, qstr):
        raw_res = self._exec_query(qstr)
        # TODO: Process it.
        return raw_res


class BrickTimeseries(object):
    def __init__(self, dbname, user, pw, host, port=5601):
        self.DB_NAME = dbname
        self.TABLE_NAME = 'brick_data'
        conn_str = "dbname='{dbname}' host='{host}' port='{port}' " \
            .format(dbname=self.DB_NAME, host=host, port=port) + \
            "password='{pw}' user='{user}'".format(user=user, pw=pw)
        self.conn = psycopg2.connect(conn_str)
        #self.cur = self.conn.cursor()

    def _get_cursor(self):
        return self.conn.cursor()

    def display_data(self, res):
        times = []
        uuids = []
        values = []
        locs = []
        for row in res:
            [uuid, t, value, loc] = row
            times.append(t)
            uuids.append(uuid)
            values.append(value)
            locs.append(loc)
        df = pd.DataFrame({
            'time': times,
            'uuid': uuids,
            'value': values,
            'loc': locs
        })
        print(df)

    def get_all_data(self, query=''):
        cur = self._get_cursor()
        cur.execute("""SELECT uuid, time, value, ST_AsGeoJson(loc) FROM {0}""".format(self.TABLE_NAME))
        res = cur.fetchall()
        return res

    def _format_select_res(self, res):
        # TODO: Implement
        return res

    def _exec_query(self, qstr):
        cur = self._get_cursor()
        cur.execute(qstr)
        query_type = cur.statusmessage.split()[0]
        if query_type =='SELECT':
            raw_res = cur.fetchall()
        elif query_type == 'DELETE':
            raw_res = None
        elif query_type == 'INSERT':
            raw_res = None
        else:
            raise Exception('not implemented yet')
        return raw_res

    def _timestamp2str(self, ts):
        return datetime.fromtimestamp(ts)

    def raw_query(self, qstr):
        raw_res = self._exec_query(qstr)
        res = self._format_select_res(raw_res)
        return res

    def delete(self, start_time=None, end_time=None, uuids=[]):
        assert uuids, 'Any UUIDs should be given for deleting timeseries data'
        qstr = """
        DELETE FROM {0}
        WHERE
        """.format(self.TABLE_NAME)
        qstr += "uuid IN ({0})\n AND "\
            .format("'" + "', '".join(uuids) + "'")
        if start_time:
            qstr += "time >= '{0}'\n AND "\
                .format(self._timestamp2str(start_time))
        if end_time:
            qstr += "time < '{0}'\n AND "\
                .format(self._timestamp2str(end_time))
        qstr = qstr[:-4]
        res = self.raw_query(qstr)
        self.conn.commit()

    def query(self, start_time=None, end_time=None, uuids=[]):
        qstr = """
        SELECT uuid, time, value, ST_AsGeoJson(loc) FROM {0}
        """.format(self.TABLE_NAME)
        if not (start_time or end_time or uuids):
            qstr += 'DUMY' # dummy characters to be removed.
        else:
            qstr += 'WHERE\n'
            if start_time:
                qstr += "time >= '{0}'\n AND "\
                    .format(self._timestamp2str(start_time))
            if end_time:
                qstr += "time < '{0}'\n AND "\
                    .format(self._timestamp2str(end_time))
            if uuids:
                qstr += "uuid IN ({0})\n AND "\
                    .format("'" + "', '".join(uuids) + "'")
        qstr = qstr[:-4]
        res = self.raw_query(qstr)
        return res


    def _encode_value_data(self, data):
        return [(datum[0], self._timestamp2str(datum[1]), datum[2])
                for datum in data]

    def _encode_loc_data(self, data):
        data = [(datum[0],
                 self._timestamp2str(datum[1]),
                 Point((datum[2][0], datum[2][1])).wkb_hex
                 ) for datum in data]
        return data

    def _add_value_data(self, data):
        cur = self._get_cursor()
        sql = """
              INSERT INTO {0}(uuid, time, value)
              VALUES %s

              ON CONFLICT (time, uuid) DO UPDATE SET value = excluded.value;
              """.format(self.TABLE_NAME)
        encoded_data = self._encode_value_data(data)
        execute_values(cur, sql, encoded_data)
        self.conn.commit()

    def _add_loc_data(self, data):
        cur = self._get_cursor()
        sql = """
              INSERT INTO {0}(uuid, time, loc)
              VALUES %s
              ON CONFLICT (time, uuid) DO UPDATE SET loc = excluded.loc;
              """.format(self.TABLE_NAME)
        encoded_data = self._encode_loc_data(data)
        execute_values(cur, sql, encoded_data)
        self.conn.commit()

    def _add_loc_data_dep(self, data):
        cur = self._get_cursor()
        sql = """
          INSERT INTO {0}(uuid, time, loc)
          VALUES (%(uuid)s, %(time)s, ST_SetSRID(%(geom)s::geometry, %(srid)s))
          ON CONFLICT (time, uuid) DO UPDATE SET loc = excluded.loc;
          """.format(self.TABLE_NAME)
        for datum in data:
            point = Point((datum[2][0], datum[2][1]))
            cur.execute(sql,
                        {'geom': point.wkb_hex,
                         'srid': 4326,
                         'uuid': datum[0],
                         'time': self._timestamp2str(datum[1])
                         })
        self.conn.commit()

    def add_data(self, data, data_type='value'):
        """
        - input
            - uuid (str): a unique id of one sensor
            - data (list(tuple)): timeseries data. E.g., [(1055151, 70.0), 1055153, 70.1)]
        """
        assert data_type in ['value', 'loc'] # TODO: Make these ENUM.

        if not data:
            raise Exception('Empty data to insert')
        if data_type == 'value':
            self._add_value_data(data)
        elif data_type == 'loc':
            self._add_loc_data(data)

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

    loc_data = [
        ['id1', 1524436788, [0,0]],
        ['id2', 1524436788, [0,1]],
    ]
    brick_ts.add_data(loc_data, 'loc')

    res = brick_ts.query()
    brick_ts.display_data(res)
    print(res)
