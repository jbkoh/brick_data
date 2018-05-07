"""
building structure interafce in PostgreSQL/PostGIS
"""
import pdb
from datetime import datetime
import pandas as pd

from shapely.geometry import LineString, Point, Polygon
from shapely import wkb
import psycopg2
from psycopg2.extras import execute_values
from geoalchemy2.shape import from_shape

from .timeseries_interface.postgres_wrapper import PostgresInterface

class BuildingStructure(PostgresInterface):

    def __init__(self, dbname, user, pw, host, port=5601):
        self.TABLE_NAME = 'building_geometry'
        """
        self.DB_NAME = dbname
        conn_str = "dbname='{dbname}' host='{host}' port='{port}' " \
            .format(dbname=self.DB_NAME, host=host, port=port) + \
            "password='{pw}' user='{user}'".format(user=user, pw=pw)
        self.conn = psycopg2.connect(conn_str)
        """
        super(BuildingStructure, self).__init__(dbname, self.TABLE_NAME,
                                                user, pw, host, port)
        self.create_table()

    def create_table(self):
        # create table
        sql = 'create table if not exists {0} (uri TEXT, geom GEOMETRY);'\
            .format(self.TABLE_NAME)
        cur = self._get_cursor()
        cur.execute(sql)
        self.conn.commit()
        # create index
        cur = self._get_cursor()
        sql = 'create unique index if not exists {0} on {1} (uri)'\
            .format(self.TABLE_NAME + '_uri', self.TABLE_NAME)
        cur.execute(sql)
        self.conn.commit()

    def add_geom(self, uri, geom):
        # Currently having only one geometry per entity. I.e., no versioning
        sql = """
              INSERT INTO {0}(uri, geom)
              VALUES (%(uri)s, %(geom)s::geometry)
              ON CONFLICT (uri) DO UPDATE SET geom = excluded.geom;
              """.format(self.TABLE_NAME)
        geom = [(p[0], p[1]) for p in geom]
        geom = Polygon(geom)
        self._get_cursor().execute(sql, {'geom': geom.wkb_hex, 'uri': uri})
        self.conn.commit()

    def display_data(self, res):
        uris = []
        locs = []
        for row in res:
            [uri, loc] = row
            uris.append(uri)
            locs.append(loc)
        df = pd.DataFrame({
            'uri': uris,
            'loc': locs
        })
        print(df)

    def get_all_data(self, query=''):
        cur = self._get_cursor()
        cur.execute('SELECT uri, ST_AsGeoJson(geom) FROM {0}'.format(self.TABLE_NAME))
        res = cur.fetchall()
        return res

    def query(self, uris=[]):
        qstr = """
SELECT uri, ST_AsGeoJson(geom) FROM {0}
""".format(self.TABLE_NAME)
        if not uris: # If no condition is given.
            qstr += 'DUMY' #dummy characters to be removed.
        else:
            qstr += 'WHERE\n'
            if uris:
                qstr += 'uri IN ({0})\n AND '\
                    .format("'" + "', '".join(uris) + "'")
        qstr = qstr[:-4]
        raw_res = self._exec_query(qstr)
        res = self._format_select_res(raw_res)
        return res

    def _format_select_res(self, raw_res):
        res = raw_res
        return res

    def _make_polygon(self, geom):
        geom = [(p[0], p[1]) for p in geom]
        return Polygon(geom)

    def _encode_loc_data(self, data):
        data = [(datum[0], self._make_polygon(datum[1]).wkb_hex)
                for datum in data]
        return data

    def add_data(self, data):
        cur = self._get_cursor()
        sql = """
              INSERT INTO {0}(uri, loc)
              VALUES %s
              ON CONFLICT (uri) DO UPDATE SET loc = excluded.loc;
              """.format(self.TABLE_NAME)
        encoded_data = self._encode_loc_data(data)
        execute_values(cur, sql, encoded_data)
        self.conn.commit()

if __name__ == '__main__':
    dbname = 'brick'
    user = 'bricker'
    pw = 'brick-demo'
    host = 'localhost'
    port = 6001
    struct_db = BuildingStructure(dbname, user, pw, host, port)

    room_geom_uri = 'http://example.com/room101_geom'
    room_geom = [(0.00005, 0.00005),
                 (0.00005, 0.000075),
                 (0.000075, 0.000075),
                 (0.000075, 0.00005)]
    struct_db.add_geom(room_geom_uri, room_geom)

    res = struct_db.query()
    struct_db.display_data(res)
    print(res)
