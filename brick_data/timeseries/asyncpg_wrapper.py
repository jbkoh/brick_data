from datetime import datetime
import pdb
import asyncio
import pytz

import pandas as pd
from shapely.geometry import Point
import asyncpg

POSTGRESQL_LOC = 'ST_AsGeoJson(loc)'

def encode_loc_type(value_type):
    if value_type == 'loc':
        return POSTGRESQL_LOC
    else:
        return value_type

def striding_windows(l, w_size):
    curr_idx = 0
    while curr_idx < len(l):
        yield l[curr_idx:curr_idx + w_size]
        curr_idx += w_size


class AsyncpgTimeseries(object):
    def __init__(self, dbname, user, pw, host, port=5601, pool_config={}):
        self.DB_NAME = dbname
        self.TABLE_NAME = 'brick_data'
        self.conn_str = f'postgres://{user}:{pw}@{host}:{port}/{dbname}'
        self.value_cols = ['number', 'text', 'loc']
        self.pagination_size = 500

    async def init(self, **pool_config):
        self.pool = await asyncpg.create_pool(dsn=self.conn_str, **pool_config)
        await self._init_table()
        print('Timeseries Initialized')

    async def _init_table(self):
        qstrs = [
            """
            CREATE TABLE IF NOT EXISTS brick_data (
                uuid TEXT NOT NULL,
                --time TIMESTAMP without time zone NOT NULL,
                time TIMESTAMP NOT NULL,
                number DOUBLE PRECISION,
                text TEXT,
                loc geometry(Point,4326),
                PRIMARY KEY (uuid, time)
            );
            """,

            """
            CREATE INDEX IF NOT EXISTS brick_data_time_index ON brick_data
            (
                time DESC
            );
            """
        ]
        async with self.pool.acquire() as conn:
            for qstr in qstrs:
                res = await conn.execute(qstr)
        print('init table')

    def display_data(self, res):
        times = []
        uuids = []
        numbers = []
        texts = []
        locs = []
        for row in res:
            [uuid, t, number, text, loc] = row
            times.append(t)
            uuids.append(uuid)
            numbers.append(number)
            texts.append(test)
            locs.append(loc)
        df = pd.DataFrame({
            'time': times,
            'uuid': uuids,
            'number': numbers,
            'loc': locs
        })
        print(df)

    def serialize_records(self, records):
        return [tuple(row) for row in records]

    async def get_all_data(self, query=''):
        return await self._fetch("""
                                 SELECT uuid, time, number, text, ST_AsGeoJson(loc)
                                 FROM {0}
                                 """.format(self.TABLE_NAME)
                                 )

    def _format_select_res(self, res, return_type=None):
        if not return_type:
            pass
        elif return_type == 'sparql-like':
            var_begin = qstr.lower().index('select') + 6
            var_end = qstr.lower().index('from')
            var_names = qstr[var_begin:var_end].split()
            res = {
                'var_names': var_names,
                'tuples': res,
            }
        return res

    async def _fetch(self, qstr, *args, **kwargs):
        async with self.pool.acquire() as conn:
            return self.serialize_records(await conn.fetch(qstr, *args, **kwargs))

    async def _execute(self, qstr, *args, **kwargs):
        async with self.pool.acquire() as conn:
            return await conn.execute(qstr, *args, **kwargs)

    async def _exec_query(self, qstr):
        await self._execute(qstr)
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
        #return datetime.fromtimestamp(ts, tz=pytz.utc)
        # TODO: Debug timezone
        return datetime.fromtimestamp(ts)

    async def raw_query(self, qstr, return_type=None):
        raw_res = self._exec_query(qstr)
        res = self._format_select_res(raw_res, return_type)
        return res

    async def delete(self, uuids, start_time=None, end_time=None):
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
        res = await self._execute(qstr)
        return res


    def encode_value_types(self, value_types):
        return list(map(encode_loc_type, value_types))

    async def query(self, uuids=[], start_time=None, end_time=None, value_types=['number']):
        #qstr = """
        #SELECT uuid, time, number, text, ST_AsGeoJson(loc) FROM {0}
        #""".format(self.TABLE_NAME)
        assert value_types
        qstr = """
        SELECT uuid, time, {value_types} FROM {table}
        """.format(value_types=', '.join(value_types), table=self.TABLE_NAME)
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
        return await self._fetch(qstr)

    # TODO: Unify encode & add_data over different data types.


    def _encode_number_data(self, data):
        return [(datum[0], self._timestamp2str(datum[1]), datum[2]) for datum in data]

    def _encode_text_data(self, data):
        return [(datum[0], self._timestamp2str(datum[1]), str(datum[2])) for datum in data]

    def _encode_loc_data(self, data):
        data = [(datum[0],
                 self._timestamp2str(datum[1]),
                 Point((datum[2][0], datum[2][1])).wkb_hex
                 ) for datum in data]
        return data

    async def _add_number_data(self, data):
        sql = """
              INSERT INTO {0}(uuid, time, number)
              VALUES ($1, $2, $3)

              ON CONFLICT (time, uuid) DO UPDATE SET number = excluded.number;
              """.format(self.TABLE_NAME)
        encoded_data = self._encode_number_data(data)
        await self._paginate_executemany(sql, encoded_data)

    async def _add_text_data(self, data):
        sql = """
              INSERT INTO {0}(uuid, time, text)
              VALUES ($1, $2, $3)

              ON CONFLICT (time, uuid) DO UPDATE SET text = excluded.text;
              """.format(self.TABLE_NAME)
        encoded_data = self._encode_text_data(data)
        await self._paginate_executemany(sql, encoded_data)

    async def _paginate_executemany(self, sql, encoded_data):
        async with self.pool.acquire() as conn:
            for data_window in striding_windows(encoded_data, self.pagination_size):
                await conn.executemany(sql, data_window)


    async def _add_loc_data(self, data):
        cur = self._get_cursor()
        sql = """
              INSERT INTO {0}(uuid, time, loc)
              VALUES %s
              ON CONFLICT (time, uuid) DO UPDATE SET loc = excluded.loc;
              """.format(self.TABLE_NAME)
        encoded_data = self._encode_loc_data(data)
        self._execute(sql, encoded_data)

    async def add_data(self, data, data_type='number'):
        """
        - input
            - uuid (str): a unique id of one sensor
            - data (list(tuple)): timeseries data. E.g., [(1055151, 70.0), 1055153, 70.1)]
        """
        assert data_type in self.value_cols # TODO: Make these ENUM.

        if not data:
            raise Exception('Empty data to insert')
        if data_type == 'number':
            await self._add_number_data(data)
        elif data_type == 'loc':
            self._add_loc_data(data)
        elif data_type == 'text':
            self._add_text_data(data)


if __name__ == '__main__':
    dbname = 'brick'
    user = 'bricker'
    pw = 'brick-demo'
    host = 'localhost'
    port = 6001
    brick_ts = AsyncpgTimeseries(dbname, user, pw, host, port)

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
