#insert_data
import asyncio
import numpy as np
import arrow
import pdb
import random
import sys
sys.path.append('./')

from brick_data.timeseries import AsyncpgTimeseries
from brick_data.sparql import BrickSparql

brick_db = BrickSparql(sparql_url='http://localhost:8890/sparql',
                       brick_version='1.0.3',
                       graph='http://example.com',
                       base_ns='http://example.com#',
                       username='dba',
                       password='dba',
                       )
ts_db = AsyncpgTimeseries(
    dbname='brick',
    user='bricker',
    pw='brick-demo',
    host='localhost',
    port=5432,
)

def gen_random_data(point_type, begin_time, end_time, srcid):
    latency_base = 300 # seconds
    latency_noise_factor = 30 # seconds
    if point_type == 'Zone_Temperature_Sensor':
        max_val = 80
        min_val = 60
        day_interval = 24 * 60 * 60
        noise_size = max_val * 0.01 # ratio
        noiser = np.vectorize(
            lambda x: x + random.random() * noise_size - noise_size / 2)
        data = []
        # Add data with linear interpolation + noise
        t = begin_time
        xs = []
        ys = None
        while t < end_time:
            next_t = t + day_interval
            xp = [t, t + day_interval / 2, next_t]
            yp = [min_val, max_val, min_val]
            x = [point + random.random() * latency_noise_factor
                 - latency_noise_factor / 2
                 for point in range(t, next_t, latency_base)]
            y = noiser(np.interp(x, xp, yp))
            xs += x
            if not ys:
                ys = y
            else:
                ys = np.concatenate([ys, y])
            t = next_t
    data = [(srcid, x, y) for x, y in zip(xs, ys)]
    return data


def gen_random_metadata(num_rooms, brick_db):
    for room_num in range(0, num_rooms):
        room = 'room_{0}'.format(room_num)
        znt = 'znt_{0}'.format(room_num)
        cc = 'cc_{0}'.format(room_num)
        brick_db.add_brick_instance(room, 'Room')
        brick_db.add_brick_instance(znt, 'Zone_Temperature_Sensor')
        brick_db.add_brick_instance(cc, 'Cooling_Command')

async def main():
    gen_random_metadata(2, brick_db)
    begin_time = arrow.get(2018,4,1).timestamp
    end_time = arrow.get(2018,4,2).timestamp
    data = gen_random_data('Zone_Temperature_Sensor', begin_time, end_time, 'znt1')
    await ts_db.add_data(data)
    res = await ts_db.get_all_data()
    assert res
    queried = await ts_db.query(begin_time + 1000, end_time - 1000)
    assert queried
    deleted = await ts_db.delete(['znt1'], begin_time - 1000, end_time + 1000)
    assert int(deleted.split()[-1])
    query_after_del = await ts_db.query(begin_time + 1000, end_time - 1000)
    assert not query_after_del
    return queried, deleted


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    res = loop.run_until_complete(main())


