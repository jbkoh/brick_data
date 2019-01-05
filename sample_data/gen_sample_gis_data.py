import pdb
import json

import sys
import os.path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from brick_data.sparqlwrapper_brick import BrickEndpoint
from brick_data.timeseries import BrickTimeseries
from brick_data.building_structure import BuildingStructure

base_ns = 'http://example.com/'

### Sample Entities
jane = 'jane'
jane_uri = base_ns + jane
jane_loc = 'jane_loc'
room101 = 'room101'
room101_geom = 'room101_geom'

# Init DBs
dbname = 'brick'
user = 'bricker'
pw = 'brick-demo'
host = 'localhost'
port = 6001
brick_ts = BrickTimeseries(dbname, user, pw, host, port)
struct_db = BuildingStructure(dbname, user, pw, host, port)
sparql = BrickEndpoint('http://localhost:8890/sparql', '1.0.3', base_ns)


# Add entities to Brick.
person_uri = sparql.add_brick_instance(jane, 'Person')
person_loc_uri = sparql.add_brick_instance(jane_loc, 'LocationTrackSensor')
sparql.add_triple(person_uri, 'bf:hasPoint', person_loc_uri)

room_uri = sparql.add_brick_instance(room101, 'Room')
room_geom_uri = sparql.add_brick_instance(room101_geom, 'Geometry')
sparql.add_triple(room_uri, 'bf:hasGeometry', room_geom_uri)


# Add person tracking timeseries data
# She is linearly moving from (0, 0) to (0.0001, 0.0001).
max_steps = 100
base_time = 1525578457
delta_t = 5 # seconds
ts = [base_time + delta_t * i for i in range(0, max_steps)]
base_pos = [0,0]
step = 0.000001
positions = [[step * i]*2 for i in range(0, max_steps)]
data = [[jane_loc, t, pos] for t, pos in zip(ts, positions)]
brick_ts.add_data(data, 'loc')


# Add geometry to the structure server
room_geom = [
    [0.00005, 0.00005],
    [0.00005, 0.000075],
    [0.000075, 0.000075],
    [0.000075, 0.00005]
]

struct_db.add_geom(room_geom_uri, room_geom)


### Basic Tests

# Test if the entities are defined
entity_query = """
select ?s where {
    {?s a brick:Person .}
    UNION
    {?s a brick:LocationTrackSensor .}
    UNION
    {?s a brick:Room .}
    UNION
    {?s a brick:Geometry .}
}
"""
res = sparql.query(entity_query)
assert res[1]
print(res)


# Test if the data of LocationTrack are inserted
begin_time = 1525578457
end_time = begin_time + 150
res = brick_ts.query(begin_time, end_time)
assert res
print(res)

# Test if the data of a room is inserted
res = struct_db.query([room_geom_uri])
assert res
print(res)
