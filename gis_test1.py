import pdb

from brick_data.timeseries import BrickTimeseries
from brick_data.building_structure import BuildingStructure
from brick_data.sparql import BrickEndpoint
from brick_data.queryprocessor.queryprocessor import QueryProcessor
from brick_data.queryprocessor.querysynthesizer import *
from brick_data.common import TS_DB, BRICK_DB, STRUCT_DB


# Init dbs
sparql = BrickEndpoint('http://localhost:8890/sparql', '1.0.3')
sparql.load_schema()
dbname = 'brick'
user = 'bricker'
pw = 'brick-demo'
host = 'localhost'
port = 6001
brick_ts = BrickTimeseries(dbname, user, pw, host, port)
struct_db = BuildingStructure(dbname, user, pw, host, port)

dbs = {
    BRICK_DB: sparql,
    TS_DB: brick_ts,
    STRUCT_DB: struct_db,
}

common_vars = (('?person_loc',), ('?room_geom',))

person_query = """
select ?person_loc where {
  <http://example.com/jane>   bf:hasPoint ?person_loc.
  ?person_loc a brick:LocationTrackSensor .
}
"""
room_geom_query = """
select ?room_geom_uri where {
  :room101 bf:hasGeometry ?room_geom_uri.
  }
"""

begin_time = '2018-05-05 20:47:37'
end_time_false = '2018-05-05 20:49:37'
end_time_true = '2018-05-05 20:55:37'

gis_query1 = """
select exists (
      select 1 from brick_data, building_geometry
      where
      brick_data.uuid = '?person_loc' AND
      brick_data.time >= '2018-05-05 20:47:37' AND
      brick_data.time < '2018-05-05 20:59:53' AND
      building_geometry.uri='?room_geom' AND
      ST_Within(brick_data.loc, building_geometry.geom)
);
"""

queries = [
    (BRICK_DB, person_query),
    (BRICK_DB, room_geom_query),
    (TS_DB, gis_query1)
]
synthesizers = {
    BRICK_DB: BrickSynthesizer(),
    TS_DB: TimescaledbSynthesizer(),
    STRUCT_DB: None,
}

query = {
    'common_variables': common_vars,
    'queries': queries
}

proc = QueryProcessor(dbs, synthesizers)
proc.query(query)
