import pdb

from .querysynthesizer import TimescaledbSynthesizer, BrickSynthesizer

from ..timeseries_interface import BrickTimeseries
from ..building_structure import BuildingStructure
from ..sparqlwrapper_brick import BrickEndpoint
from ..common import TS_DB, BRICK_DB, STRUCT_DB

"""
All DB interfaces should return UUID or URI tuples in the current result set additionally.
(variable_names per tuple, a list of tuples)
"""

"""
Example queries:
{
    'brick': '''
        select ?znt ?cc where {
            ?znt a brick:Zone_Temperature_Sensor .
            ?cc a brick:Cooling_Command .
            ?znt bf:hasLocation ?loc .
            ?cc bf:hasLocation ?loc .
        }
        ''',
    'timeseries': '''
        select (uuid, time, value) from brick_data
        where
        uuid = ?znt /* this line will be intepreted by the query planner */
        AND
        value > 70
}
"""

class QueryProcessor(object):
    def __init__(self, dbs, synthesizers):
        """
        ## params
            - dbs (dict): key=db type, value: db object
            ```
            dbs = {
                #qtype: db
                'rdf': brick_db,
                'timeseries': ts_db
            }
            ```
        """
        self.dbs = dbs
        self.synthesizers = synthesizers

    def _exec_query(self, qtype, qstr):
        pass

    def exec_queries(self, queries, common_vars):
        tot_res = []
        for i, (db_name, query) in enumerate(queries):
            db = self.dbs[db_name]
            #curr_res = db.

    def synthesize_query(self, db_name, curr_result, qstr):
        # Naive implementation for TimescaleDB only for now
        if db_name == TS_DB:
            data = curr_result[BRICK_DB]
            new_qstr = self.synthesizers[TS_DB].synthesize(qstr, 'uuid', data)
        else:
            new_qstr = qstr
        return new_qstr

    def query(self, query):
        common_vars = query['common_variables']
        pseudo_queries = query['queries']
        common_results = {common_var: [] for common_var in common_vars}
        raw_results = []
        results = []
        for db_name, pseudo_query in pseudo_queries:
            db = self.dbs[db_name]
            synth = self.synthesizers[db_name] # TODO: merge this into the db
            queries = synth.synthesize_query(pseudo_query, common_vars, common_results)
            for db_query in queries: #TODO: currently assuming one query produced
                res = db.raw_query(db_query)
                common_res, raw_res = db.parse_result(res)
            found_vars = tuple(common_res[0])
            common_results[found_vars] = common_res[1]
            raw_results.append(raw_res)

    def plan_query_dep(self, query):
        """
        ## params
        - query (dict): queries in JSON
        """
        vs = query['variables']
        queries = query['queries']
        curr_results = {db_name: None for db_name in queries.keys()}
        # TODO: This is testing. Remove later.
        curr_results[BRICK_DB] = {
            '?znt': ['znt1', 'znt2'],
            '?cc': ['cc1', 'cc2']
        }

        #for db_name, qstr in queries.items():
        planned_queries = []
        for db_name in [BRICK_DB, TS_DB]:
            qstr = queries[db_name]
            db = self.dbs[db_name]
            modified_query = self.synthesize_query(db_name, curr_results, qstr)
            #res = db.query_data(modified_query)
            #curr_results[db_name] = res
            planned_queries.append((db_name, modified_query))
        return planned_queries

if __name__ == '__main__':
    sparql_query = """
        select ?znt ?cc where {
            ?znt a brick:Zone_Temperature_Sensor .
            ?cc a brick:Cooling_Command .
            ?znt bf:hasLocation ?loc .
            ?cc bf:hasLocation ?loc .
        }
        """
    ts_query = """
        select (uuid, time, value) from brick_data
        where
        uuid IN [?znt] // approximate matching
        /* uuid = ?znt  //exact matching */
        AND
        value > 70
        """
    query = {
        'common_variables': ['?znt', '?cc'],
        'queries': {
            BRICK_DB: sparql_query,
            TS_DB: ts_query
        }
    }

    # Init dbs
    sparql_endpoint = BrickEndpoint('http://localhost:8890/sparql', '1.0.3')
    endpoint.load_schema()
    dbname = 'brick'
    user = 'bricker'
    pw = 'brick-demo'
    host = 'localhost'
    port = 6001
    brick_ts = BrickTimeseries(dbname, user, pw, host, port)
    struct_db = BuildingStructure(dbname, user, pw, host, port)

    dbs = {
        BRICK_DB: sparql_endpoint,
        TS_DB: brick_ts,
        STRUCT_DB: struct_db,
    }

    # Adapt query, which is ignored for now. (just bypassing)
    synthesizers = {
        BRICK_DB: BrickSynthesizer(),
        TS_DB: TimescaledbSynthesizer(),
        STRUCT_DB: None,
    }
    proc = QueryProcessor(dbs, synthesizers)
    planned_queries = proc.plan_query(query)
