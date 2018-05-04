import pdb

from querysynthesizer import TimescaledbSynthesizer, BrickSynthesizer

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

TS_DB = 'timescaledb'
BRICK_DB = 'brickdb'

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

    def synthesize_query(self, db_name, curr_result, qstr):
        # Naive implementation for TimescaleDB only for now
        if db_name == TS_DB:
            data = curr_result[BRICK_DB]
            new_qstr = self.synthesizers[TS_DB].synthesize(qstr, 'uuid', data)
        else:
            new_qstr = qstr
        return new_qstr



    def plan_query(self, query):
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
        for db_name in [BRICK_DB, TS_DB]:
            qstr = queries[db_name]
            db = self.dbs[db_name]
            modified_query = self.synthesize_query(db_name, curr_results, qstr)
            #res = db.query_data(modified_query)
            #curr_results[db_name] = res
            pdb.set_trace()

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
        'variables': ['?znt', '?cc'],
        'queries': {
            BRICK_DB: sparql_query,
            TS_DB: ts_query
        }
    }
    dbs = {
        BRICK_DB: 'aaa',
        TS_DB: 'bbb'
    }
    synthesizers = {
        BRICK_DB: BrickSynthesizer(),
        TS_DB: TimescaledbSynthesizer()
    }
    proc = QueryProcessor(dbs, synthesizers)
    proc.plan_query(query)


