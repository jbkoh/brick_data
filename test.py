#insert_data

from brick_server.sparqlwrapper_brick import BrickEndpoint




endpoint = BrickEndpoint('http://localhost:8890/sparql', '1.0.3')

variables = ['?znt', '?cc']
brick_query = """
    select ?znt ?cc:w
    where {
        ?s rdfs:subClassOf* brick:Zone_Temperature_Sensor .
    }
    """

ts_query = """
    SELECT * FROM brick_data ORDER BY time DESC LIMIT 100;
"""


def get_random_data(num_rooms, endpoint):
    for room_num in range(0, num_rooms):
        room = 'room_{0}'.format(room_num)
        znt = 'znt_{0}'.format(room_num)
        cc = 'cc_{0}'.format(room_num)
        endpoint.add_instance(room, 'Room')
        endpoint.add_instance(znt, 'Zone_Temperature_Sensor')
        endpoint.add_instance(cc, 'Cooling_Command')

if __name__ == '__main__':
    endpoint.load_schema()
    get_random_data(2, endpoint)
