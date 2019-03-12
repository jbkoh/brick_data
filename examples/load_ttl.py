from brick_data.sparql import BrickSparql


brick_endpoint = BrickSparql('http://localhost:8890/sparql', '1.0.3')
brick_endpoint.load_rdffile('ebu3b_brick.ttl')
