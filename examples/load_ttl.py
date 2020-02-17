from io import StringIO
import pdb

from brick_data.sparql import BrickSparql


brick_endpoint = BrickSparql('http://bd-testbed.ucsd.edu:8890/sparql',
                             '1.0.3',
                             graph='http://example.com',
                             base_ns='http://example.com#',
                             )
with open('examples/bldg.ttl', 'r') as fp:
    ttl_io = StringIO(fp.read())
    brick_endpoint.load_rdffile(ttl_io)
