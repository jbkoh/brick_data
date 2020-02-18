import asyncio
from asyncio import FIRST_COMPLETED, ALL_COMPLETED
import pdb

from brick_data.sparql import BrickSparqlAsync

brick_endpoint = BrickSparqlAsync('http://bd-testbed.ucsd.edu:8890/sparql',
                             '1.0.3',
                             graph='http://example.com',
                             base_ns='http://example.com#',
                             )
loop = asyncio.get_event_loop()
brick_endpoint.load_schema()
res = loop.run_until_complete(brick_endpoint.load_schema())

qstr = """
select ?s ?p ?o where {
?s ?p ?o.
}
"""

res = loop.run_until_complete(brick_endpoint.query(qstr))

tasks = []
#tasks.append(brick_endpoint.query(qstr))
fut = brick_endpoint.add_triple(
    'http://example.com#aaa',
    'http://example.com#bbb',
    'http://example.com#ccc',
)
tasks.append(fut)
res = loop.run_until_complete(asyncio.gather(*tasks))


loop.close()

