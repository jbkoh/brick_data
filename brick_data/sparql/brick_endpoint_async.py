import aiosparql
import asyncio
import pdb
import semver
from aiosparql.client import SPARQLClient

from .brick_endpoint import BrickSparql


class BrickSparqlAsync(BrickSparql):

    def __init__(self, sparql_url, *args, **kwargs):
        super(BrickSparqlAsync, self).__init__(sparql_url, *args, **kwargs)

    def init_sparql(self, sparql_url):
        self.sparql = SPARQLClient(sparql_url)

    def _load_schema(self):
        parsed_brick_version = semver.parse(self.BRICK_VERSION)
        if parsed_brick_version['major'] <= 1 and parsed_brick_version['minor'] < 1:
            schema_ns = [self.BRICK, self.BRICK_USE, self.BF, self.BRICK_TAG]
        else:
            schema_ns = [self.BRICK]
        schema_urls = [str(ns)[:-1] + '.ttl' for ns in schema_ns]
        load_query_template = 'LOAD <{0}> into <{1}>'
        futures = []
        for schema_url in schema_urls:
            qstr = load_query_template.format(schema_url.replace('https', 'http'), self.base_graph)
            futures.append(self.query(qstr, is_update=True))
            #loop.run_until_complete(self.query(qstr, is_update=True))
        #loop = asyncio.get_event_loop()
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*futures))

    async def query(self, qstr, graphs=[], is_update=False, is_insert=False, is_delete=False):
        if not graphs:
            graphs = [self.base_graph]
        if is_insert or is_delete:
            assert is_update
            qstr = self.add_graphs_to_insert_qstr(qstr, graphs)

        qstr = self.q_prefix + qstr

        if not is_update:  # TODO: Implement this for update as well.
            qstr = self.add_graphs_to_select_qstr(qstr, graphs)

        if is_update:
            res = await self.sparql.update(qstr)
            print(res)
        else:
            res = await self.sparql.query(qstr)
            res = self._format_select_res(res)
        return res


    async def add_triple(self, pseudo_s, pseudo_p, pseudo_o, graph=None):
        return await self.add_triples([(pseudo_s, pseudo_p, pseudo_o)], graph)

    async def add_triples(self, pseudo_triples, graph=None):
        if not graph:
            graph = self.base_graph
        triples = [self.make_triple(*pseudo_triple) for pseudo_triple in pseudo_triples]
        q = self._create_insert_query(triples, graph)
        res = await self.query(q, is_update=True)
        return res

    async def load_rdffile(self, f, graph=None):
        super(BrickSparqlAsync, self).load_rdffile(f, graph)
