import aiosparql
import asyncio
import time
import pdb
from aiosparql.client import SPARQLClient
from io import StringIO

import rdflib
import semver

from .brick_endpoint import BrickSparql, striding_windows


class BrickSparqlAsync(BrickSparql):

    def __init__(self, sparql_url, *args, **kwargs):
        super(BrickSparqlAsync, self).__init__(sparql_url, *args, **kwargs)

    def init_sparql(self, sparql_url, *args, **kwargs):
        self.sparql = SPARQLClient(sparql_url)

    async def load_schema(self):
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
        await asyncio.gather(*futures)

    async def query(self, qstr, graphs=[], is_update=False, is_insert=False, is_delete=False):
        if not graphs and self.base_graph:
            graphs = [self.base_graph]
        if is_insert or is_delete:
            assert is_update
            qstr = self.add_graphs_to_insert_qstr(qstr, graphs)

        qstr = self.q_prefix + qstr

        if not is_update:  # TODO: Implement this for update as well.
            qstr = self.add_graphs_to_select_qstr(qstr, graphs)

        if is_update:
            try:
                res = await self.sparql.update(qstr)
            except:
                time.sleep(0.2)
                res = await self.sparql.update(qstr)
        else:
            try:
                res = await self.sparql.query(qstr)
            except:
                time.sleep(0.2)
                res = await self.sparql.query(qstr)


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
        if not graph:
            graph = self.base_graph
        if (isinstance(f, str) and os.path.isfile(f)) or isinstance(f, StringIO):
            # TODO: Optimize this with using Virtuoso API directly
            new_g = rdflib.Graph()
            new_g.parse(f, format='turtle')
            res = [row for row in new_g.query('select ?s ?p ?o where {?s ?p ?o.}')]
            #futures = []
            #for rows in striding_windows(res, 500):
            #    self.add_triples(rows, graph=graph)
            futures = [self.add_triples(rows, graph=graph) for rows in striding_windows(res, 500)]
            await asyncio.gather(*futures)

        elif isinstance(f, str) and validators.url(f):
            raise Exception('Load ttl not implemented for {0}'.format('url'))
        else:
            raise Exception('Load ttl not implemented for {0}'.format(type(f)))
