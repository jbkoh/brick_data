import aiosparql
import pdb
from aiosparql.client import SPARQLClient

from .brick_endpoint import BrickSparql


class BrickSparqlAsync(BrickSparql):

    def __init__(self, sparql_url, *args, **kwargs):
        super(BrickSparqlAsync, self).__init__(sparql_url, *args, **kwargs)
        self.sparql = SPARQLClient(sparql_url)

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
