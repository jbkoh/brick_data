import pdb
from functools import reduce
from copy import deepcopy

from moz_sql_parser import parse

adder = lambda x,y: x+y

class QuerySynthesizer(object):
    def __init__(self):
        pass

class BrickSynthesizer(QuerySynthesizer):
    def __init__(self):
        super(BrickSynthesizer, self).__init__()

    def synthesize_query(self, qstr, common_vars, curr_vars, curr_values):
        return [qstr]

class TimescaledbSynthesizer(QuerySynthesizer):
    def __init__(self):
        super(TimescaledbSynthesizer, self).__init__()

    def modify_query_dfs(self, parsed, var_type, filters):
        # TODO This should be formalized later.
        for q_k in parsed.keys():
            q_v = parsed[q_k]
            if q_k == 'eq' and isinstance(q_v, list):
                if var_type == 'uuid':
                    var_name = q_v[1]
                    new_statement = ['uuid',
                                     [{'literal': val} for val in values]]
                    del parsed[q_k]
                    parsed['in'] = new_statement
            elif isinstance(q_v, dict):
                self.inplace_replace_dfs(q_v, var_type, filters)
            elif isinstance(q_v, list):
                parsed[q_k] = [self.inplace_replace_dfs(q_v_elem)
                               for q_v_elem in q_v]
        return parsed

    def naive_replace(self, qstr, var_type, filters):
        """
        Many assumptions on qstr
        1. each line has one statement.
        """
        variables = filters.keys()
        lines = qstr.split('\n')
        for i, line in enumerate(lines):
            for var_name, values in filters.items():
                if var_name in line:
                    if var_type == 'uuid':
                        lines[i] = 'uuid IN ({0})'.format(str(values)[1:-1])
        return '\n'.join(lines)


    def synthesize_query(self, qstr, common_vars, curr_vars, curr_values):
        """
        """
        common_var_idxs = [curr_vars.index(common_var) for common_var in common_vars]
        #value_template = [None] * len(var_locs)

        #found_values_list = []
        #prev_found_values_list = [deepcopy(value_template)]
        #for common_vars, common_values in zip(common_vars_set, common_values_set):
        res_qstrs = []
        #for found_values in found_values_list:
        for value_tuple in curr_values:
            q = qstr
            for common_var_idx, common_var in zip(common_var_idxs, common_vars):
                q = q.replace(common_var, value_tuple[common_var_idx])
            res_qstrs.append(q)
        return res_qstrs

    def synthesize_dep(self, qstr, var_type, filters):
        """
        E.g.,
        filters = {
            "uuid": ['uuid1', 'uuid2', ...],
            "time": ['
        # ORed inside the lists.
        # ANDed across different keys.
        """
        synthesized = self.naive_replace(qstr, var_type, filters)
        return synthesized


if __name__ == '__main__':
    synth = TimescaledbSynthesizer()
    #query = 'select (uuid, time, value) from brick_data\nwhere uuid = "?znt"\n'
    #filters = {
    #    '?znt': ['znt1', 'znt2']
    #}
    #res = synth.synthesize(query, 'uuid', filters)
    #print('Original Query: \n{0}\n'.format(query))
    #print('Modeified: \n{0}'.format(res))
    qstr = """select (uuid, time, value) from brick_data
    where
    uuid = '?znt' AND
    time = '?ttt'
    """

    common_vars_set = (('?znt',), ('?ttt',))
    #common_vars_set = (('?znt',), ('?ttt', 'xxx',))
    common_values_set = (
        [('znt1',), ('znt2',)],
        [('ttt1',), ('ttt2',)]
    )

    res = synth.synthesize_query(qstr, common_vars_set, common_values_set)

