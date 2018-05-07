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

    def synthesize_query(self, qstr, common_vars_set, common_values_set):
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


    def synthesize_query(self, qstr, common_vars_set, common_values_set):
        """
        Return: Synthesized queries to execute.
        Notes
        1. common_vars and common_values are synchronized.
            I.e., they should be zippable.
        2. Variables in common_vars_set should be unique.
            I.e., A variable occurs once.
        """
        found_vars = [var for var in reduce(adder, common_vars_set, ())
                      if var in qstr]
        var_locs = {var: i for i, var in enumerate(found_vars)}
        value_template = [None] * len(var_locs)

        found_values_list = []
        prev_found_values_list = [deepcopy(value_template)]
        for common_vars, common_values \
                in zip(common_vars_set, common_values_set):
            for value_tuple in common_values:
                for var, val in zip(common_vars, value_tuple):
                    if var in found_vars:
                        for prev_found_value in prev_found_values_list:
                            updated_value = deepcopy(prev_found_value)
                            updated_value[var_locs[var]] = val
                            found_values_list.append(updated_value)
            prev_found_values_list = found_values_list
            found_values_list = []
        found_values_list = prev_found_values_list
        res_qstrs = []
        for found_values in found_values_list:
            q = qstr
            for var, found_value in zip(found_vars, found_values):
                q = q.replace(var, found_value)
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

