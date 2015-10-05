# -*- coding: utf-8 -*-

from collections import OrderedDict
from string import Formatter
import re

_route_rule_types = {
    'str'  : r'[^/]+',
    'int'  : r'-?\d+',
    'float': r'-?[\d.]+',
    'path' : r'.+?'
}

def parse_route_path(path):
    fmt = Formatter()

    rule = turn_noncapturing(path)

    arguments = OrderedDict()
    pattern = ''
    for literal_text, field_name, format_spec, conversion in fmt.parse(rule):
        pattern += literal_text
        if field_name is None:
            continue

        format_spec = format_spec.lower()
        subpattern = _route_rule_types.get(format_spec, None)
        if subpattern is None:
            subpattern = _route_rule_types.get('str')

        if field_name in arguments:
            err = "The argument \{%s:%s\} are already defined in %s"
            err %= (field_name, format_spec, path)
            raise SyntaxError(err)

        arguments[field_name] = format_spec

        pattern += '(?P<' + field_name + '>' + subpattern + ')'

    return pattern, arguments

def turn_noncapturing(pattern):
    """
    Turn all capturing groups in a regex pattern into non-capturing groups.
    """

    if '(' not in pattern:
        return pattern

    def subst_func(m):
        if len(m.group(1)) % 2:
            return m.group(0)
        else:
            return m.group(1) + '(?:'

    return re.sub(r'(\\*)(\(\?P<[^}]+>|\((?!\?))', subst_func, pattern)
