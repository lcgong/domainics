# -*- coding: utf-8 -*-

import pytest
import decimal
import re

from urllib.parse import urljoin
from datetime import date, datetime
from string import Formatter

from domainics.tornice.route_path import parse_route_path

def test_parse():

    path = '/a/current'
    pattern, args = parse_route_path('/a/(?:{sn:int}|current)')
    assert args['sn'] == 'int'
    m = re.match(pattern, path)
    assert len(m.groups()) == 1 and m.groups()[0] is None
    assert m.group('sn') is None

    pattern, args = parse_route_path('/a/({sn:int}|current)')
    print(pattern, args)
    assert args['sn'] == 'int'
    m = re.match(pattern, path)
    assert len(m.groups()) == 1 and m.groups()[0] is None
    assert m.group('sn') is None

    path = '/a/1001'
    m = re.match(pattern, path)
    assert m.group('sn') == '1001'

    # -----------------------------------------------------------------
    pattern, args = parse_route_path('/a/{sn:\d+|current}(/c)?')
    print(pattern, args)
    assert 'sn' in args

    path = '/a/1001'
    m = re.match(pattern, path)
    assert len(m.groups()) == 1 and m.groups()[0] == '1001'
    assert m.group('sn') == '1001'

    path = '/a/1002/c'
    m = re.match(pattern, path)
    assert len(m.groups()) == 1 and m.groups()[0] == '1002'
    assert m.group('sn') == '1002'

    path = '/a/current/c'
    m = re.match(pattern, path)
    assert len(m.groups()) == 1 and m.groups()[0] == 'current'
    assert m.group('sn') == 'current'

    # -----------------------------------------------------------------
    pattern, args = parse_route_path('/a/{sn:int}(/{file:path})?')
    pattern = pattern + '$'
    print(pattern, args)
    assert 'sn' in args and 'file' in args

    path = '/a/1001'
    m = re.match(pattern, path)
    assert m.group('sn') == '1001' and m.group('file') is None

    path = '/a/1001/b/c'
    m = re.match(pattern, path)
    assert m.group('sn') == '1001' and m.group('file') == 'b/c'

    with pytest.raises(SyntaxError):
        parse_route_path('(?<name>\d+)/{sid}/{sid}')
