# -*- coding: utf-8 -*-

import pytest
import decimal
from urllib.parse import urljoin
from datetime import date, datetime

from domainics.domobj import DPage, DSet, dobject, datt, dset
from domainics.db import dtable
from domainics.tornice import rest, route_base
from domainics.tornice.client import rest_client, restcli

from domainics.domobj.pagination import parse_header_range

route_base('/tests/')

class test_a(dtable):
    sn = datt(int)
    line = datt(int)
    name = datt(str)

    __dobject_key__ = [sn, line]


@rest.GET(r'case1/')
def case1(page: DPage)  :
    print('paginaton: ', page)

    assert len(page.sortable) == 2
    assert page.start == 0 and page.limit == 25
    assert page.sortable[0].name == 'sn' and page.sortable[0].ascending

    page.next(2)

    assert page.start == 50 and page.limit == 25


@rest_client
def test_case1(app_url):
    restcli.base_url = urljoin(app_url, '/tests/')

    data = restcli.get('case1/',
                        qs_args=dict(range='0-24@+sn,-date'))



class test_a(dtable):
    sn = datt(int)
    line = datt(int)
    name = datt(str)

    __dobject_key__ = [sn, line]


@rest.GET(r'case2/')
def case2(page: DPage) -> DSet[test_a] :
    print(3444, page)
    page = DPage(start=3, limit=5, sortable='+sn,-line')

    ASet = dset(test_a, _key=dict(sn=datt(int)))
    ds1 = ASet(sn=101, _page=page)
    for i in range(3,8):
        ds1 += [test_a(line=i, name='L%03d' % i)]

    return ds1

@rest_client
def test_case2(app_url):
    restcli.base_url = urljoin(app_url, '/tests/')

    page = DPage(start=2, limit=4, sortable='+sn,-line')
    data = restcli.get('case2/', page=page)
    content_range = restcli.response.headers.get('Content-Range', None)
    assert content_range
    start, limit, total, sortable = parse_header_range(content_range)
    assert start == 3 and limit == 5 and sortable == '+sn,-line'
