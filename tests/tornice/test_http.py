# -*- coding: utf-8 -*-

import pytest
import decimal
from urllib.parse import urljoin
from datetime import date, datetime


from domainics.domobj import DSet, DObject, dset, dobject, datt
from domainics.tornice import rest, route_base
from domainics.tornice.client import rest_client, restcli

route_base('sys/')

class A(dobject):
    sn = datt(str)
    name = datt(str)

AA = A._re(_key=['sn'])

@rest.GET(r'abc/{sn:int}/{path:path}')
def hello(sn: int, name, val: float, d1: datetime, path: str, obj: DSet[AA]):
    print(8888, sn,  path, name)
    print(8888, val, d1,  obj)
    return sn


@rest_client
def test_hi(app_url):
    restcli.base_url = urljoin(app_url, '/api-test/v1/sys/')

    data = [{'sn':1021, 'name':'test-a'}]
    data = restcli.get('abc/%d/abc/ds' % 1001,
                            qs_args=dict(name='testd',
                            val=12.34,
                            d1='2014-12-31T09:43:23'),
                            json_args=data)

    print(data)
