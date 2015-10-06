# -*- coding: utf-8 -*-

import pytest

from domainics.db import set_dsn, transaction, dbc

def setup_module(module):
    print()
    set_dsn(sys='postgres', database="ci_test", host='localhost', user='dbo')


@transaction
def get_list():
  dbc << "SELECT * FROM (VALUES (1,2), (20,30)) s(a, b)"
  for r in dbc:
  	yield r

@transaction
def test_gen():
    data = []
    for r in get_list():
        data.append(r)

    assert data[0].a == 1 and data[1].a == 20
