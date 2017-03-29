# -*- coding: utf-8 -*-

import pytest
from domainics import P, transaction, sqltext
from domainics.db.sqlblock import set_dsn, sqltext
import asyncio

def setup_module(module):
    set_dsn(sys='postgres', database="postgres", host='localhost', user='dbo')

@transaction
def test_func1():

    P.sql << "SELECT 1 AS a;"
    r = P.sql.next
    assert r and r.a == 1

    P.sql << "SELECT  2 AS a;"
    r = P.sql.next
    assert r and r.a == 2

@transaction
def test_sqltext_1():
    sn = 1
    sql_values = sqltext(('({i}, { i + 100 })' for i in range(5)), sep=',')
    P.sql << "SELECT * FROM (VALUES {sql_values}) d(s, v);\n"

    rows = list(P.sql)
    assert len(rows) == 5

@transaction
def test_sqltext_2():
    sql_values = sqltext(sep=',')
    for i in range(10):
        sql_values << '({i}, {i+100})'

    P.sql << "SELECT * FROM (VALUES {sql_values}) d(s, v);\n"
    rows = list(P.sql)
    assert len(rows) == 10 and 45 == sum(r.s for r in rows)

@transaction
@transaction.sql_b(dsn="DEFAULT", autocommit=True)
def test_func3():
    P.sql << "SELECT 1 as sn, pg_backend_pid() as pid FOR UPDATE";
    r1 = P.sql.next
    assert r1

    P.sql_b << "SELECT 2 as sn, pg_backend_pid() as pid";
    r2 = P.sql_b.next
    assert r2

    assert r1.sn == 1 and r2.sn == 2
    assert r1.pid != r2.pid
