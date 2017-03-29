# -*- coding: utf-8 -*-

import pytest
from domainics import P, set_dsn, transaction, sqltext

def setup_module(module):
    set_dsn(sys='postgres', database="postgres", host='localhost', user='dbo')
    set_dsn(dsn='testdb', sys='postgres', database="postgres", host='localhost', user='dbo')

@transaction
def test_func1():

    P.sql << "SELECT 1 AS a;"
    r = P.sql.next
    assert r and r.a == 1

    P.sql << "SELECT  2 AS a;"
    r = P.sql.next
    assert r and r.a == 2

@transaction
def test_sqltext_template_style_1():
    sql_values = sqltext(('({i}, { i + 100 })' for i in range(5)), sep=',')
    P.sql << "SELECT * FROM (VALUES {sql_values}) d(s, v);\n"

    rows = list(P.sql)
    assert len(rows) == 5

@transaction
def test_sqltext_template_style_2():
    sql_values = sqltext(sep=',')
    for i in range(10):
        sql_values << '({i}, {i+100})'

    P.sql << "SELECT * FROM (VALUES {sql_values}) d(s, v);\n"
    rows = list(P.sql)
    assert len(rows) == 10 and 45 == sum(r.s for r in rows)

@transaction
@transaction.sql_b(dsn="DEFAULT", autocommit=True)
@transaction.sql_c(dsn="testdb")
def test_triple_connection():
    P.sql << "SELECT 1 as sn, pg_backend_pid() as pid FOR UPDATE;";
    r1 = P.sql.next
    assert r1

    P.sql_b << "SELECT 2 as sn, pg_backend_pid() as pid;";
    r2 = P.sql_b.next
    assert r2

    P.sql_c << "SELECT 3 as sn, pg_backend_pid() as pid;";
    r3 = P.sql_c.next
    assert r3

    assert r1.sn == 1 and r2.sn == 2 and r3.sn == 3
    assert r1.pid != r2.pid and r1.pid != r3.pid and r2.pid != r3.pid

def test_nested_call():
    @transaction
    def get_list():
      P.sql << "SELECT * FROM (VALUES (1, 2), (20, 30)) s(a, b);"
      for r in P.sql:
      	yield r

    @transaction
    def func():
        P.sql << "SELECT 123 AS sn;"
        assert P.sql.next.sn == 123

        data = [r for r in get_list()]
        assert data[0].a == 1 and data[1].a == 20

    func()


@transaction
def test_insert():
    P.sql << """
    CREATE TEMPORARY TABLE _test_sqlblock (
        sn      INTEGER,
        tags    TEXT[],
        data    JSONB
    ) ON COMMIT DROP;
    """

    from collections import namedtuple
    Entity = namedtuple('Entity', ['sn', 'tags', 'data'])

    entities = [
        Entity(1000, ['tag1', 'tag2'], {"name": 'tom'}),
        Entity(1010, ['orange', 'apple'], {"name": 'jerry', "age": 12}),
    ]

    for ent in entities:
        P.sql << """
        INSERT INTO _test_sqlblock (sn, tags, data) VALUES
            ({ent.sn}, {ent.tags}, {ent.data});
        """

    P.sql << "SELECT * FROM _test_sqlblock ORDER BY sn;"
    rows = list(P.sql)
    assert len(rows) == 2
    assert rows[1].sn == 1010 and rows[1].data['name'] == 'jerry'


    P.sql << "DELETE FROM _test_sqlblock;"

    # another way to insert
    sql_values = sqltext(sep=', ')
    for ent in entities:
        sql_values << '({ent.sn}, {ent.tags}, {ent.data})'

    P.sql << 'INSERT INTO _test_sqlblock (sn, tags, data) VALUES {sql_values};'

    P.sql << "SELECT * FROM _test_sqlblock ORDER BY sn;"
    rows = list(r for r in P.sql)
    assert len(rows) == 2
