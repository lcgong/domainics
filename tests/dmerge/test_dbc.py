# -*- coding: utf-8 -*-

import pytest

from domainics.db import set_dsn, transaction, dbc

def setup_module(module):
    print()
    set_dsn(sys='postgres', database="ci_test", host='localhost', user='dbo')


@transaction
def test_dbc():
    dbc << """
    DROP TABLE IF EXISTS test_json;
    CREATE TABLE test_json (
        sn      INTEGER,
        tags    TEXT[],
        data    JSONB
    );

    INSERT INTO test_json (sn, tags, data) VALUES
        (1000, '{"abc", "xyz"}'::TEXT[], '{"a":"123"}');
    INSERT INTO test_json (sn, tags, data) VALUES
        (1010, '{"orange", "apple"}'::TEXT[], '[{"a":"456"}]');
    """

    dbc << 'SELECT * FROM test_json'
    rows = list(r for r in dbc)
    print(rows)

    assert rows[1].data == [{"a":"456"}]
    
