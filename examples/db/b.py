#! /usr/bin/python3
# -*- coding: utf-8 -*-

from decimal import Decimal
from datetime import date


from domainics import set_dsn, transaction, dbc
set_dsn(sys='postgres', database="demo", host='localhost', user='postgres')


@transaction
def test():
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
    for r in dbc:
        print(r)



if __name__ == '__main__':
    test()


