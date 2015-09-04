#! /usr/bin/python3
# -*- coding: utf-8 -*-

from decimal import Decimal
from datetime import date


from domainics import set_dsn, transaction, dbc
set_dsn(sys='postgres', database="demo", host='localhost', user='postgres')

from domainics import dset


from domainics.dtable import DBSchema, dmerge


from domainics.dtable import dtable, tcol, dsequence, array, json_object

class test_item(dtable):
    sn    = tcol(int, doc='item sn') 
    tags  = tcol(array(str), doc='item tags')
    extra = tcol(json_object, doc='item jsons')

    mat   = tcol(array(int, dimensions=2), doc='matrices')
    jsons = tcol(array(json_object), doc='json array')


    __primary_key__ = [sn] # 


def initdb():
    schema = DBSchema()

    schema.add_module('__main__')
    schema.drop()
    schema.create()


@transaction
def test():

    dbc << """
    INSERT INTO test_item (sn, tags, extra, jsons) VALUES 
        (1000, '{"abc", "xyz"}'::TEXT[], '{"a":"123"}', '{"{}"}'::JSONB[]);
    INSERT INTO test_item (sn, tags, extra) VALUES  
        (1010, '{"orange", "apple"}'::TEXT[], '[{"a":"456"}]');
    """ 


    dbc << 'SELECT * FROM test_item'
    data = dset(test_item, dbc)

    for r in data:
        print(r)


@transaction
def test2():
    s1 =  dset(item_type=test_item)

    x = test_item(sn='123')
    x.sn = 123
    x.mat = [[1,2],[3,4]]
    x.tags = ['tag1', 'tag2']

    x.extra = [{"tom":123}]

    s1.append(x)

    dmerge(s1)

    print('===' * 20)

    dbc << 'SELECT * FROM test_item'
    data = dset(test_item, dbc)
    for r in data:
        print(r)

if __name__ == '__main__':
    initdb()

    test()

    test2()


