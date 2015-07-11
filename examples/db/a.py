#! /usr/bin/python3
# -*- coding: utf-8 -*-

from decimal import Decimal
from datetime import date



import datetime as dt
import sys

from domainics.domobj import dobject, identity, datt

from domainics.dtable import DBSchema, dtable_merge
from domainics.domobj import dset, dobject
from domainics import set_dsn, transaction, sql, dbc
from domainics.dtable import dtable, dsequence
from domainics.db import dbc

set_dsn(sys='postgres', database="demo", host='localhost', user='postgres')

class tcol(datt):

    def __init__(self, type, len=None, doc=None):
        self.len = len
        super(tcol, self).__init__(type, doc=doc)

class mm_po_item(dtable):
    po_no   = tcol(str) 
    line_no = tcol(int, doc='line number of P.O.')

    item_sn = tcol(int, doc='')
    item_no = tcol(str, len=12, doc='item number')

    qty     = tcol(Decimal, len=(16,2), doc='')
    price   = tcol(Decimal, len=(16,2))
    notes   = tcol(str, doc='description of item')

    identity(po_no, line_no)


class mm_po(dtable):
    po_no         = tcol(str,  doc='purchase order number')
    po_date       = tcol(date, doc='P.O. date')
    vender_sn     = tcol(int,  doc='internal sn of vender')
    notes         = tcol(str,  doc='addtional notes')

    identity(po_no)

class mm_vendor(dtable):
    vendor_sn = tcol(int)

class seq_po(dsequence):
    start = 10000
    step  = 1




schema = DBSchema()

schema.add_module('__main__')
schema.drop()
schema.create()


@transaction
def test():
    mm = mm_po('P003')
    print(mm)

    dbc << 'SELECT %s'
    dbc << (1,)
    dbc << (2,)
    dbc << [(3,), (4,)]

    dbc << ''

    dbc << 'SELECT %s' << (200,)

    dbc << 'select 100'

    s1 =  dset(item_type=mm_po)
    s1.append(mm_po(po_no='P001', po_date=dt.date(2015,7,1), notes='abc'))
    s1.append(mm_po(po_no='P002', po_date=dt.date(2015,7,2), notes='xyz'))
    s1.append(mm_po(po_no='P004', po_date=dt.date(2015,7,4), notes='hij'))
    s1.append(mm_po(po_no='P003', po_date=dt.date(2015,7,3), notes='efg'))

    dtable_merge(s1, None)

    s2 = s1.copy()
    s2[0].notes = 'abc21'

    s2[2].po_date = dt.date(2015, 6, 2)
    s2[2].notes   = 'abc22'

    dtable_merge(s2, s1)

    s3 = s2.copy()
    del s3[1]
    dtable_merge(s3, s2)    

    po_no = 'P001'

    dbc << 'SELECT * FROM mm_po WHERE po_no=%s' 
    po_old = dset(mm_po, dbc << (po_no,))

    po_new = po_old.copy()
    po_new[0].notes = 'test2-123'

    dtable_merge(po_new, po_old)    
    print(po_new)



if __name__ == '__main__':
    # main()
    test()

