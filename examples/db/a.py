#! /usr/bin/python3
# -*- coding: utf-8 -*-

from decimal import Decimal
from datetime import date



import datetime as dt
import sys


from domainics.dtable import DBSchema, dmerge
# from domainics.domobj import dobject, dset, identity, datt

from domainics.domobj import dset
from domainics import set_dsn, transaction, dbc
from domainics.dtable import dtable, tcol, dsequence, array
from domainics.db import dbc

set_dsn(sys='postgres', database="demo", host='localhost', user='postgres')

class mm_po_item(dtable):
    po_no   = tcol(str) 
    line_no = tcol(int, doc='line number of P.O.')

    item_sn = tcol(int, doc='')
    item_no = tcol(str, len=12, doc='item number')

    qty     = tcol(Decimal, len=(16,2), doc='')
    price   = tcol(Decimal, len=(16,2))
    notes   = tcol(str, doc='description of item')

    __primary_key__ = [po_no, line_no] # 


class seq_po(dsequence):
    start = 10000
    step  = 1

class vendor_seq(dsequence):
    start = 10000
    step  = 1


class mm_po(dtable):
    po_sn      = tcol(seq_po, nullable=False)
    po_no      = tcol(str,  doc='purchase order number')
    po_date    = tcol(date, doc='P.O. date')
    vendor_sn  = tcol(vendor_seq,  doc='internal sn of vender')
    notes      = tcol(str,  doc='addtional notes')

    tags       = tcol(array(str, dimensions=1), doc='tags')

    __primary_key__ = [po_sn]


class mm_vendor(dtable):
    vendor_sn = tcol(int)





schema = DBSchema()

schema.add_module('__main__')
schema.drop()
schema.create()

@transaction
def test2():
    mm = mm_po(po_no='P003')
    s1 =  dset(item_type=mm_po)
    s1.append(mm_po(po_no='P201'))
    s1.append(mm_po(po_no='P202'))
    s1.append(mm_po(po_no='P203'))

    dmerge(s1)

    s2 = s1.copy()
    s2[0].vendor_sn = vendor_seq()
    s2[1].vendor_sn = vendor_seq()
    dmerge(s2, s1)    
    print(s2)


@transaction
def test():
    s = seq_po()
    # s.value =100
    print('100, ', bool(s))
    s.value = '100'
    print('200, ', bool(s))
    print('ddd %d' % s)

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

    dmerge(s1, None)

    s2 = s1.copy()
    s2[0].notes = 'abc21'

    s2[2].po_date = dt.date(2015, 6, 2)
    s2[2].notes   = 'abc22'

    dmerge(s2, s1)

    s3 = s2.copy()
    del s3[1]
    dmerge(s3, s2)    

    po_no = 'P001'

    dbc << 'SELECT * FROM mm_po WHERE po_no=%s' 
    po_old = dset(mm_po, dbc << (po_no,))

    po_new = po_old.copy()
    po_new[0].notes = 'test2-123'

    dmerge(po_new, po_old)    
    print(po_new)



if __name__ == '__main__':
    # main()
    test2()

