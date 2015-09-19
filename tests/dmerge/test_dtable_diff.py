# -*- coding: utf-8 -*-

import pytest

from datetime import datetime, date
from decimal import Decimal
from domainics.db import dtable, tcol
from domainics.domobj import dset, reshape



def setup_module(module):
    print()

class t_bill_item(dtable):
    bill_sn = tcol(int)
    line_no = tcol(int, doc='serial no')
    title = tcol(str)
    price = tcol(Decimal)
    notes = tcol(str)

    __primary_key__ = [bill_sn, line_no]

def test_diff():

    ds = dset(t_bill_item, [
            t_bill_item(1001, 1, 'Apple', '2.3', 'note...1'),
            t_bill_item(1002, 2, 'Orange', '4.3', 'note...1'),
            ])

    from domainics.db.dmerge import _dtable_diff as diff

    reshape(t_bill_item)

    result = diff(ds)
    print(result)
