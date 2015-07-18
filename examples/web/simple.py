#! /usr/bin/python3
# -*- coding: utf-8 -*-


# import os
# import sys


from domainics import WebApp, route_base, http_route, rest_route, handler

route_base('/abc', method='GET')

from decimal import Decimal
from domainics import dobject, dset, datt, dident

class BillItem(dobject):
	item_no = datt(str)
	name    = datt(str)
	qty     = datt(Decimal)

	dident(item_no)

class Bill(dobject):
	bill_no = datt(str)
	items   = dset(BillItem)
	notes   = datt(str)

	dident(bill_no)

@rest_route('/rest/{sid:int}', method='GET, POST', qargs='page,sort')
def hi(sid, page, jsonbody):

    print(55, jsonbody, handler.request.body)

    bill = Bill(bill_no='PO#001-%05d' % sid , notes='something...', 
    	items=[
		    BillItem(item_no='001', name='item-a', qty=100),
		    BillItem(item_no='002', name='item-b', qty=100),
		    BillItem(item_no='003', name='item-c', qty=100),
    ])

    raise ValueError('2334')


    return bill


@http_route('/http/{sid:int}', method='GET, POST', qargs='page,sort')
def hi(sid, page, jsonbody):
	handler.write('hi %d, at page %s' % (sid, page))
	
	handler.principal = '123'
	print(handler.principal)
	assert handler.principal == '123'
	handler.principal = None
	assert handler.principal



if __name__ == '__main__':
    app = WebApp(port=8888)
    # app.add_handler_module('__main__')
    app.add_static_handler('/{:path}', folder='static', default='/index.html')
    app.settings['cookie_secret'] = '__cookie_secret__'
    app.main()

