#! /usr/bin/python3
# -*- coding: utf-8 -*-


# import os
# import sys


from domainics import WebApp, route_base, http_route, rest_route, handler

route_base('/abc', method='GET')


@rest_route('/{sid:int}', method='GET, POST', qargs='page,sort')
def hi(sid, page, data=None):
    # print('123')
    # handler.write("Hello, world, sid=%s, page=%s\n" % (sid, page))  
    # handler.write("data: %r" % get_list())
    print(55, data)

    return {1:3}


if __name__ == '__main__':
    app = WebApp(port=8888)
    # app.add_handler_module('__main__')
    app.add_static_handler('/{path:path}', folder='static', default='/index.html')
    app.setup()

    import domainics.ioloop
    domainics.ioloop.run() # 服务主调度
