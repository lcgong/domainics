# -*- coding: utf-8 -*-

import decimal
import datetime as dt


import pytest

# from domainics.tornice.route import rest, http


import tornado.ioloop
import tornado.web

import multiprocessing

class Handler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")
        print(123)

app = tornado.web.Application([ (r"/", Handler) ])
@pytest.fixture
def application():
    return app


def test_hi(app_url):

    import time
    for i in range(5):
        import urllib.request
        response = urllib.request.urlopen(app_url)
        html = response.read()
        # print(html)

def test_hi2(app_url):

    import time
    for i in range(5):
        import urllib.request
        try :
            response = urllib.request.urlopen(app_url)
            html = response.read()
            print(html)
        except Exception as ex:
            print(ex)


if __name__ == '__main__':
    # from . import server
    pass
    # server.start()
    # test_hi()
    # server.stop()
