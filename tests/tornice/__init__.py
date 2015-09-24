



# class Handler(tornado.web.RequestHandler):
#     def get(self):
#         self.write("Hello, world")
#         print(123)
#
# application = tornado.web.Application([ (r"/", Handler) ])

# class TornadoStop(Exception):
#     pass

# def stop():
#     raise TornadoStop



import pytest


#
# def setup_module(module):
#     print(1)
#     server.start()
#
# def teardown_module(module):
#     server.stop()


# class Handler(tornado.web.RequestHandler):
#     def get(self):
#         self.write("Hello, world")
#         print(123)
#
# application = tornado.web.Application([ (r"/", Handler) ])
# application.listen(9999)
# class ThreadedServer:
#
#     def run(self):
#
#         tornado.ioloop.IOLoop.instance().start()
#
#     def start(self):
#         import threading
#         threading.Thread(target=self.run).start()
#
#     def stop(self):
#         # make thread-safty
#         ioloop = tornado.ioloop.IOLoop.instance()
#         ioloop.add_callback(ioloop.stop)
#
#
# server = ThreadedServer()
# def setup_module(module):
#     server.start()
#
# def teardown_module(module):
#     server.stop()
