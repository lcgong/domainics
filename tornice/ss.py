
import multiprocessing
import time
import sys


import logging




def run_webapp_process(port, handlers, settings):
    import tornado.web

    application = tornado.web.Application(handlers, **settings)
    application.listen(port) 

    import logging
    logging.getLogger("tornado.access").setLevel(logging.INFO)
    logging.getLogger("tornado.application").setLevel(logging.DEBUG)
    logging.getLogger("tornado.general").setLevel(logging.INFO)


    import tornado.ioloop
    server = tornado.ioloop.IOLoop.instance() 
    
    # 登记对CTRL-C消息的服务退出处理 
    import signal
    signal.signal(signal.SIGINT, lambda s, f: server.stop())

    print('Server[%d] Started. ' % port)
    server.start() # 启动服务器 
    print('Server[%d] stopped' % port) 


    # import atexit
    # atexit.register(goodbye)


def main_loop(start_port=8001, num_port=2, handlers=None, settings=None):
    import signal
    from collections import namedtuple


    Job = namedtuple('Job', ['name', 'proc', 'port'])
    def create_job(port) :
        name = 'web-%d' % port
        proc = multiprocessing.Process(name=name, 
            target=run_webapp_process, args=(port, handlers, settings))
        proc.daemon = None
        proc.start()
        return Job(name, proc, port)


    jobs = [create_job(start_port + i) for i in range(num_port)]


    is_running = True
    def handle_sigint(s, f):
        print('stopping')
        nonlocal is_running 
        is_running = False

    signal.signal(signal.SIGINT, handle_sigint)

    while is_running:
        for i in range(len(jobs)):
            job = jobs[i]
            if is_running and not job.proc.is_alive() :
                print('dead:', job.name)
                job = create_job(job.port)
                jobs[i] = job
        try:
            time.sleep(0.5)
        except InterruptedError:
            pass





import tornado.web
# HTTP请求处理器(HTTP Request Handler)
class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world1") 

handlers = [ 
    (r"/", MainHandler), 
]
settings = {'debug' : True}


if __name__ == '__main__':
    # multiprocessing.set_start_method('spawn')


    main_loop(handlers=handlers, settings=settings)
