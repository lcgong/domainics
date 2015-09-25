# -*- coding: utf-8 -*-

import pytest
import logging
logging.basicConfig(level=logging.DEBUG)

@pytest.fixture
def application(request): # create a default web application
    from domainics.tornice import Application
    app = Application(debug=True)
    app.add_module(request.module)
    app.log_route_handlers()

    return app

@pytest.fixture(scope="function")
def app_url(request, application):

    from domainics.daemon.server import ApplicationServerProcess
    server = ApplicationServerProcess(application, port=8888)
    server.start()

    def finalizer():
        server.stop()
    request.addfinalizer(finalizer)

    return server.home_url
