import logging

logging.basicConfig(level=logging.DEBUG)

import pytest

from domainics.daemon.server import ApplicationServerProcess


# @pytest.fixture(scope="function")
# def application(request):
#     return None


@pytest.fixture(scope="function")
def app_url(request, application):

    server = ApplicationServerProcess(application)
    server.start()

    def finalizer():
        server.stop()
    request.addfinalizer(finalizer)


    return server.home_url
