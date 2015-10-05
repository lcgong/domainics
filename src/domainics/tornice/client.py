# -*- coding: utf-8 -*-

import logging
import functools
import http.client
import urllib.parse

import urllib.request
from http.cookiejar import CookieJar, DefaultCookiePolicy
from urllib.request import HTTPCookieProcessor, build_opener
from urllib.parse import urljoin

from .. import json as _json
from ..pillar import _pillar_history, pillar_class, PillarError

import urllib.error


from ..exception import UnauthorizedError, ForbiddenError, BusinessLogicError


class RESTfulClient:

    def __init__(self, base_url):
        self._base_url = base_url

        self.headers = {}

        self.__response = None

        policy = DefaultCookiePolicy(rfc2965=True,
                        strict_ns_domain=DefaultCookiePolicy.DomainStrict)
        self._opener = build_opener(HTTPCookieProcessor(CookieJar(policy)))

        self._response = None

    @property
    def base_url(self):
        return self._base_url

    @base_url.setter
    def base_url(self, url):
        self._base_url = urljoin(self._base_url, url)

    def request(self, method, url=None, url_args=None, qs_args=None,
                post_args=None, json_args=None) :

        if url_args is not None:
            # apply the arguments in URL
            assert isinstance(url_args, dict)

            if self._base_url is not None:
                base_url = self._base_url.format(**url_args)
            else:
                base_url = None

            if url is not None:
                req_url  = url.format(**url_args)
        else:
            base_url = self.base_url
            req_url  = url

        req_url = urljoin(base_url, req_url)

        if qs_args is not None:
            assert isinstance(qs_args, dict)
            req_url = urljoin(req_url,  '?' + urllib.parse.urlencode(qs_args))

        assert post_args is None or json_args is None

        headers = dict(self.headers)
        body    = None
        if json_args is not None:
            headers['Content-type'] = 'application/json'
            body = _json.dumps(json_args).encode('UTF-8')

        if post_args is not None:
            headers['Content-type'] = 'application/x-www-form-urlencoded'
            body = urllib.parse.urlencode(post_args).encode('UTF-8')


        req = urllib.request.Request(req_url, body, headers=headers)
        req.get_method = lambda : method.upper()

        self._request = req

        #
        try :
            res = self._opener.open(self._request)
            data = self.decode_data(res)
            self._response = RESTfulClient.Response(res, data)
            return self._response.data
        except urllib.error.HTTPError as ex:

            if ex.headers.get_content_type() == 'application/json':
                res = ex
                data = self.decode_data(ex)
                data = data[0]

                errmsg = "HTTP ERROR(%d): %s\n"
                errmsg += "Request: %s handled by %s\n"
                errmsg += 'Caught Exception %s : %s\n'
                errmsg %= (data['status_code'], ex.reason,
                            req_url, data['handler'],
                            data['exception'], data['message'])

                errmsg += '\n'.join(["    at %s\n        %s"
                                        % (ln['at'], ln['code'])
                                            for ln in data['traceback']])
                self.logger.error(errmsg)

                if ex.status == 401 :
                    raise UnauthorizedError(data['message']) from ex

                elif ex.status == 403 :
                    raise UnauthorizedError(data['message']) from ex

                elif ex.status == 409 :
                    raise BusinessLogicError(data['message']) from ex
            else:
                charset = ex.headers.get_content_charset()
                if charset is None:
                    charset = "UTF-8"

                data = ex.read().decode(charset)
                errmsg = "HTTP ERROR(%d): %s\nRequest: %s\n%s"
                errmsg %= (ex.status, ex.reason, ex.geturl(), data)
                self.logger.error(errmsg, exc_info=ex)

            raise ex

    def get(self, url=None, url_args=None, qs_args=None,
                    post_args=None, json_args=None):

        return self.request('GET', url, url_args, qs_args, post_args, json_args)

    def post(self, url=None, url_args=None, qs_args=None,
                    post_args=None, json_args=None):

        return self.request('POST', url, url_args, qs_args, post_args, json_args)

    def put(self, url=None, url_args=None, qs_args=None,
                    post_args=None, json_args=None):
        return self.request('PUT', url, url_args, qs_args, post_args, json_args)

    def delete(self, url=None, url_args=None, qs_args=None,
                    post_args=None, json_args=None):
        return self.request('DELETE', url, url_args, qs_args,
                                post_args, json_args)

    @property
    def response(self):
        return self._response


    @staticmethod
    def decode_data(response):
        data = response.read()

        headers = response.headers
        content_type = headers.get_content_type()
        charset = headers.get_content_charset()
        if charset is None:
            charset = 'UTF-8'

        if content_type == 'application/json':
            data = _json.loads(data.decode(charset))
        elif content_type == 'text/plain':
            data = data.decode(charset)

        return data

    class Response:

        def __init__(self, response, data):
            self._response = response
            self._data = data

        @property
        def status(self):
            return self._response.status

        @property
        def reason(self):
            return self._response.reason

        @property
        def headers(self):
            return self._response.headers

        @property
        def data(self):
            return self._data

    @property
    def logger(self):
        if hasattr(self, '_logger'):
            return self._logger
        self._logger = logging.getLogger('restcli')
        return self._logger


_restcli_pillar_class = pillar_class(RESTfulClient)
_restcli_pillar = _restcli_pillar_class(_pillar_history)
restcli = _restcli_pillar


def rest_client(*args, base_url=None):
    """
    A RESTfulClient decorator.

    @rest_client(base_url='http://localhost:9999')
    def test_hello():

        restcli.get('api/v1')

        .....
    """

    def _decorator(func):
        def wrapper(*args, **kwargs):

            def exit_callback(etyp, eval, tb):
                pass

            client = RESTfulClient(base_url=base_url)

            bound_func = _pillar_history.bound(func,
                                        [(restcli, client)], exit_callback)

            ret = bound_func(*args, **kwargs)
            return ret

        functools.update_wrapper(wrapper, func)

        return wrapper

    if len(args) == 1 and callable(args[0]):
        return _decorator(*args) # decorator without arguments @http_client
    else:
        return _decorator # decorator with argument @http_client()
