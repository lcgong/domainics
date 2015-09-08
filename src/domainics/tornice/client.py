
import functools
import http.client
import urllib.parse

import urllib.request
from http.cookiejar import CookieJar, DefaultCookiePolicy
from urllib.request import HTTPCookieProcessor, build_opener
from urllib.parse import urljoin

from .. import json as _json
from ..pillar import _pillar_history, pillar_class, PillarError




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

        req = urllib.request.Request(req_url)

        body = None
        if json_args is not None:
            req.add_header('Content-type', 'application/json')
            body = _json.dumps(json_args)

        if post_args is not None:
            req.add_header('Content-type', 'application/x-www-form-urlencoded')
            body = urllib.parse.urlencode(qs_args)

        self._request = req
        

    def get(self, url=None, url_args=None, qs_args=None, 
            post_args=None, json_args=None):
        
        self.request('GET', url, url_args, qs_args, post_args, json_args)

    def post(self, url=None, url_args=None, qs_args=None, 
            post_args=None, json_args=None):
        
        self.request('POST', url, url_args, qs_args, post_args, json_args)

    def put(self, url=None, url_args=None, qs_args=None, post_args=None, json_args=None):
        self.request('PUT', url, url_args, qs_args, post_args, json_args)

    def put(self, url=None, url_args=None, qs_args=None, post_args=None, json_args=None):
        self.request('DELETE', url, url_args, qs_args, post_args, json_args)

    @property
    def response(self):
        if self._response is None:
            self._response = RESTfulClient.Response(self._opener.open(self._request))

        return self._response


    class Response:
        
        def __init__(self, response):
            self._response = response
            self._data = response.read()
            
            headers = response.headers
            content_type = headers.get_content_type()
            charset = headers.get_content_charset()
            if charset is None:
                charset = 'UTF-8'

            if  content_type == 'application/json':
                self._data = _json.loads(self._data.decode(charset))
            elif content_type == 'text/plain':
                self._data = self._data.decode(charset)

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

