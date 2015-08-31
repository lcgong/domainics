# -*- coding: utf-8 -*-



from .db import dbc, transaction, set_dsn

from .domobj import dobject, dset, datt
# from .domobj import dobject, dset, datt, dident


from .tornice.route import route_base, http_route, rest_route
from .tornice.route import _request_handler_pillar
from .tornice.app   import WebApp

handler = _request_handler_pillar




