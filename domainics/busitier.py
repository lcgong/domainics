
# -*- coding: utf-8 -*-

import logging
from .pillar import _pillar_history, pillar_class
from .exception import UnauthorizedError, ForbiddenError, BusinessLogicError

class BusinessLogicLayer:

    def __init__(self, service_id, principal_id):
        self.service_id   = service_id
        self.principal_id = principal_id
        self.__logger     = None

    @property
    def logger(self):
        if not self.__logger:
            self.__logger = logging.getLogger(self.service_id)
        return self.__logger



    def test(self, cond, failed_msg):
        if not cond:
            raise TestFailedError(failed_msg)

    def fail(self, msg):
        raise BusinessLogicError(msg)

    def unauthorized(self, msg=None):
        raise UnauthorizedError(msg)

    def forbidden(self, msg=None):
        raise ForbiddenError(msg)

_busilogic_pillar_class = pillar_class(BusinessLogicLayer)
_busilogic_pillar = _busilogic_pillar_class(_pillar_history)
busilogic = _busilogic_pillar

