# -*- coding: utf-8 -*-



class SecurityException(Exception):
    pass

class UnauthorizedError(SecurityException):
    pass

class ForbiddenError(SecurityException):
    pass

class BusinessLogicError(Exception):
    pass
