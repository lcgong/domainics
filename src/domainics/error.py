# -*- coding: utf-8 -*-


class DomainRuleError(Exception):
	pass

class NotFoundError(Exception):
	"""Domain object or resource is not found"""
	pass

class AccessControlError(Exception):
	pass

class AuthenticationError(AccessControlError):
	pass

class PermissionError(AccessControlError):
	pass

