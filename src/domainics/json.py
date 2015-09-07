# -*- coding: utf-8 -*-

import json
import datetime

from .domobj import dset, dobject
from .db.dtable import dsequence
from decimal import Decimal


def loads(s):
	"""Deserialize s to a python object"""
	return json.loads(s)


def dumps(obj):
	return json.dumps(obj, cls=DefaultJSONEncoder)	

class DefaultJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, (Decimal)) :
            return float(obj)
        elif isinstance(obj, (dset, dobject)):
            return obj.export()
        elif isinstance(obj, (dset, dsequence)):
            return int(obj)        
        else:
            print(111, obj, type(obj))
            return super(DefaultJSONEncoder, self).default(obj)

