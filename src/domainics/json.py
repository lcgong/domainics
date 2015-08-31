# -*- coding: utf-8 -*-

import json
import datetime

from . import dset, dobject



def loads(s):
	"""Deserialize s to a python object"""
	return json.loads(s)



def dumps(obj):
	return json.dumps(obj, cls=_JSONEncoder)	



class _JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, (Decimal)) :
            return float(obj)
        elif isinstance(obj, (dset, dobject)):
            return obj.export()
        else:
            return super(DJSONEncoder, self).default(obj)

