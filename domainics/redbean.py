import inspect

from domainics.domobj.pagination import DPage
from domainics.domobj.pagination import parse_query_range, parse_header_range
from domainics.domobj import dset, dobject, DObject, DSet


from domainics.pillar import _pillar_history, pillar_class

from redbean.handler_argument import register_argument_getter, read_json
from redbean.handler_response import register_response_writer

from aiohttp.web import json_response
from redbean.json import json_dumps


def setup(app):
    register_argument_getter(_dobject_value_getter)
    register_argument_getter(_dpage_value_getter)
    register_response_writer(_rest_dobject_response_factory)

#----------------------------------------------------------------------------

def _dobject_value_getter(proto, method, handler, path_params, arg_name):
    arg_spec = inspect.signature(handler).parameters[arg_name]
    ann_type = arg_spec.annotation


    if issubclass(ann_type, DSet[DObject]):
        item_type = ann_type.__parameters__[0]

        async def _getter(request):
            json_obj = await read_json(request)
            return dset(item_type)(json_obj)

        return _getter

    if issubclass(ann_type, DObject):
        async def _getter(request):
            json_obj = await read_json(request)
            return ann_type(json_obj)

        return _getter


def _dpage_value_getter(proto, method, handler, path_params, arg_name):
    arg_spec = inspect.signature(handler).parameters[arg_name]
    ann_type = arg_spec.annotation

    if not issubclass(ann_type, DPage):
        return

    async def getter(request, arg_val):
        pass
        # arg_val = make_pagination(handler)
        # # arg_val = ann_type(arg_val)
        # return arg_val

    return getter




# ----------------------------------------------------------------------------
def _rest_dobject_response_factory(proto, method, handler):
    if proto != 'REST':
        return

    ret_type = inspect.signature(handler).return_annotation
    if not (issubclass(ret_type, DSet[DObject])
            or issubclass(ret_type, DObject)
            or issubclass(ret_type, Mapping)
            or issubclass(ret_type, Sequence)):
        return

    def _response(request, return_value):
        return json_response(return_value, dumps=json_dumps)

    return _response

#
#
# def service_func_handler(proto, service_func, service_name, path_sig) :
#
#     def rest_handler(self, *args, **kwargs):
#         obj = http_handler(self, *args, **kwargs)
#
#         if not isinstance(obj, (list, tuple, DSetBase)):
#             obj = [obj] if obj is not None else []
#
#         if isinstance(obj, DSetBase) and hasattr(obj, '_page'):
#             content_range = obj._page.format_content_range()
#             self.set_header('Content-Range', content_range)
#             if obj._page.start != 0 or obj._page.limit is not None:
#                 self.set_status(206)
#
#         self.set_header('Content-Type', 'application/json; charset=UTF-8')
#         self.write(_json.dumps(obj))
#
#     if proto == 'REST':
#         return rest_handler
#     elif proto == 'HTTP':
#         return http_handler
#     else:
#         raise ValueError('Unknown')
