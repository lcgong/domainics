
from tornice.web import route, route_base


route_base(path='/serv/abc', proto='REST')

@route('(?P<sid>\w+)', methods=['GET'], fields=['page'])
def hello(request, sid, page):
	msg = '%s, %s' % (sid, page)
	request.write("Hello, world\n" + msg)
	print('hi', msg)

