# -*- coding: utf-8 -*-

from collections import OrderedDict
from collections import namedtuple
from collections.abc import Iterable, Mapping
from itertools import islice

import re

_sortable_pattern = re.compile(r'(?P<flag>[+-]?)(?P<name>\w+),?')

_sortable_field = namedtuple('SortableField', ['name', 'ascending'])


"""

    Request Header:
        Range: items=0-24, sortable=+a,-b

    URL Query string:
            range=0-24@+a,-b
            range=0-@+a,-b


    Response header:

        Content-Range: items=0-24/*, sortable=+a,-b
"""

class DPage:

    def __init__(self, start=0, limit=None, sortable=None):
        """
        sortable:
            flag '+' is acsending, and '-' is descending.
        """
        self.start = start
        self.limit = limit
        self.total = None
        self.sortable = []

        if sortable:
            for m in _sortable_pattern.finditer(sortable):
                flag = m.group('flag')
                name = m.group('name')
                if not flag:
                    flag = '+'
                elif flag not in ('+', '-'):
                    err = "flag is required: +/-"
                    raise SyntaxError(err)

                self.sortable.append(_sortable_field(name, flag=='+'))

    def __bool__(self):
        return bool(self.start or self.limit or self.sortable)

    @property
    def page_no(self):
        if  self.limit is None:
            return 1

        return self.start // self.limit + 1

    def next(self, pages=1):
        if  self.limit is None:
            return

        page_no = self.page_no
        page_no += pages
        if page_no <= 0 :
            page_no = 1

        self.start = (page_no - 1) * self.limit

    def goto(self, page_no=None, start=None, limit=None):
        if self.page_no is None:
            if start is not None:
                self.start = start

            if limit is not None:
                self.limit = limit
        else:
            if limit is not None:
                self.limit = limit

            if self.limit is not None:
                self.start = (page_no - 1) * self.limit

    def __repr__(self):
        clsname = self.__class__.__name__

        attrs = []
        if self.start is not None:
            attrs.append('start=%d' % self.start)

        if self.limit is not None:
            attrs.append('limit=%d' % self.limit)

        if self.sortable:
            attrs.append('sortable=%r' % self.format_sortable())

        return clsname + '(' + ', '.join(attrs) + ')'

    def copy(self):
        sortable = self.format_sortable()
        page = DPage(start=self.start, limit=self.limit, sortable=sortable)
        return page

    def set_sortable(self, sortable):
        """
        set_sortable([('attr_name', true_or_false)])
        """
        self.sortable.clear()
        for attr_name, ascending in sortable:
            self.sortable.append(_sortable_field(attr_name, ascending))

    def format_content_range(self):

        if self.start is None:
            start = 0
        else:
            start = self.start

        if self.limit is not None:
            end = str(start + self.limit - 1)
        else:
            end = ''

        start = str(start)

        if not self.total:
            total = '*'

        range_string = "items=%s-%s/%s" % (start, end, total)

        if self.sortable :
            range_string += ', sortable=' + self.format_sortable()

        return range_string

    def format_sortable(self):
        return ','.join(('+' if asc else '-') + name
                            for name, asc in self.sortable)


_qs_range_ptn = re.compile(r'(?P<start>\d+)-(?P<end>\d+|\*)?'
                           r'(?:@(?P<sort>.*))?$')

def parse_query_range(s):
    m = _qs_range_ptn.match(s)
    if not m:
        return None, None, None

    sortable = m.group('sort')
    start = m.group('start')
    end = m.group('end')

    if start and (end and end != '*'):
        limit = int(end) - int(start) + 1

    if isinstance(start, str) :
        start = int(start)

    return start, limit, sortable

_hdr_range_ptn = re.compile(r'\s*(?:items\s*=\s*'
                            r'(?P<start>\d+)-(?P<end>\d+)'
                            r'(?:/(?P<total>\d+|\*))?'
                            r'(?:,\s*|\s+)?)?(?:sortable\s*=\s*(?P<sort>.*))?$')

def parse_header_range(s):
    m = _hdr_range_ptn.match(s)
    if not m:
        return None, None, None, None

    sortable = m.group('sort')
    start = m.group('start')
    end = m.group('end')
    total = m.group('total')
    if total == '*':
        total = None

    if start and (end and end != '*'):
        limit = int(end) - int(start) + 1

    if isinstance(start, str) :
        start = int(start)

    if isinstance(total, str) :
        total = int(total)

    return start, limit, total, sortable
