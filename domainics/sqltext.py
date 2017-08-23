import sys, string

class SQLText:
    __tuple__ = ('_segments',)

    def __init__(self):
        self._segments = []

    def __bool__(self):
        return bool(self._segments)

    def __iadd__(self, sqltext):
        if isinstance(sqltext, SQLText):
            self._segments += sqltext._segments
            return self

        elif isinstance(sqltext, str):
            return self._join(sqltext, sep='', frame=sys._getframe(1))

        else:
            raise TypeError(type(sqltext))

    def __add__(self, sqltext):
        if isinstance(sqltext, SQLText):
            newone = SQLText()
            newone._segments += self._segments
            newone._segments += sqltext._segments

        elif isinstance(sqltext, str):
            segments = _sqlstr_parse(sqltext, sys._getframe(1))

            newone = SQLText()
            newone._segments += self._segments
            newone._segments += segments
            return newone

        else:
            raise TypeError(type(sqltext))

        return self

    def _join(self, *sqltexts, sep='', frame=sys._getframe(1)):
        if not sqltexts: return

        sql_text_iter = iter(sqltexts)
        sqltext = next(sql_text_iter, None)

        if isinstance(sqltext, str):
            segments = _sqlstr_parse(sqltext, frame)
        elif isinstance(sqltext, SQLText):
            segments = [] + sqltext._segments
        else:
            raise TypeError()

        if segments and self._segments:
            self._segments.append(SQLSegment(sep, frame))

        self._segments += segments

        for sqltext in sql_text_iter:
            if isinstance(sqltext, str):
                segments = _sqlstr_parse(sqltext, frame)
            elif isinstance(sqltext, SQLText):
                segments = [] + sqltext._segments
            else:
                raise TypeError()

            # segments = qlstr_parse(sqlstr, frame)
            if segments:
                self._segments.append(SQLSegment(sep, frame))

            self._segments += segments

        return self

    def get_statment(self, params=None):
        sql_text = ''


        placeholders = []

        var_counter = 0
        for seg in self._segments:
            if isinstance(seg, SQLSegment):
                sql_text += seg.text

            elif isinstance(seg, SQLPlaceholder):
                placeholders.append(seg)
                var_counter += 1
                sql_text += f"${var_counter}"

        if params is None or isinstance(params, dict):
            return sql_text, eval_param_vals(params, placeholders), False

        elif isinstance(params, list):
            sql_vals_list = []
            params_list = params
            for params in params_list:
                sql_vals_list.append(eval_param_vals(params, placeholders))

            return sql_text, sql_vals_list, True

        return sql_text, sql_vals

    def __str__(self):
        return f"SQLText({str(self._segments)}])"

def eval_param_vals(params, placeholders):
    sql_vals = []

    for seg in placeholders:
        localvars = {}
        localvars.update(seg.frame.f_locals)
        if params:
            localvars.update(params)

        value = eval(seg.field_name, None, localvars)

        sql_vals += [value]
    return sql_vals

class SQLSegmentBase:
    def __init__(self, frame):
        self.offset = (0, 0) # lineno, charpos at line
        self.frame = frame   # frame.f_lineno frame.f_code.co_filename

class SQLSegment(SQLSegmentBase):
    def __init__(self, text, frame):
        super().__init__(frame)
        self.text = text

        # compute the offset of this segment
        lines = text.splitlines()
        if lines:
            offset_lineno = len(lines) - 1
            offset_charpos = len(lines[offset_lineno])

            self.offset = (offset_lineno, offset_charpos)

    def __repr__(self):
        return f"SQLSegment(text='{self.text}', offset={self.offset})"

class SQLPlaceholder(SQLSegmentBase):
    def __init__(self, field_name, value, frame):
        super().__init__(frame)
        self.value = value
        self.field_name = field_name

    def __repr__(self):
        return f"SQLPlaceholder(value='{self.value}', offset={self.offset})"



_formatter = string.Formatter()
def _sqlstr_parse(sqlstr, frame):
    segments = []
    for text, field_name, format_spec, conversion in _formatter.parse(sqlstr):
        segments.append(SQLSegment(text, frame))

        if field_name:
            try:
                val = eval(field_name, None, frame.f_locals)
            except NameError as exc:
                val = exc

            if isinstance(val, SQLText):
                segments += val._segments
            else:
                seg = SQLPlaceholder(field_name, val, frame)
                segments.append(seg)

    return segments


def SQL(*sqlstrs, sep=''):
    sqltext = SQLText()
    sqltext._join(*sqlstrs, sep=sep, frame=sys._getframe(1))
    return sqltext

__all__ = ['SQL']
