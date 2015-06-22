# -*- coding: utf-8 -*-

import logging

_logger = logging.getLogger(__name__)


from collections  import OrderedDict as _OrderedDict
from collections  import namedtuple  as _namedtuple
from tornice.util import nameddict   as _nameddict


_dsn_class = {}

def set_dsn(**kwargs):
    ''' '''
    dbsys = kwargs.pop('sys', 'postgres')
    dsn   = kwargs.get('dsn', 'DEFAULT')
    if dbsys in ('postgres', 'pgsql', 'pg') :
    	import tornice.db_postgres as _pg

    	_dsn_class[dsn] = _pg.PostgreSQLBlock
    	_pg.PostgreSQLBlock.set_dsn(**kwargs)
    elif dbsys in ('sqlite'):
    	raise NotImplemented('sqlite')

    elif dbsys in ('mysql'):
    	raise NotImplemented('mysql')

def sqlblock(dsn='DEFAULT', autocommit=False, record_type=None):
	
	sqlblock_class = _dsn_class[dsn]

	blkobj = sqlblock_class(
		dsn=dsn, 
		autocommit=autocommit, 
		record_type=record_type)

	return blkobj

def record_dict(cursor):
    fields = (d[0] for d in cursor.description)
    for row in cursor:
        yield _OrderedDict(zip(fields, row))

def record_namedtuple(cursor):
    dt = _namedtuple("Row", [d[0] for d in cursor.description or ()])
    for row in cursor:
        yield dt(*row)

def record_plainobj(cursor):
    dt = _nameddict("Row", [d[0] for d in cursor.description or ()])
    for row in cursor:
        yield dt(*row)


class BaseSQLBlock:
    def __init__(self, dsn='DEFAULT', autocommit=False, record_type=None):
        
        self._record_type = record_type or _default_record_type

        self.dsn = dsn
        self.autocommit = autocommit
                    
    def __enter__(self):
        self._open()
        return self

    def __exit__ (self, etyp, ev, tb):
        try :
            conn = self._conn
            cur  = self._cursor
            if not conn.autocommit and ev :
                conn.rollback()
                _logger.warning("transaction rollbacked", exc_info=ev)
            else:
                conn.commit()
        finally:
            self._close()

        return False

    def __call__(self, sql, params=None, many_params=None):
        if many_params is not None:
            self._cursor.executemany(sql, many_params)
        else:
            self._cursor.execute(sql, params)

        self._iter = None

    def __iter__(self):
        self._iter = self._record_type(self._cursor)
        return self._iter

    def __next__(self):
        _iter = self._iter or self.__iter__()
        return _iter.__next__()

    @property 
    def rowcount(self):
        self._cursor.rowcount

    @property
    def record_type(self):
        return self._record_type

    @record_type.setter
    def record_type(self, val):
        self._record_type = val

_default_record_type = record_plainobj


def with_sql(*d_args, dsn='DEFAULT', autocommit=False):
    """ 对函数方法装配sqlblock，实例会增加sql属性，无异常结束提交事务，有异常则回滚.
    
    @with_sql
    @with_sql()
    @with_sql(auto_commit=True, dsn='other_db')
    
    具体使用，必须提供第一个参数
    @with_sql()
    def do_something(self, arg):
        ...
        return ...
        
    如果该实例已经存在非空的sql属性则会抛出AttributeError
    """
    
    def _decorator(func):
        def sqlblock_wrapper(*args, **kwargs):
            obj = args[0]
            if hasattr(obj, 'sql') and obj.sql is not None:
                # sqlblock嵌套重入，如果当前线程已经建立了一个连接，
                # 则继续使用这一个连接
                return func(*args, **kwargs)
            else :
                # 新建sqlblock环境
                with SimpleSqlBlock(dsn, autocommit) as sqlblk:
                    try :
                        setattr(obj, 'sql', sqlblk)
                        return func(*args, **kwargs)
                    finally:
                        setattr(obj, 'sql', None)
                
        functools.update_wrapper(sqlblock_wrapper, func)
        return sqlblock_wrapper
    
    if len(d_args) == 1 and callable(d_args[0]): # 没有参数的decorator
        return _decorator(d_args[0])
    else:
        return _decorator

