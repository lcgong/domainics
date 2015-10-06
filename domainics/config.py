# -*- coding: utf-8 -*-
import logging

import yaml
import sys
import os
import os.path as fp

from domainics.db import set_dsn


def _get_home_path():

    main_file = sys.modules['__main__'].__file__
    
    homepath = fp.abspath(fp.dirname(main_file))

    
    return homepath

def load(homepath=None):

    if homepath is None:
        homepath = _get_home_path()
    
    print('homepath: ' + homepath)

    conf_dir = fp.join(homepath, 'conf')
    
    
    # 加载日志的配置文件
    import logging.config    
    conf_file = fp.join(conf_dir, 'logging.yaml')
    if fp.exists(conf_file):
        with open(conf_file) as f:
            conf = yaml.load(f)
        conf.setdefault('version', 1)
        logging.config.dictConfig(conf)
    # logging.basicConfig()

    _logger = logging.getLogger(__name__)
    _logger.info('configuration loaded, ' + conf_file) 

    abcs =logging.getLogger('abcs')

    abcs.info('test')
     
    conf_file = fp.join(conf_dir, 'database.yaml')
    if fp.exists(conf_file):
        with open(conf_file) as f:
            conf = yaml.load(f)
            for dsn, settings in conf.items():
                if dsn.upper() == 'DEFAULT':
                    dsn = 'DEFAULT'
                
                try:                
                    set_dsn(dsn=dsn, **settings)
                except Exception as ex:
                    _logger.error(str(ex), exc_info=True)

    _logger.info('configuration loaded, ' + conf_file)                       

# load()
