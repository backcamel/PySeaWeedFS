# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: mango
@contact: w4n9@sina.com
@create: 16/6/30
hail hydra!
"""

__author__ = "mango"
__version__ = "0.1"


import os
import sys
import tornado.httpserver
import tornado.ioloop
import tornado.web
from tornado.options import options

from settings import settings
from urls import url_patterns
from handlers.sqlitefs.fs_sqlite import create_tables


def db_init(db_path):
    """
    数据库初始化
    :param db_path:
    :return:
    """
    if os.path.exists(db_path):
        return True
    else:
        if create_tables():
            return True
        else:
            return False


# noinspection PyAbstractClass
class LittleFSApp(tornado.web.Application):
    def __init__(self):
        tornado.web.Application.__init__(self, url_patterns, **settings)


def main():
    db_path = "{root_path}/{db}".format(root_path=settings['sqlite'], db=settings['db'])
    if not db_init(db_path):
        print "Init DB Failed"
        sys.exit(999)
    print "Init DB Success"
    app = LittleFSApp()
    http_server = tornado.httpserver.HTTPServer(app, max_buffer_size=1024 * 1024 * 1024)
    http_server.listen(options.port)
    print "Start PyWeedFS Success"
    tornado.ioloop.IOLoop.instance().start()


if __name__ == "__main__":
    main()
