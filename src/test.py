# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: mango
@contact: w4n9@sina.com
@create: 16/7/22
hail hydra!
"""

__author__ = "mango"
__version__ = "0.1"


from handlers.sqlitefs.fs_sqlite import WeedFSDB, create_tables


create_tables()
w = WeedFSDB()
data = {'file_name': 'aaaaaa', 'file_size': 100, 'fid': '12345'}
print w.weed_insert(data)
print w.weed_file_exist('aaaaaa')