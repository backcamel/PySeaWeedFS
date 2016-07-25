# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: mango
@contact: w4n9@sina.com
@create: 16/7/22
hail hydra!
主要功能
1.判断文件在数据库中是否存在
2.插入数据
3.更新数据
4.删除数据
"""

__author__ = "mango"
__version__ = "0.1"


import peewee
from peewee import SqliteDatabase, CompositeKey
# noinspection PyUnresolvedReferences
from settings import settings


sqlite_db = SqliteDatabase("{root_path}/{db}".format(root_path=settings['sqlite'], db=settings['db']),
                           threadlocals=True)


class BaseModel(peewee.Model):
    """A base model that will use our Sqlite database."""
    class Meta:
        database = sqlite_db


class weedfs_info(BaseModel):
    name = peewee.CharField(max_length=255, unique=True)
    size = peewee.IntegerField()
    fid = peewee.CharField(max_length=16)

    class Meta:
        primary_key = CompositeKey('name', 'fid')
        indexes = ((('name', 'fid'), True), )


# noinspection PyBroadException
def create_tables():    # 建表
    try:
        sqlite_db.connect()
        sqlite_db.create_tables([weedfs_info])
        sqlite_db.close()
    except:
        return False
    return True


# noinspection PyMethodMayBeStatic,PyBroadException
class WeedFSDB(object):
    def __init__(self):
        self.connect()

    def connect(self):
        try:
            if sqlite_db.is_closed():
                sqlite_db.connect()
            else:
                pass
        except:
            return False
        return True

    def close(self):
        try:
            if not sqlite_db.is_closed():
                sqlite_db.close()
        except:
            return False
        return True

    def weed_insert(self, in_dict):
        """
        数据插入
        :param in_dict: 插入的数据  dict
        :return: 成功返回 true ,失败返回 false
        """
        try:
            iq = (weedfs_info
                  .insert(**in_dict))
            iq.execute()
            return True, None
        except Exception as error:
            return False, str(error)
        finally:
            self.close()

    def weed_update(self, update_dict, file_name):
        try:
            uq = (weedfs_info
                  .update(**update_dict)
                  .where(weedfs_info.name == file_name))
            uq.execute()
            return True, None
        except Exception as error:
            return False, str(error)
        finally:
            self.close()

    def weed_delete(self, file_name):
        try:
            d = (weedfs_info
                 .delete()
                 .where(weedfs_info.name == file_name))
            d.execute()
            return True, None
        except Exception as error:
            return False, str(error)
        finally:
            self.close()

    def weed_file_exist(self, file_name):
        """
        文件信息查询
        :param file_name: 文件名  str
        :return:
        0 查询成功
        1 未查询到数据
        2 查询错误
        """
        try:
            query_data = (weedfs_info
                          .select()
                          .where(weedfs_info.name == file_name))
            if query_data:
                return 0, query_data.dicts().get()
            else:
                return 1, None
        except Exception as error:
            return 2, str(error)
        finally:
            self.close()


if __name__ == '__main__':
    create_tables()