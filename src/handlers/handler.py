# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: mango
@contact: w4n9@sina.com
@create: 16/7/7
hail hydra!
"""

__author__ = "mango"
__version__ = "0.1"

import os
import json
import logging

import tornado.web
import tornado.gen
from tornado.web import HTTPError

from streamer.post_streamer import PostDataStreamer
from base import BaseHandler
from sqlitefs.fs_sqlite import WeedFSDB
from weedfs.sea_weedfs import SeaWeedFS

logger = logging.getLogger('weedfs.' + __name__)


# noinspection PyAbstractClass,PyAttributeOutsideInit
@tornado.web.stream_request_body
class UploadHandler(BaseHandler):
    def initialize(self):
        self.set_header('Content-type', 'application/json')
        self.db_client = WeedFSDB()
        self.weed_master = self.settings.get('weed_master')
        self.fs_client = SeaWeedFS(self.weed_master)
        self.weed_volume_list = self.settings.get('weed_volume')
        self.volume_flag_min = 0
        self.volume_flag_max = len(self.weed_volume_list) - 1
        self.file_tmp_path = None
        self.res_status = dict()
        self.file_info = dict()
        self.upload_dir = self.settings.get('tmp_dir')

    @tornado.gen.coroutine
    def prepare(self):
        """
        第二步执行,读取请求头
        :return:
        """
        try:
            total = int(self.request.headers.get("Content-Length", "0"))
        except:
            total = 0
        self.ps = PostDataStreamer(total, self.upload_dir)

    @tornado.gen.coroutine
    def data_received(self, chunk):
        """
        第三步执行,写文件
        :param chunk: 文件内容
        :return:
        """
        self.ps.receive(chunk)

    def upload_file(self, file_path, file_name):
        exist_stat, exist_res = self.db_client.weed_file_exist(file_name)
        if exist_stat == 0:    # 存在
            delete_stat, delete_res = self.fs_client.delete_file(exist_res.get('fid'))
            if delete_stat:
                upload_stat, upload_res = self.fs_client.upload_file(self.weed_volume_list[self.volume_flag_min],
                                                                     file_path)
                if upload_stat == 0:
                    if isinstance(upload_res, dict):
                        upload_res['name'] = file_name
                    db_update_stat, db_update_res = self.db_client.weed_update(upload_res, file_name)
                    if db_update_stat:
                        if self.volume_flag_min < self.volume_flag_max:
                            self.volume_flag_min += 1
                        elif self.volume_flag_min == self.volume_flag_max:
                            self.volume_flag_min = 0
                        return True, None
                    else:
                        return False, db_update_res
                else:
                    return False, upload_res
            else:
                return False, delete_res
        elif exist_stat == 1:    # 不存在
            upload_stat, upload_res = self.fs_client.upload_file(self.weed_volume_list[self.volume_flag_min], file_path)
            if upload_stat == 0:
                if isinstance(upload_res, dict):
                    upload_res['name'] = file_name
                db_insert_stat, db_insert_res = self.db_client.weed_insert(upload_res)
                if db_insert_stat:
                    if self.volume_flag_min < self.volume_flag_max:
                        self.volume_flag_min += 1
                    elif self.volume_flag_min == self.volume_flag_max:
                        self.volume_flag_min = 0
                    return True, None
                else:
                    return False, db_insert_res
            else:
                return False, upload_res
        else:
            return False, exist_res

    @tornado.gen.coroutine
    @tornado.web.asynchronous
    def post(self, *args, **kwargs):
        """
        第四步执行,获取文件信息,上传写数据库,销毁文件
        :param args:
        :param kwargs:
        :return:
        """
        file_name = self.get_argument('filename', default=None, strip=True)
        try:
            self.ps.finish_receive()
            # 获取文件信息
            for idx, part in enumerate(self.ps.parts):
                self.file_info['file_size'] = part.get('size', 0)
                self.file_tmp_path = part.get("tmpfile").name
                for header in part["headers"]:
                    params = header.get("params", None)
                    if params:
                        if file_name:
                            self.file_info['file_name'] = file_name
                        else:
                            self.file_info['file_name'] = params.get("filename", "")
            upload_stat, upload_res = self.upload_file(self.file_tmp_path, self.file_info['file_name'])
            if upload_stat:
                self.res_status['status'], self.res_status['result'] = 0, self.file_info['file_name']
            else:
                self.res_status['status'], self.res_status['result'] = 1, upload_res
        except Exception as error:
            logging.error(str(error))
            self.res_status['status'], self.res_status['result'] = 2, str(error)
        finally:
            self.file_info.clear()
            self.ps.release_parts()    # 删除处理
            self.write(json.dumps(self.res_status))
            self.finish()


# noinspection PyAbstractClass,PyAttributeOutsideInit
class DownloadHandler(BaseHandler):
    def initialize(self):
        self.set_header('Content-Type', 'application/octet-stream')
        self.db_client = WeedFSDB()
        self.weed_master = self.settings.get('weed_master')
        self.fs_client = SeaWeedFS(self.weed_master)

    @tornado.gen.coroutine
    @tornado.web.asynchronous
    def get(self, file_name):
        try:
            if file_name:
                exist_stat, exist_res = self.db_client.weed_file_exist(file_name)
                if exist_stat == 0:
                    query_stat, query_res = self.fs_client.download_file(exist_res.get('fid'), file_name)
                    if query_stat:
                        self.redirect(url=query_res, permanent=False, status=None)
                    else:
                        raise HTTPError(404)
                else:
                    raise HTTPError(404)
        except Exception as error:
            logging.error(str(error))
            raise HTTPError(404)
        finally:
            self.finish()

    @tornado.gen.coroutine
    @tornado.web.asynchronous
    def head(self, *args, **kwargs):
        return self.get(*args, **kwargs)