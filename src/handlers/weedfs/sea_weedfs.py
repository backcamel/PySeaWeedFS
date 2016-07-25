# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: mango
@contact: w4n9@sina.com
@create: 16/7/21
hail hydra!
主要功能
1.上传文件
> curl -X POST http://localhost:9333/dir/assign
{"count":1,"fid":"3,01637037d6","url":"127.0.0.1:8080","publicUrl":"localhost:8080"}
> curl -X PUT -F file=@/home/chris/myphoto.jpg http://127.0.0.1:8080/3,01637037d6
{"size": 43234}
2.删除文件
> curl -X DELETE http://127.0.0.1:8080/3,01637037d6
3.下载文件
> curl http://localhost:9333/dir/lookup?volumeId=3
{"locations":[{"publicUrl":"localhost:8080","url":"localhost:8080"}]}
http://localhost:8080/3/01637037d6/my_preferred_name.jpg
http://localhost:8080/3/01637037d6.jpg
http://localhost:8080/3,01637037d6.jpg
http://localhost:8080/3/01637037d6
http://localhost:8080/3,01637037d6
"""

__author__ = "mango"
__version__ = "0.1"


import json
import requests


# noinspection PyMethodMayBeStatic
class SeaWeedFS(object):
    def __init__(self, master_info):
        self.master_info = master_info

    def get_fid(self):
        """
        上传文件前先获取fid
        :return:
        """
        fid_url = "http://{master}/dir/assign".format(master=self.master_info)
        try:
            r = requests.get(fid_url, timeout=2)
            return json.loads(r.text).get("fid", None), None
        except Exception as error:
            return None, error

    def get_volume_info(self, volume_id):
        """
        下载文件前先获取volume信息
        http://localhost:9333/dir/lookup?volumeId=3
        :return:
        """
        get_volume_url = "http://{master}/dir/lookup?volumeId={volume_id}".format(master=self.master_info,
                                                                                  volume_id=volume_id)
        try:
            r = requests.get(get_volume_url, timeout=2)
            r = json.loads(r.text)
            error = r.get('error', None)
            if error:
                return False, error
            else:
                return True, r.get('locations', None)
        except Exception as error:
            return False, error

    def put_file(self, volume_info, fid, file_path):
        """
        文件上传模块
        :param volume_info:
        :param fid:
        :param file_path:
        :return:
        """
        put_url = "http://{volume}/{fid}".format(volume=volume_info, fid=fid)
        try:
            files = {'file': open(file_path, 'rb')}
            r = json.loads(requests.post(put_url, files=files).text)
            error = r.get('error', None)
            if error:
                return False, error
            else:
                return True, r
        except Exception as error:
            return False, error

    def upload_file(self, volume_info, file_path):
        """
        上传文件
        :param volume_info:
        :param file_path:
        :return:
        """
        fid_stat, fid_info = self.get_fid()
        if fid_stat:
            upload_stat, upload_info = self.put_file(volume_info, fid_stat, file_path)
            if upload_stat:
                upload_info['fid'] = fid_stat
                return 0, upload_info
            else:
                return 1, upload_info
        else:
            return 2, fid_info

    def download_file(self, fid, file_name):
        """
        获取下载链接
        :param fid:
        :param file_name:
        :return:
        """
        volume_id, file_id = fid.split(',')
        volume_stat, volume_info = self.get_volume_info(volume_id)
        if volume_stat:
            if volume_info:
                for i in volume_info:
                    public_url = i.get('publicUrl', None)
                    if public_url:
                        download_url = "http://{public}/{volume_id}/{file_id}/{file_name}".format(public=public_url,
                                                                                                  volume_id=volume_id,
                                                                                                  file_id=file_id,
                                                                                                  file_name=file_name)
                        return True, download_url
                return False, 'not download url'
            else:
                return False, 'volume info none'
        else:
            return False, volume_info

    def delete_file(self, fid):
        volume_id, file_id = fid.split(',')
        volume_stat, volume_info = self.get_volume_info(volume_id)
        if volume_stat:
            if volume_info:
                for i in volume_info:
                    public_url = i.get('publicUrl', None)
                    if public_url:
                        delete_url = "http://{public}/{fid}".format(public=public_url, fid=fid)
                        r = json.loads(requests.delete(delete_url, timeout=2).text)
                        error = r.get('error', None)
                        size = r.get('size', None)
                        if error:
                            return False, error
                        else:
                            return True, None
                return False, 'not download url'
            else:
                return False, 'volume info none'
        else:
            return False, volume_info


if __name__ == '__main__':
    fs = SeaWeedFS('192.168.14.185:9333')
    #print fs.upload_file("192.168.14.185:8080", "/Users/wangyazhe/Downloads/zh-google-styleguide-master.zip")
    print fs.download_file('3,1ab9e41676', 'zh-google-styleguide-master.zip')