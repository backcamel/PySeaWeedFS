# !/usr/bin/env python
# -*- coding: utf-8 -*-

"""
@author: mango
@contact: w4n9@sina.com
@create: 16/7/21
hail hydra!
Upload File
> curl -X POST http://localhost:9333/dir/assign
{"count":1,"fid":"3,01637037d6","url":"127.0.0.1:8080","publicUrl":"localhost:8080"}
> curl -X PUT -F file=@/home/chris/myphoto.jpg http://127.0.0.1:8080/3,01637037d6
{"size": 43234}
Delete
> curl -X DELETE http://127.0.0.1:8080/3,01637037d6
Download File
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

    def get_volume(self):
        """
        下载文件前先获取volume信息
        :return:
        """
        pass

    def put_file(self, volume_info, fid, file_path):
        """
        put 上传文件
        :param volume_info:
        :param fid:
        :param file_path:
        :return:
        """
        put_url = "http://{volume}/{fid}".format(volume=volume_info, fid=fid)
        try:
            with open(file_path, 'rb') as fp:
                files = {'file': fp}
                r = requests.post(put_url, files=files)
                file_size = json.loads(r).get('size', 0)
                if file_path != 0:
                    return 0, file_size
                else:
                    return 1, "file size is 0"
        except Exception as error:
            return 1, error

    def upload_file(self):
        pass

    def download_file(self):
        pass

    def delete_file(self):
        pass


if __name__ == '__main__':
    fs = SeaWeedFS('192.168.14.185:9333')
    print fs.get_fid()