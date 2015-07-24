# coding=utf-8
from scrapy.pipelines.files import FilesPipeline
import sys
from twisted.internet import threads

from qiniu import Auth, BucketManager


__author__ = 'zephyre'


class QiniuFilesStore(object):
    ACCESS_KEY = 'jU6KkDZdGYODmrPVh5sbBIkJX65y-Cea991uWpWZ'
    SECRET_KEY = 'OVSfoBU_Lzb6QgMWCvOD0x1mDO10JxIOwCuIYNr0'

    def __init__(self, bucket):
        self.bucket = bucket

        q = Auth(self.ACCESS_KEY, self.SECRET_KEY)
        self.bucket_mgr = BucketManager(q)

    def get_file_stat(self, key):
        stat, error = self.bucket_mgr.stat(self.bucket, key)
        return stat

    def stat_file(self, key, info):
        def _onsuccess(stat):
            if stat:
                checksum = stat['hash']
                timestamp = stat['putTime'] / 10000000
                return {'checksum': checksum, 'last_modified': timestamp}
            else:
                return {}

        return threads.deferToThread(self.get_file_stat, key).addCallback(_onsuccess)

    def persist_file(self, path, buf, info, meta=None, headers=None):
        """
        因为我们采用七牛的fetch模型，所以，当request返回的时候，图像已经上传到了七牛服务器
        """
        pass

    def fetch_file(self, url, key):
        ret, error = self.bucket_mgr.fetch(url, self.bucket, key)
        if ret:
            return ret
        else:
            raise IOError


class QiniuPipeline(FilesPipeline):
    DEFAULT_EXPIRES = sys.maxint
    MEDIA_NAME = "file"
    DEFAULT_FILES_URLS_FIELD = 'file_urls'
    DEFAULT_FILES_RESULT_FIELD = 'files'

    def __init__(self, bucket):
        self.store = QiniuFilesStore(bucket)
        super(FilesPipeline, self).__init__(download_func=self.fetch)

    def fetch(self, request, spider):
        key = self.file_path(request)
        ret = self.store.fetch_file(request.url, key)

        # 伪造response
        response = object
        response.status = 200
        response.body = ret
        response.flags = False

        return response

    @classmethod
    def from_settings(cls, settings):
        bucket = settings.get('QINIU_BUCKET', 'aizou')

        cls.FILES_URLS_FIELD = settings.get('FILES_URLS_FIELD', cls.DEFAULT_FILES_URLS_FIELD)
        cls.FILES_RESULT_FIELD = settings.get('FILES_RESULT_FIELD', cls.DEFAULT_FILES_RESULT_FIELD)
        cls.EXPIRES = settings.get('QINIU_EXPIRE', cls.DEFAULT_EXPIRES)

        return cls(bucket)

    def file_path(self, request, response=None, info=None):
        from scrapy.utils.request import request_fingerprint

        return request_fingerprint(request)

    def file_downloaded(self, response, request, info):
        return response.body['hash']