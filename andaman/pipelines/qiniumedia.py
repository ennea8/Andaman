# coding=utf-8
import json
import sys

from scrapy.http import Response

from scrapy.pipelines.files import FilesPipeline
from twisted.internet import threads
from qiniu import Auth, BucketManager


__author__ = 'zephyre'


class QiniuFilesStore(object):
    def _get_key(self, key):
        from andaman.utils import etcd

        etcd_info = etcd.get_etcd_info(self.settings)
        return etcd.get_etcd_key(etcd_info, '/project-conf/andaman/qiniu/%s' % key).encode('utf-8')

    def get_access_key(self):
        if not self._access_key:
            ak = self._get_key('accessKey')
            self._access_key = ak
        return self._access_key

    def get_secret_key(self):
        if not self._secret_key:
            sk = self._get_key('secretKey')
            self._secret_key = sk
        return self._secret_key

    def get_bucket_mgr(self):
        if not self._bucket_mgr:
            ak = self.access_key
            sk = self.secret_key
            q = Auth(ak, sk)
            self._bucket_mgr = BucketManager(q)

        return self._bucket_mgr

    bucket_mgr = property(get_bucket_mgr)

    access_key = property(get_access_key)

    secret_key = property(get_secret_key)

    def __init__(self, bucket, settings):
        self.bucket = bucket
        self.settings = settings

        self._access_key = None
        self._secret_key = None
        self._bucket_mgr = None

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

    def __init__(self, bucket, settings=None):
        self.store = QiniuFilesStore(bucket, settings)
        super(FilesPipeline, self).__init__(download_func=self.fetch)

    def fetch(self, request, spider):
        key = self.file_path(request)
        ret = self.store.fetch_file(request.url, key)

        return Response(request.url, body=json.dumps(ret))

    @classmethod
    def from_settings(cls, settings):
        bucket = settings.get('QINIU_BUCKET', 'aizou')

        cls.FILES_URLS_FIELD = settings.get('FILES_URLS_FIELD', cls.DEFAULT_FILES_URLS_FIELD)
        cls.FILES_RESULT_FIELD = settings.get('FILES_RESULT_FIELD', cls.DEFAULT_FILES_RESULT_FIELD)
        cls.EXPIRES = settings.get('QINIU_EXPIRE', cls.DEFAULT_EXPIRES)

        return cls(bucket, settings=settings)

    def file_path(self, request, response=None, info=None):
        from scrapy.utils.request import request_fingerprint

        return request_fingerprint(request)

    def file_downloaded(self, response, request, info):
        return json.loads(response.body)['hash']