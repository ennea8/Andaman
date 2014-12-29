# coding=utf-8
import json
import urllib2

import time


__author__ = 'zephyre'

try:
    import qiniu.conf

    qiniu.conf.ACCESS_KEY = "QBsaz_MsErywKS2kkQpwJlIIvBYmryNuPzoGvHJF"
    qiniu.conf.SECRET_KEY = "OTi4GrXf8CQQ0ZLit6Wgy3P8MxFIueqMOwBJhBti"

    import qiniu.io
    import qiniu.rs
except ImportError:
    pass

def io_ops(op, retry=3, cooldown=3, extra_except=None):
    """
    通用的网络通信交互的函数。
    :param op:
    :param retry: 重试次数。
    :param cooldown: 如果失败的话，冷却多少时间（秒）
    """
    e1 = None
    except_list = extra_except if extra_except else []
    except_list.append(IOError)
    except_list = tuple(set(except_list))
    for retry_idx in xrange(retry):
        try:
            return op()
        except except_list as e:
            e1 = e
            if retry_idx < retry - 1:
                time.sleep(cooldown)

    raise e1 if e1 else IOError()


def upload_file(key, localfile, bucket='lvxingpai-img-store', retry=3, cooldown=3):
    """
    上传一个本地文件。
    :param bucket:
    :param key:
    :param localfile:
    :param retry:
    :param cooldown:
    :return:
    """
    return io_ops(lambda: qiniu.io.put_file(qiniu.rs.PutPolicy(bucket).token(), key, localfile), retry, cooldown)


def stat(key, bucket='lvxingpai-img-store', retry=3, cooldown=3):
    """
    获得图像的属性。
    :param bucket:
    :param key:
    :param retry:
    :param cooldown:
    :return:
    """
    return io_ops(lambda: qiniu.rs.Client().stat(bucket, key), retry, cooldown)


def image_info(key, bucket='lvxingpai-img-store', retry=3, cooldown=3):
    """
    获得图像基本属性。
    :param key:
    :param bucket:
    :param retry:
    :param cooldown:
    :return:
    """

    def op():
        response = urllib2.urlopen('http://%s.qiniudn.com/%s?imageInfo' % (bucket, key), timeout=5)
        data = response.read()
        return (json.loads(data), None)

    return io_ops(op, retry, cooldown, [ValueError])