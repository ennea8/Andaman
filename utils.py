# coding=utf-8
import ConfigParser
import os
import re
from math import radians, cos, sin, asin, sqrt

import pymongo.errors
import pymongo
import time

import conf


__author__ = 'zephyre'


def get_mongodb(db_name, col_name, profile=None, host='localhost', port=27017, user=None, passwd=None):
    """
    建立MongoDB的连接。

    :param host:
    :param port:
    :param db_name:
    :param col_name:
    :return:
    """
    if profile:
        section = conf.global_conf.get(profile, None)
        host = section.get('host', 'localhost')
        port = int(section.get('port', '27017'))
        user = section.get('user', None)
        passwd = section.get('passwd', None)

    col = None
    retry = 0
    while True:
        retry += 1
        try:
            mongo_conn = pymongo.Connection(host, port)
            db = getattr(mongo_conn, db_name)
            if user and passwd:
                db.authenticate(name=user, password=passwd)
            col = getattr(db, col_name)
            break
        except pymongo.errors.PyMongoError as e:
            if retry >= 5:
                raise e
            else:
                time.sleep(2)
    return col


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))

    # 6367 km is the radius of the Earth
    km = 6367 * c
    return km


def get_short_loc(name):
    """
    根据locality全称，获得简称。比如：重庆市->重庆，海西蒙古族自治州->海西等。
    :param name:
    """
    match = re.search(ur'(族|自治)', name)
    if match:
        while True:
            stripped = False

            name = name[:match.start()]
            idx = len(name) - 1
            while idx > 1:
                minor_cand = name[idx:]
                if minor_cand[-1] != u'族':
                    minor_cand += u'族'
                if '|' + minor_cand + '|' in u'|汉族|壮族|满族|回族|苗族|维吾尔族|土家族|彝族|蒙古族|藏族|布依族|侗族|瑶族|朝鲜族|白族|哈尼族|哈萨克族|黎族|傣族|畲族|傈僳族|仡佬族|东乡族|高山族|拉祜族|水族|佤族|纳西族|羌族|土族|仫佬族|锡伯族|柯尔克孜族|达斡尔族|景颇族|毛南族|撒拉族|布朗族|塔吉克族|阿昌族|普米族|鄂温克族|怒族|京族|基诺族|德昂族|保安族|俄罗斯族|裕固族|乌兹别克族|门巴族|鄂伦春族|独龙族|塔塔尔族|赫哲族|珞巴族|':
                    name = name[:idx]
                    stripped = True
                    break
                else:
                    idx -= 1

            if not stripped:
                break

    match = re.search(ur'(市|县|省|直辖市|自治县|自治区|地区)$', name)
    if match:
        tmp = name[:match.start()]
        if len(tmp) > 1:
            name = tmp

    return name


def cfg_entries(section, key):
    config = ConfigParser.ConfigParser()
    d = os.path.split(os.path.realpath(__file__))[0]
    path = os.path.realpath(os.path.join(d, 'conf/private.cfg'))
    config.read(path)
    return config.get(section, key)


def cfg_sections():
    config = ConfigParser.ConfigParser()
    d = os.path.split(os.path.realpath(__file__))[0]
    path = os.path.realpath(os.path.join(d, 'conf/private.cfg'))
    config.read(path)
    return config.sections()


def cfg_options(section):
    config = ConfigParser.ConfigParser()
    d = os.path.split(os.path.realpath(__file__))[0]
    path = os.path.realpath(os.path.join(d, 'conf/private.cfg'))
    config.read(path)
    return config.options(section)