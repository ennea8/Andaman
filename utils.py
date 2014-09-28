# coding=utf-8
import pymongo
from math import radians, cos, sin, asin, sqrt

__author__ = 'zephyre'

def get_mongodb(db_name, col_name, host='localhost', port=27017):
    # def get_mongodb(db_name, col_name, host='localhost', port=28017):
    """
    建立MongoDB的连接。

    :param host:
    :param port:
    :param db_name:
    :param col_name:
    :return:
    """
    mongo_conn = pymongo.Connection(host, port)

    db = getattr(mongo_conn, db_name)
    col = getattr(db, col_name)
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
