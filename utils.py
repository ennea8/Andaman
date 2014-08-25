# coding=utf-8
import pymongo

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
