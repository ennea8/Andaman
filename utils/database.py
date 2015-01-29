# coding=utf-8
from conf import load_yaml
import conf

__author__ = 'zephyre'


def get_mongodb(db_name, col_name, profile):
    """
    建立MongoDB的连接。
    :param db_name:
    :param col_name:
    :return:
    """

    cached = getattr(get_mongodb, 'cached', {})

    if profile in cached:
        client = cached[profile]['client']
        db_set = cached[profile]['dbset']
    else:
        client = None
        db_set = None

    if not client:
        cfg = load_yaml()
        section = filter(lambda v: v['profile'] == profile, cfg['mongodb'])[0]

        host = section.get('host', 'localhost')
        port = int(section.get('port', '27017'))

        if section.get('replica', False):
            from pymongo import MongoReplicaSetClient
            from pymongo import ReadPreference

            client = MongoReplicaSetClient('%s:%d' % (host, port), replicaSet=section.get('replName'))

            pref = section.get('readPref', 'PRIMARY')
            client.read_preference = getattr(ReadPreference, pref)

        else:
            from pymongo import MongoClient

            client = MongoClient(host, port)

        cached[profile] = {'client': client}
        setattr(get_mongodb, 'cached', cached)

    db = client[db_name]

    if not db_set:
        db_set = set([])
    if db_name not in db_set:
        cfg = load_yaml()
        section = filter(lambda v: v['profile'] == profile, cfg['mongodb'])[0]

        auth = section.get('auth')
        if auth:
            db_auth = filter(lambda v: db_name in v['database'], auth)
            if db_auth:
                db_auth = db_auth[0]
                user = db_auth['user']
                passwd = db_auth['passwd']
                credb = client[getattr(auth, 'credb', 'admin')]
                if user and passwd:
                    credb.authenticate(name=user, password=passwd)

        db_set.add(db_name)
        cached[profile]['dbset'] = db_set
        setattr(get_mongodb, 'cached', cached)

    return db[col_name]


def get_mysql_db(db_name, user=None, passwd=None, profile=None, host='localhost', port=3306):
    """
    建立MySQL连接
    :param db_name:
    :param user:
    :param passwd:
    :param profile:
    :param host:
    :param port:
    :return:
    """

    cached = getattr(get_mysql_db, 'cached', {})
    sig = '%s|%s|%s|%s|%s|%s' % (db_name, profile, host, port, user, passwd)
    if sig in cached:
        return cached[sig]

    cfg = load_yaml()
    if profile and profile in cfg:
        section = cfg[profile]
        host = section.get('host', 'localhost')
        port = int(section.get('port', '3306'))
        user = section.get('user', None)
        passwd = section.get('passwd', None)

    from MySQLdb.cursors import DictCursor
    import MySQLdb

    return MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db_name, cursorclass=DictCursor,
                           charset='utf8')


def get_mongodb2(db_name, col_name, profile=None, host='localhost', port=27017, user=None, passwd=None):
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

    from pymongo import MongoClient

    mongo_conn = MongoClient(host, port)
    db = mongo_conn[db_name]
    if user and passwd:
        db.authenticate(name=user, password=passwd)
    return db[col_name]