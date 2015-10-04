# coding=utf-8
__author__ = 'zephyre'


class EtcdConf(object):
    """
    Scrapy extension: 加载etcd中的配置项。需要用到以下Setting项目：

    * ETCD_HOST: etcd服务的地址
    * ETCD_PORT: etcd服务的端口
    * ETCD_USER(optional): etcd服务的用户名
    * ETCD_PASSWORD(optional): etcd服务的密码
    """

    def __init__(self, settings):
        self._build_conf(settings)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    @staticmethod
    def _build_conf(settings):
        """
        从etcd服务器中获得配置项目
        :param settings:
        :return:
        """
        from requests.auth import HTTPBasicAuth
        from pyconf import build

        user = settings.get('ETCD_USER')
        password = settings.get('ETCD_PASSWORD')
        if user and password:
            auth = HTTPBasicAuth(user, password)
        else:
            auth = None

        host = settings.get('ETCD_HOST')
        port = settings.getint('ETCD_PORT')

        if host:
            url = 'http://%s:%d' % (host, port)
            conf = build(url, auth, (('mongo-dev', 'mongo'),), ('andaman',))
            settings.set('ANDAMAN_SERVICES', conf['services'])
            settings.set('ANDAMAN_CONF', conf['andaman'])

