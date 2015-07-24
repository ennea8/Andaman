import requests

__author__ = 'zephyre'


def get_etcd_info(settings):
    from requests.auth import HTTPBasicAuth

    user = settings.get('ETCD_USER', '')
    password = settings.get('ETCD_PASSWORD', '')

    if user and password:
        auth = HTTPBasicAuth(user, password)
    else:
        auth = None

    return {'host': settings.get('ETCD_HOST', 'etcd'), 'port': settings.getint('ETCD_PORT', 2379), 'auth': auth}


def get_etcd_key(etcd, key):
    base_url = 'http://%s:%d/v2/keys' % (etcd['host'], etcd['port'])
    url = base_url + key
    data = requests.get(url, auth=etcd['auth']).json()['node']
    return data['value'] if 'value' in data else None

