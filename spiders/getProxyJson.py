import json

import MySQLdb


def getProxyJson():
    conn = MySQLdb.connect(host='127.0.0.1',  # @UndefinedVariable
                           user='root',
                           passwd='123',
                           db='plan',
                           charset='utf8')
    cursor = conn.cursor()
    SQL_string = 'select ip from proxy'
    cursor.execute(SQL_string)
    proxys = cursor.fetchall()
    conn.commit()
    proxy_list = []
    for it in proxys:
        proxy_list.append(it[0])
    with open('proxy.json', 'w')as file:
        file.write(json.dumps(proxy_list))


if __name__ == "__main__":
    getProxyJson()
    with open('proxy.json', 'r')as file:
        t = file.readline()
        print t
        proxy = json.loads(t)
        print proxy