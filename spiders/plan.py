# coding=utf-8
import ConfigParser
import os
import re

import MySQLdb
from MySQLdb.cursors import DictCursor
from bson import ObjectId
from bson.errors import InvalidId
import pymongo
from scrapy import Item, Field, Request, log
from scrapy.contrib.spiders import CrawlSpider

import utils


__author__ = 'zephyre'


class PlanItem(Item):
    # define the fields for your item here like:
    planId = Field()
    title = Field()
    tags = Field()
    lxpTag = Field()
    targets = Field()
    days = Field()
    vsCnt = Field()
    desc = Field()
    moreDesc = Field()
    images = Field()
    travelMonth = Field()
    manualPriority = Field()
    tips = Field()
    mTime = Field()
    details = Field()
    enabled = Field()


class PlanImportSpider(CrawlSpider):
    """
    抓取到路线以后，我们会经过人工审核，放在MySQL数据库中。该爬虫类的作用，是将这些路线导入MongoDB数据库。
    """
    name = 'plan-import'

    def __init__(self, *a, **kw):
        super(PlanImportSpider, self).__init__(*a, **kw)

        # 人工校对的景点-id映射表
        self.vs_dict = {}
        # 城市的对应父节点
        self.city_tree = {}

        self.city_missing = set([])
        self.vs_id_missing = set([])
        self.vs_missing = set([])

    def start_requests(self):
        yield Request(url='http://www.baidu.com')

    @staticmethod
    def get_config(section, key):
        config = ConfigParser.ConfigParser()
        d = os.path.split(os.path.realpath(__file__))[0]
        path = os.path.realpath(os.path.join(d, '../conf/private.cfg'))
        config.read(path)
        return config.get(section, key)

    def parse(self, response):
        host = self.get_config('cms-mysqldb', 'host')
        port = int(self.get_config('cms-mysqldb', 'port'))
        user = self.get_config('cms-mysqldb', 'user')
        passwd = self.get_config('cms-mysqldb', 'passwd')
        my_conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db='lvplan', cursorclass=DictCursor,
                                  charset='utf8')
        cursor = my_conn.cursor()
        param = getattr(self, 'param', {})
        stmt = 'SELECT * FROM pre_plan WHERE is_delete=0 AND abroad=1'
        if 'cond' in param:
            stmt = '%s AND %s' % (stmt, ' AND '.join(param['cond']))
        if 'limit' in param:
            stmt = '%s LIMIT %s' % (stmt, param['limit'][0])

        cursor.execute(stmt)

        for entry in cursor:
            item = PlanItem()
            item['planId'] = entry['id']
            item['title'] = entry['title']
            if entry['tags']:
                item['tags'] = filter(lambda val: val, [tmp.strip() for tmp in re.split(ur'[,，]', entry['tags'])])
            else:
                item['tags'] = []
            for k1, k2 in (('intro', 'desc'), ('detail', 'moreDesc')):
                tmp = entry[k1].strip() if entry[k1] else ''
                # TODO 原始数据里面有些时候出现了html标签，暂时不作处理
                if '<div' in tmp:
                    continue
                item[k2] = tmp
            item['manualPriority'] = entry['orderby']
            item['mTime'] = entry['update_time']
            item['lxpTag'] = entry['one_tag']

            if entry['city'] in self.city_missing:
                continue
            loc = self.fetch_loc(entry['city'])
            if not loc:
                self.log(u'CITY %s CANNOT BE FOUND' % entry['city'], log.CRITICAL)
                self.city_missing.add(entry['city'])
                continue

            # 处理获得景点-id映射
            aview = filter(lambda val: val, [tmp.strip() for tmp in re.split(ur'[,、，]', entry['aviews'])]) if entry[
                'aviews'] else []
            vids = filter(lambda val: val, [tmp.strip() for tmp in re.split(ur'[,、，]', entry['vids'])]) if entry[
                'vids'] else []
            for idx in xrange(min(len(vids), len(aview))):
                vs_id = vids[idx]
                if vs_id == '0':
                    continue
                elif vs_id == '1':
                    vs = aview[idx]
                    if vs not in self.vs_dict and vs not in self.vs_missing:
                        self.log(u'%d: VS %s NOT FOUND' % (item['planId'], vs), log.CRITICAL)
                        self.vs_missing.add(vs)
                else:
                    vs_id_str = vs_id
                    if vs_id_str in self.vs_id_missing:
                        continue
                    try:
                        vs_id = ObjectId(vs_id)
                    except InvalidId:
                        self.log('VS_ID %s INVALID' % vs_id)
                    vs = self.fetch_vs_id(vs_id)
                    if not vs:
                        self.log(u'%d: VS_ID %s NOT FOUND' % (item['planId'], vs_id), log.CRITICAL)
                        self.vs_id_missing.add(vs_id)
                        continue

                    if aview[idx] != vs['name']:
                        self.log(u'%s -> %s' % (aview[idx], vs['name']), log.INFO)
                    self.vs_dict[aview[idx]] = vs['_id']

            targets = {}
            plan_details = []

            # 1Day: 遇龙河漂流,大榕树,月亮山,菩萨水岩,银子岩 2Day: 银子岩,兴坪,古镇渔村,兴坪,杨堤
            # 按天拆分
            for day_entry in filter(lambda val: val,
                                    [tmp.strip() for tmp in
                                     re.split(r'\d+Day:\s*', entry['views'], flags=re.IGNORECASE)]):
                # 按景点拆分
                plan_day = []
                for vs_name in filter(lambda val: val, [tmp.strip() for tmp in re.split(ur'[、,，]', day_entry)]):
                    m_idx = vs_name.find('-')
                    if m_idx >= 0:
                        vs_name = vs_name[m_idx + 1:]
                    # 是否在人工映射表中已存在？
                    if vs_name in self.vs_dict:
                        vs = self.fetch_vs_id(self.vs_dict[vs_name])
                    else:
                        # 先尝试精确匹配
                        vs = self.fetch_vs(vs_name, style=0, city=loc)
                        if not vs:
                            vs = self.fetch_vs(vs_name, style=1, city=loc)

                    if not vs:
                        continue

                    # 获得景点的城市树
                    city = vs['city']
                    if city['id'] not in self.city_tree:
                        self.city_tree[city['id']] = self.fetch_loc_tree(city['id'])
                    for tmp in self.city_tree[city['id']].values():
                        targets[tmp['id']] = tmp

                    try:
                        lng, lat = vs['location']['coordinates']
                    except KeyError:
                        lng, lat = None, None
                    vs_item = {'item': {'id': vs['_id'], 'zhName': vs['name']},
                               'loc': {'id': city['id'], 'zhName': city['zhName']},
                               'type': 'vs', 'lng': lng, 'lat': lat}
                    plan_day.append(vs_item)

                if plan_day:
                    plan_details.append({'actv': plan_day})

            item['targets'] = targets.values()
            item['details'] = plan_details
            item['days'] = len(plan_details)
            item['enabled'] = True

            details = []
            for day_entry in item['details']:
                details.append(u'(%s)' % ('->'.join([tmp['item']['zhName'] for tmp in day_entry['actv']])))
            self.log('PlanId: %d, %s' % (item['planId'], ' '.join(details)), log.INFO)

            yield item


    def fetch_loc(self, name, stype=0):
        """
        获得城市
        :param stype: 0: 精确匹配; 1: 前缀匹配; 2: 模糊匹配
        """
        col = utils.get_mongodb('geo', 'Locality', profile='mongodb-general')
        loc_list = list(
            col.find({'alias': name}, {'zhName': 1, 'enName': 1, 'location': 1}).sort('level', pymongo.ASCENDING).limit(
                1))
        if not loc_list:
            return None
        return loc_list[0]

    def fetch_vs(self, name, style=0, city=None, proximity=500):
        """
        查找景点
        :param name:
        :param city: （可选）景点位于哪个城市
        :param proximity: （可选）景点和城市的偏差阈值
        """
        col = utils.get_mongodb('poi', 'ViewSpot', profile='mongodb-general')
        if style == 0:
            query = {'alias': name}
        else:
            q_name = name.replace('(', r'\(').replace(')', r'\)')
            query = {'alias': re.compile('^' + q_name)}
        if city and city['location']:
            lng, lat = city['location']['coordinates']
            query['location'] = {'$near': {'$geometry': {'type': 'Point',
                                                         'coordinates': [lng, lat]},
                                           '$minDistance': 0,
                                           '$maxDistance': proximity * 1000}}
        vs_list = list(col.find(query, {'name': 1, 'location': 1, 'city': 1}).limit(1))
        # 取距离city最近的一个
        return vs_list[0] if vs_list else None

    def fetch_vs_id(self, vs_id):
        col = utils.get_mongodb('poi', 'ViewSpot', profile='mongodb-general')
        return col.find_one({'_id': vs_id}, {'name': 1, 'location': 1, 'city': 1})

    def fetch_loc_tree(self, loc_id):
        col = utils.get_mongodb('geo', 'Locality', profile='mongodb-general')
        ret = {}
        while True:
            loc = col.find_one({'_id': loc_id}, {'zhName': 1, 'superAdm': 1})
            if not loc:
                break
            ret[loc_id] = {'id': loc_id, 'zhName': loc['zhName']}
            try:
                loc_id = loc['superAdm']['id']
            except KeyError:
                break
        return ret


class PlanImportPipeline(object):
    spiders = [PlanImportSpider.name]

    def process_item(self, item, spider):
        col = utils.get_mongodb('plan', 'Plan', profile='mongodb-general')
        plan = col.find_one({'planId': item['planId']})
        if not plan:
            plan = {}

        for k in item:
            if item[k]:
                plan[k] = item[k]

        col.save(plan)