# coding=utf-8
import random
import re
import math

import MySQLdb
from MySQLdb.cursors import DictCursor
from bson import ObjectId
from bson.errors import InvalidId
import pymongo
from scrapy import Item, Field, Request, log

import conf
from spiders import AizouCrawlSpider, AizouPipeline


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
    cover = Field()


class PlanImportSpider(AizouCrawlSpider):
    """
    抓取到路线以后，我们会经过人工审核，放在MySQL数据库中。该爬虫类的作用，是将这些路线导入MongoDB数据库。
    """
    name = 'plan-import'
    uuid = '1aa057c0-747d-11e4-b116-123b93f75cba'

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

    def parse(self, response):
        section = conf.global_conf.get('cms-mysqldb', {})
        host = section['host']
        port = int(section['port'])
        user = section['user']
        passwd = section['passwd']
        my_conn = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db='lvplan', cursorclass=DictCursor,
                                  charset='utf8')
        cursor = my_conn.cursor()
        param = getattr(self, 'param', {})
        stmt = u'SELECT * FROM pre_plan WHERE is_delete=0'
        if 'cond' in param:
            stmt = u'%s AND %s' % (stmt, u' AND '.join(param['cond']))
        if 'limit' in param:
            stmt = u'%s LIMIT %s' % (stmt, param['limit'][0])
        stmt = stmt.encode('utf-8')

        cursor.execute(stmt)

        for entry in cursor:
            item = PlanItem()
            item['planId'] = entry['id']
            self.log('Parsing plan: %d' % entry['id'], log.INFO)

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

            # 由于viewspot发生了重大变化，原有的人工映射不再有效。现在忽略aview和vids这对映射关系
            aview = []
            vids = []
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
            imagestore = []
            plan_visited_vs = set([])

            # 1Day: 遇龙河漂流,大榕树,月亮山,菩萨水岩,银子岩 2Day: 银子岩,兴坪,古镇渔村,兴坪,杨堤
            # 按天拆分
            for day_entry in filter(lambda val: val,
                                    [tmp.strip() for tmp in
                                     re.split(r'\d+Day:\s*', entry['views'], flags=re.IGNORECASE)]):
                # 按景点拆分
                plan_day = []
                # 这一天所在的城市，默认为整条路线所对应的城市
                day_loc = loc

                # 拆分景点
                vs_list = filter(lambda val: val, [tmp.strip() for tmp in re.split(ur'[、,，]', day_entry)])
                if not vs_list:
                    continue

                if entry['abroad'] == 1:
                    # 尝试获得这一天活动所在的城市
                    first_vs = vs_list[0]
                    m_idx = first_vs.find('-')
                    city_name = None
                    if m_idx >= 0:
                        vs_list[0] = first_vs[m_idx + 1:]
                        city_name = first_vs[:m_idx]
                    else:
                        # 查看一下vs_list的第一个是否为城市
                        if self.fetch_loc(vs_list[0]):
                            city_name = vs_list[0]
                            vs_list = vs_list[1:]

                    if city_name and city_name not in self.city_missing:
                        tmp = self.fetch_loc(city_name)
                        if not tmp:
                            self.log(u'CITY %s CANNOT BE FOUND' % city_name, log.CRITICAL)
                            self.city_missing.add(city_name)
                        else:
                            day_loc = tmp

                if 'location' not in day_loc:
                    continue
                location = day_loc['location']

                # 同一个景点同一天不能访问超过一次
                visited_vs = set([])
                for vs_idx, vs_name in enumerate(vs_list):
                    # 每天的第一个景点：要求和城市中心坐标距离不超过150，其它景点：和上一个景点坐标相比较
                    proximity = 250 if vs_idx == 0 else 100

                    # 是否在人工映射表中已存在？
                    if vs_name in self.vs_dict:
                        vs = self.fetch_vs_id(self.vs_dict[vs_name])
                    else:
                        # 先尝试精确匹配
                        vs = self.fetch_vs(vs_name, style=0, location=location, proximity=proximity)
                        if not vs:
                            vs = self.fetch_vs(vs_name, style=1, location=location, proximity=proximity)

                    if not vs:
                        continue
                    if vs['_id'] in visited_vs:
                        continue

                    self.log(u'Fetched: %s - %s' % (vs['_id'], vs['zhName']))

                    # 获得图像
                    r = vs['rating'] if 'rating' in vs and vs['rating'] else 0.6
                    if 'images' not in vs or not vs['images']:
                        vs['images'] = []
                    for tmp in vs['images']:
                        imagestore.append({'url': tmp['url'], 'fSize': tmp['fSize'], 'w': tmp['w'], 'h': tmp['h'],
                                           'weight': tmp['fSize'] * math.pow(r, 2.5)})

                    visited_vs.add(vs['_id'])
                    plan_visited_vs.add(vs['_id'])
                    # 用该景点的坐标代替location
                    location = vs['location']

                    for t in vs['targets'] if 'targets' in vs else []:
                        if t not in targets:
                            targets[t] = {'id': t, '_id': t}

                    # 获得景点的城市树
                    if 'locality' in vs:
                        vs_loc = self.fetch_loc_id(vs['locality']['_id'])
                        if vs_loc['_id'] not in self.city_tree:
                            self.city_tree[vs_loc['_id']] = self.fetch_loc_tree(vs_loc['_id'])
                        for tmp in self.city_tree[vs_loc['_id']].values():
                            targets[tmp['_id']] = tmp

                    try:
                        lng, lat = vs['location']['coordinates']
                    except KeyError:
                        lng, lat = None, None

                    vs_item = {'id': vs['_id'], '_id': vs['_id']}
                    for tmp in ('zhName', 'enName'):
                        if tmp in vs:
                            vs_item[tmp] = vs[tmp]
                    loc_item = {'id': vs_loc['_id'], '_id': vs_loc['_id']}
                    for tmp in ('zhName', 'enName'):
                        if tmp in vs_loc:
                            loc_item[tmp] = vs_loc[tmp]
                    plan_day.append({'item': vs_item, 'loc': loc_item, 'type': 'vs', 'lng': lng, 'lat': lat})

                if plan_day:
                    plan_details.append({'actv': plan_day})

            if not plan_details or len(plan_visited_vs) < 3:
                continue

            item['targets'] = targets.values()
            item['details'] = plan_details
            item['days'] = len(plan_details)
            item['enabled'] = True

            # 随机取一张图像
            if imagestore:
                imagestore = sorted(imagestore, key=lambda val: val['weight'], reverse=True)
                if len(imagestore) > 20:
                    imagestore = imagestore[:20]
                for tmp in imagestore:
                    tmp.pop('weight')
                tmp = imagestore[random.randint(0, len(imagestore) - 1)]
                item['cover'] = tmp
                item['images'] = imagestore

            details = []
            for day_entry in item['details']:
                details.append(u'(%s)' % ('->'.join([tmp['item']['zhName'] for tmp in day_entry['actv']])))
            self.log('PlanId: %d, %s' % (item['planId'], ' '.join(details)), log.INFO)

            yield item

    def fetch_loc_id(self, cid):
        col = self.fetch_db_col('geo', 'Locality', 'mongodb-general')
        ret = col.find_one({'_id': cid}, {'zhName': 1, 'enName': 1})
        return ret

    def fetch_loc(self, name, stype=0):
        """
        获得城市
        :param stype: 0: 精确匹配; 1: 前缀匹配; 2: 模糊匹配
        """
        col = self.fetch_db_col('geo', 'Locality', 'mongodb-general')
        loc_list = list(
            col.find({'alias': name}, {'zhName': 1, 'enName': 1, 'location': 1}).sort('level', pymongo.ASCENDING).limit(
                1))
        if not loc_list:
            return None
        return loc_list[0]

    def fetch_vs(self, name, style=0, location=None, proximity=150):
        """
        查找景点
        :param name:
        :param location: （可选）制定一个坐标
        :param proximity: （可选）景点和指定坐标的偏差阈值
        """
        col = self.fetch_db_col('poi', 'ViewSpot', 'mongodb-general')
        if style == 0:
            query = {'alias': name}
        else:
            q_name = name.replace('(', r'\(').replace(')', r'\)')
            query = {'alias': re.compile('^' + q_name)}
        if location:
            lng, lat = location['coordinates']
            query['location'] = {'$near': {'$geometry': {'type': 'Point',
                                                         'coordinates': [lng, lat]},
                                           '$minDistance': 0,
                                           '$maxDistance': proximity * 1000}}
        vs_list = list(
            col.find(query, {'zhName': 1, 'enName': 1, 'location': 1, 'locality': 1, 'images': 1, 'rating': 1,
                             'targets': 1}).limit(1))
        # 取距离city最近的一个
        return vs_list[0] if vs_list else None

    def fetch_vs_id(self, vs_id):
        col = self.fetch_db_col('poi', 'ViewSpot', 'mongodb-general')
        return col.find_one({'_id': vs_id},
                            {'zhName': 1, 'enName': 1, 'location': 1, 'locality': 1, 'images': 1, 'rating': 1,
                             'targets': 1})

    def fetch_loc_tree(self, loc_id):
        col = self.fetch_db_col('geo', 'Locality', 'mongodb-general')
        ret = {}
        while True:
            loc = col.find_one({'_id': loc_id}, {'zhName': 1, 'superAdm': 1})
            if not loc:
                break
            ret[loc_id] = {'_id': loc_id, 'id': loc_id, 'zhName': loc['zhName']}
            try:
                loc_id = loc['superAdm']['id']
            except KeyError:
                break
        return ret


class PlanImportPipeline(AizouPipeline):
    spiders = [PlanImportSpider.name]

    spiders_uuid = [PlanImportSpider.uuid]

    def __init__(self, param):
        super(PlanImportPipeline, self).__init__(param)

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        col = self.fetch_db_col('plan', 'Plan', 'mongodb-general')
        plan = col.find_one({'planId': item['planId']})
        if not plan:
            plan = {}

        for k in item:
            if item[k]:
                plan[k] = item[k]

        col.save(plan)