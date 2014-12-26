# coding=utf-8
import copy
import hashlib
import json
import re
import urlparse

from scrapy import Item, Request, Field, Selector, log

from spiders import AizouCrawlSpider, AizouPipeline, ProcImagesMixin
from spiders.baidu_mixin import BaiduSugMixin
import utils


__author__ = 'zephyre'


class MafengwoItem(Item):
    data = Field()


class MafengwoSpider(AizouCrawlSpider):
    """
    马蜂窝目的地的抓取
    """

    name = 'mafengwo'
    uuid = '74f9b075-65f3-400d-b093-5bdbdb552e86'

    def __init__(self, *a, **kw):
        super(MafengwoSpider, self).__init__(*a, **kw)
        self.cont_filter = None
        self.region_filter = None
        self.cont_list = [
            52314,  # 亚洲
            10853,  # 南极州
            14701,  # 大洋洲
            14517,  # 非洲
            14383,  # 欧洲
            16406,  # 南美
            16867  # 北美
        ]

    def start_requests(self):
        # 大洲的过滤
        if 'cont' in self.param:
            self.cont_filter = [int(tmp) for tmp in self.param['cont']]

        if 'region' in self.param:
            self.region_filter = [int(tmp) for tmp in self.param['region']]

        if self.region_filter:
            start_entries = [{'rid': tmp, 'level': 'region'} for tmp in self.region_filter]
        else:
            start_entries = [{'rid': tmp, 'level': 'cont'} for tmp in
                             (self.cont_filter if self.cont_filter else self.cont_list)]

        for entry in start_entries:
            rid = entry['rid']
            level = entry['level']
            url = 'http://www.mafengwo.cn/gonglve/sg_ajax.php?sAct=getMapData&iMddid=%d&iType=1' % rid
            yield Request(url=url, meta={'id': rid, 'crumb': [rid], 'level': level, 'iType': 1},
                          callback=self.parse_mdd_ajax)

    def parse_mdd_ajax(self, response):
        """
        解析：http://www.mafengwo.cn/gonglve/sg_ajax.php?sAct=getMapData&iMddid=52314&iType=3
        :param response:
        """
        ret = json.loads(response.body)
        level = response.meta['level'] if 'level' in response.meta else None
        crumb = response.meta['crumb']
        itype = response.meta['iType']

        self.log('Parsing: %s' % response.url, log.INFO)

        type_mapping = {
            1: 'cy',
            2: 'hotel',
            3: 'vs',
            4: 'gw',
            5: 'yl',
            6: 'trans'
        }

        if ret['mode'] == 1:
            # 进一步抓取目的地
            for entry in ret['list']:
                oid = entry['id']

                if level == 'cont' and self.region_filter:
                    if oid not in self.region_filter:
                        continue

                data = {'id': oid, 'title': entry['name'], 'lat': entry['lat'], 'lng': entry['lng'],
                        'vs_cnt': entry['rank'], 'comment_cnt': entry['num_comment']}
                img = entry['img_link']
                img = re.sub(r'gonglve\.w\d+\.', '', img)
                data['cover'] = img
                crumb_1 = copy.deepcopy(crumb)
                crumb_1.append(oid)
                data['crumb'] = crumb_1
                data['type'] = 'country' if level == 'cont' else 'region'

                item = MafengwoItem()
                item['data'] = data

                yield Request(url='http://www.mafengwo.cn/travel-scenic-spot/mafengwo/%d.html' % oid,
                              meta={'id': oid, 'item': item}, dont_filter=True,
                              callback=self.parse_mdd_home)

                yield Request(url='http://www.mafengwo.cn/gonglve/sg_ajax.php?sAct=getMapData&iMddid=%d&iType=1' % oid,
                              meta={'crumb': copy.deepcopy(crumb_1), 'iType': 1},
                              callback=self.parse_mdd_ajax)
        elif ret['mode'] == 2:
            if not ('skip' in self.param and type_mapping[itype] in self.param['skip']):
                # 跳过某些POI类型，抓取poi
                for entry in ret['list']:
                    data = {'type': type_mapping[itype]}

                    oid = entry['id']
                    data['id'] = oid
                    data['title'] = entry['name']
                    data['lat'] = entry['lat']
                    data['lng'] = entry['lng']
                    data['rating'] = float(entry['rank']) / 5
                    data['comment_cnt'] = entry['num_comment']
                    img = entry['img_link']
                    img = re.sub(r'rbook_comment\.w\d+\.', '', img)
                    data['cover'] = img
                    crumb_1 = copy.deepcopy(crumb)
                    data['crumb'] = crumb_1

                    item = MafengwoItem()
                    item['data'] = data

                    yield Request(url='http://www.mafengwo.cn/poi/%d.html' % oid,
                                  meta={'id': oid, 'item': item},
                                  callback=self.parse_poi)
            new_t = itype
            while new_t < 6:
                new_t += 1
                if 'skip' in self.param and type_mapping[new_t] in self.param['skip']:
                    continue

                match = re.search(r'iMddid=(\d+)', response.url)
                yield Request(
                    url='http://www.mafengwo.cn/gonglve/sg_ajax.php?sAct=getMapData&iMddid=%d&iType=%d' % (
                        int(match.group(1)), new_t),
                    meta={'crumb': copy.deepcopy(crumb), 'iType': new_t},
                    callback=self.parse_mdd_ajax)

                # # 尝试抓取自身
                # if 'inc-self' in self.param:
                # self_oid = response.meta['id']
                # yield Request(url='http://www.mafengwo.cn/travel-scenic-spot/mafengwo/%d.html' % self_oid,
                # meta={'id': self_oid, 'crumb': copy.deepcopy(crumb)},
                # callback=self.parse_mdd_home, dont_filter=True)

    def parse_mdd_home(self, response):
        if 'item' not in response.meta:
            # 仅仅尝试获得下面的景点
            # 从目的地页面出发，解析POI
            yield Request(url='http://www.mafengwo.cn/jd/%d/gonglve.html' % response.meta['id'],
                          meta={'type': 'region', 'crumb': response.meta['crumb']},
                          callback=self.parse_jd)
            return

        item = response.meta['item']
        data = item['data']

        sel = Selector(response)
        for node1 in sel.xpath('//div[contains(@class,"m-tags")]/div[@class="bd"]/div[@class="t-info"]/p'):
            span_list = node1.xpath('./span/text()').extract()
            if 2 > len(span_list) > 0:
                self.log('Unsupported region homepage: %d' % data['id'], log.WARNING)
                continue
            elif not span_list:
                continue

            hdr = span_list[0].strip()
            contents = '\n'.join([tmp.strip() for tmp in span_list[1:]])
            if hdr == u'最佳旅游季节':
                data['travel_month'] = contents
            elif hdr == u'建议游玩天数':
                data['time_cost'] = contents
            else:
                self.log('Unsupported region homepage: %d' % data['id'], log.WARNING)
                continue

        tags = sel.xpath(
            '//div[contains(@class,"m-tags")]/div[@class="bd"]/ul/li[@class="impress-tip"]/a[@href]/text()').extract()
        data['tags'] = list(set(filter(lambda val: val, [tmp.strip() for tmp in tags])))
        data['images_tot'] = int(sel.xpath('//div[@class="m-photo"]/a/em/text()').extract()[0])

        col_list = []
        for node in sel.xpath('//ul[@class="nav-box"]/li[contains(@class,"nav-item")]/a[@href]'):
            for info_node in node.xpath('..//dl/dt/a[@href]'):
                info_url = self.build_href(response.url, info_node.xpath('./@href').extract()[0])
                info_cat = info_node.xpath('./text()').extract()[0].strip()
                col_list.append([info_url, self.parse_info, {'info_cat': info_cat}])

        if col_list:
            col_url, cb, meta = col_list[0]
            col_list = col_list[1:]
            meta['col_list'] = col_list
            meta['item'] = item
            yield Request(url=col_url, callback=cb, meta=meta)
        else:
            yield item

        # 从目的地页面出发，解析POI
        for poi_type in ['vs', 'cy', 'yl', 'gw']:
            if 'skip' in self.param and poi_type in self.param['skip']:
                continue

            url_key = poi_type if poi_type != 'vs' else 'jd'

            yield Request(url='http://www.mafengwo.cn/%s/%d/gonglve.html' % (url_key, data['id']),
                          meta={'type': poi_type, 'crumb': data['crumb']},
                          callback=self.parse_jd)

    def parse_poi_list(self, response):
        """
        解析页面内的poi列表
        :param response:
        :return:
        """
        poi_type = response.meta['poi_type']
        crumb = response.meta['crumb']

        for node in Selector(response).xpath(
                '//ul[@class="poi-list"]/li[contains(@class,"item")]'):
            try:
                item = MafengwoItem()
                data = {}
                item['data'] = data
                href = node.xpath('./div[@class="title"]//a[@href]/@href').extract()[0]
                data['id'] = int(re.search(r'/poi/(\d+)\.html', href).group(1))
                data['rating'] = float(node.xpath('./div[@class="grade"]/em/text()').extract()[0]) / 5
                data['comment_cnt'] = int(
                    node.xpath('./div[@class="grade"]/p[@class="rev-num"]/em/text()').extract()[0])
                data['title'] = node.xpath('./div[@class="title"]//a[@href]/text()').extract()[0]
                data['crumb'] = copy.deepcopy(crumb)
                data['type'] = poi_type

                yield Request(self.build_href(response.url, href), meta={'id': data['id'], 'item': item},
                              callback=self.parse_poi)
            except (KeyError, IndexError):
                pass

    def parse_jd(self, response):
        sel = Selector(response)

        ctype = response.meta['type']
        # 如果是目的地页面，其下的所有poi都为景点类型。否则，poi_type和ctype一致，比如都为gw, cy等
        poi_type = 'vs' if ctype == 'region' else ctype
        response.meta['poi_type'] = poi_type
        crumb = copy.deepcopy(response.meta['crumb'])

        # 抓取景点页面中的
        results = self.parse_poi_list(response)
        if results:
            for entry in results:
                yield entry

        # poi列表的翻页
        for href in sel.xpath('//div[@class="page-hotel"]/a[@href]/@href').extract():
            yield Request(self.build_href(response.url, href), callback=self.parse_poi_list,
                          meta={'poi_type': poi_type, 'crumb': copy.deepcopy(crumb)})

    def parse_info(self, response):
        sel = Selector(response)
        item = response.meta['item']
        info_cat = response.meta['info_cat']
        data = item['data']

        if 'title' not in data:
            tmp = sel.xpath(
                '//div[contains(@class,"sub-nav")]/div[@class="mdd-title"]/span[@class="s-name"]/'
                'text()').extract()[0].strip()
            m = re.search(ur'(.+)(城市|国家)概况', tmp)
            data['title'] = m.group(1).strip()

        if 'contents' not in data:
            data['contents'] = []
        contents = data['contents']
        entry = {}
        for node in sel.xpath('//div[@class="content"]/div[@class]|//div[@class="content"]/ul[@class]'):
            class_name = node.xpath('./@class').extract()[0]
            if 'm-subTit' in class_name:
                contents.append(entry)
                entry = {'title': node.xpath('./h2/text()').extract()[0].strip(), 'info_cat': info_cat, 'details': []}
            elif 'm-txt' in class_name or 'm-img' in class_name:
                entry['details'].append(node.extract().strip())
            elif 'm-tripDate' in class_name:
                entry['details'].append(node.extract().strip())
            else:
                continue
        contents.append(entry)
        data['contents'] = filter(lambda val: val, contents)
        col_list = response.meta['col_list']
        if col_list:
            col_url, cb, meta = col_list[0]
            col_list = col_list[1:]
            meta['col_list'] = col_list
            meta['item'] = item
            yield Request(url=col_url, callback=cb, meta=meta)
        else:
            # 开始抓取图像
            url = 'http://www.mafengwo.cn/mdd/ajax_photolist.php?act=getMddPhotoList&mddid=%d&page=1' % data['id']
            yield Request(url=url, callback=self.parse_photo, meta={'item': item, 'page': 1, 'act': 'getMddPhotoList'},
                          errback=self.photo_err)

    def parse_poi(self, response):
        sel = Selector(response)
        item = response.meta['item']
        data = item['data']
        comment_cnt = 0

        try:
            loc_data = json.loads('{%s}' % re.search(r'window\.Env\s*=\s*\{(.+?)\}\s*;', response.body).group(1))
            comment_cnt = loc_data['comment_count']

            # 如果通过页面进来而不是接口，此时还没有坐标信息。需要抓取
            if not ('lat' in data and 'lng' in data):
                data['lat'] = loc_data['lat']
                data['lng'] = loc_data['lng']

        except (ValueError, TypeError):
            pass

        tmp = sel.xpath('//div[contains(@class,"m-details")]/div[contains(@class,"title")]//h1/text()').extract()
        if not tmp:
            tmp = sel.xpath('//div[contains(@class,"m-intro")]/dl[contains(@class,"intro-title")]//h1/text()').extract()
        title = tmp[0].strip()

        # pid = int(re.search('mafengwo\.cn/poi/(\d+)\.html', response.url).group(1))
        #
        # loc_data = json.loads('{%s}' % re.search(r'window\.Env\s*=\s*\{(.+?)\}\s*;', response.body).group(1))
        # lat = loc_data['lat']
        # lng = loc_data['lng']

        # score = float(
        # sel.xpath('//div[@class="txt-l"]/div[@class="score"]/span[@class="score-info"]/em/text()').extract()[0]) / 5
        # comment_cnt = int(
        # sel.xpath('//div[@class="txt-l"]/div[@class="score"]/p[@class="ranking"]/em/text()').extract()[0])

        photo_cnt = 0
        tmp = sel.xpath('//div[@class="pic-r"]/a[@href]/span[@class="pic-num"]/text()').extract()
        if tmp:
            photo_cnt = int(re.search(ur'共(\d+)张', tmp[0]).group(1))
        else:
            tmp = sel.xpath('//div[@id="main_pic"]/span/strong[@title]/text()').extract()
            if tmp:
                photo_cnt = int(tmp[0])

        # 补全crumb
        crumb = data['crumb']
        for href in sel.xpath(
                '//div[@class="crumb"]/div[@class="item"]/div[@class="drop"]/span[@class="hd"]/a[@href]/@href') \
                .extract():
            match = re.search(r'/travel-scenic-spot/mafengwo/(\d+)\.html', href)
            if not match:
                continue
            crumb_id = int(match.group(1))
            if crumb_id not in crumb:
                crumb.append(crumb_id)

        desc = []
        desc_entry = {}
        for node in sel.xpath('//div[contains(@class,"poi-info")]/div[@class="bd"]/*'):
            node_str = node.extract()
            node_name = re.search(r'^\s*<([^<>]+)>', node_str).group(1).strip()
            if node_name == 'h3':
                desc.append(desc_entry)
                desc_entry = {'name': node.xpath('./text()').extract()[0].strip()}
            elif node_name == 'p':
                desc_entry['contents'] = node_str
            else:
                assert False
        desc.append(desc_entry)
        desc = filter(lambda val: val, desc)

        data['title'] = title
        data['images_tot'] = photo_cnt
        data['desc'] = desc

        url = 'http://www.mafengwo.cn/mdd/ajax_photolist.php?act=getPoiPhotoList&poiid=%d&page=1' % data['id']
        yield Request(url=url, meta={'item': item, 'page': 1, 'act': 'getPoiPhotoList'}, callback=self.parse_photo,
                      errback=self.photo_err)

        # 抓取评论
        for page_idx in xrange(1, int(comment_cnt / 15) + 2):
            url = 'http://www.mafengwo.cn/gonglve/ajax.php?act=get_poi_comments&poi_id=%d&type=0&category=1&page=%d' \
                  % (data['id'], page_idx)
            yield Request(url=url, meta={'item': item}, callback=self.parse_comments)

    def parse_comments(self, response):
        item = response.meta['item']
        poi = item['data']

        ret = json.loads(response.body)
        if ret['ret'] != 1:
            return

        html = ret['html']['html']
        sel = Selector(text=html)
        for entry_node in sel.xpath('//div[@class="comment-item"]'):
            entry = entry_node.extract()
            comment_id = int(re.search(r'comment_no_(\d+)', entry_node.xpath('./@id').extract()[0]).group(1))
            data = {'poi_id': poi['id'], 'comment_id': comment_id, 'contents': entry, 'type': 'comments'}
            item = MafengwoItem()
            item['data'] = data
            yield item


    def parse_photo(self, response):
        sel = Selector(response)
        item = response.meta['item']
        page = response.meta['page']
        data = item['data']

        images = data['imageList'] if 'imageList' in data else []
        data['imageList'] = images
        node_list = sel.xpath('//ul/li')
        # 最多去50张照片左右
        if not node_list or len(images) > 200:
            # 已经到尽头
            self.log('Yielding item: title=%s, type=%s, id=%d' % (data['title'], data['type'], data['id']), log.INFO)
            yield item
        else:
            for node in node_list:
                img_node = node.xpath('./a[@href]/img')[0]
                img_url = self.build_href(response.url, img_node.xpath('./@src').extract()[0])
                img_url = re.sub(r'\.[^\.]+\.w\d+\.', '.', img_url)
                tmp = img_node.xpath('./@title').extract()
                if tmp and tmp[0].strip():
                    title = tmp[0].strip()
                else:
                    title = None

                try:
                    favor_cnt = int(node.xpath('./span[contains(@class,"num-like")]/text()').extract()[0])
                except (IndexError, ValueError):
                    favor_cnt = 0

                image = {'url': img_url, 'favor_cnt': favor_cnt}
                if title:
                    image['title'] = title

                tmp = node.xpath('./div[contains(@class,"info")]/p/a[@href]')
                if tmp:
                    user_node = tmp[0]
                    user_url = self.build_href(response.url, user_node.xpath('./@href').extract()[0])
                    user_id = int(re.search(r'/u/(\d+)\.html', user_url).group(1))
                    user_name = user_node.xpath('./text()').extract()[0].strip()
                    image['user_id'] = user_id
                    image['user_name'] = user_name

                if img_url not in [tmp['url'] for tmp in images]:
                    images.append(image)

            page += 1
            act = response.meta['act']
            param_name = 'mddid' if act == 'getMddPhotoList' else 'poiid'
            url = 'http://www.mafengwo.cn/mdd/ajax_photolist.php?act=%s&%s=%d&page=%d' % (
                act, param_name, data['id'], page)
            yield Request(url=url, meta={'item': item, 'page': page, 'act': act}, callback=self.parse_photo,
                          errback=self.photo_err)

    def photo_err(self, failure):
        status = None
        try:
            status = failure.value.response.status
            if status == 404 or status == 403 or status == 400:
                meta = failure.request.meta
                item = meta['item']
                page = meta['page'] + 1
                data = item['data']
                images = data['imageList'] if 'imageList' in data else []
                data['imageList'] = images
                act = meta['act']
                param_name = 'mddid' if act == 'getMddPhotoList' else 'poiid'
                url = 'http://www.mafengwo.cn/mdd/ajax_photolist.php?act=%s&%s=%d&page=%d' % (
                    act, param_name, data['id'], page)
                return Request(url=url, meta={'item': item, 'page': page, 'act': act}, callback=self.parse_photo,
                               errback=self.photo_err)
        except AttributeError:
            pass

        self.log('Downloading images failed. Code=%d, url=%s' % (status, failure.request.url), log.WARNING)


class MafengwoPipeline(AizouPipeline):
    spiders = [MafengwoSpider.name]

    spiders_uuid = [MafengwoSpider.uuid]

    def __init__(self, param):
        super(MafengwoPipeline, self).__init__(param)

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']
        item_type = data['type']

        if item_type == 'country':
            col_name = 'MafengwoCountry'
        elif item_type == 'region':
            col_name = 'MafengwoMdd'
        elif item_type == 'vs':
            col_name = 'MafengwoVs'
        elif item_type == 'gw':
            col_name = 'MafengwoGw'
        elif item_type == 'yl':
            col_name = 'MafengwoYl'
        elif item_type == 'cy':
            col_name = 'MafengwoCy'
        elif item_type == 'hotel':
            col_name = 'MafengwoHotel'
        elif item_type == 'trans':
            col_name = 'MafengwoTrans'
        elif item_type == 'comments':
            col_name = 'MafengwoComment'
            col = self.fetch_db_col('raw_data', col_name, 'mongodb-crawler')
            col.update({'poi_id': data['poi_id'], 'comment_id': data['comment_id']}, {'$set': data}, upsert=True)
            return item
        else:
            return item

        # 存储原始图像
        col_img = self.fetch_db_col('raw_data', 'MafengwoImage', 'mongodb-crawler')
        if 'imageList' not in data:
            data['imageList'] = []
        image_list = data.pop('imageList')
        for tmp in image_list:
            url = tmp['url']
            # 判断链接的有效性。1：必须是有效url，2：不能使形如http://www.mafengwo.cn之类的url
            components = urlparse.urlparse(url)
            if not components.scheme or not components.netloc or not components.path:
                continue

            tmp['url_hash'] = hashlib.md5(url).hexdigest()
            sig = '%s-%d' % (col_name, data['id'])
            col_img.update({'url_hash': tmp['url_hash']}, {'$set': tmp, '$addToSet': {'itemIds': sig}}, upsert=True)

        # 存储item本身
        col = self.fetch_db_col('raw_data', col_name, 'mongodb-crawler')
        col.update({'id': data['id']}, {'$set': data}, upsert=True)

        return item


class MafengwoProcItem(Item):
    data = Field()
    col_name = Field()
    db_name = Field()


class MafengwoProcSpider(AizouCrawlSpider, BaiduSugMixin):
    """
    马蜂窝目的地的清洗

    参数列表：
    def-hot：默认的热度。默认值为0.3。
    denom：计算热度/评分等的基准因子。默认值为2000。
    lower/upper：分片处理。默认为不分片。
    slice：分片处理的步进。默认为8。
    mdd/vs/gw/cy/...：处理目的地/景点/购物/餐饮等。
    limit：条数限制。
    query：通过查询条件限制处理对象。
    bind-baidu：是否和百度进行绑定。默认为不绑定。
    """

    name = 'mafengwo-proc'
    uuid = '69d64c68-7602-4cb1-a319-1da2853cda67'

    def __init__(self, param, *a, **kw):
        super(MafengwoProcSpider, self).__init__(param, *a, **kw)

        self.def_hot = float(self.param['def-hot'][0]) if 'def-hot' in self.param else 0.3
        self.denom = float(self.param['denom'][0]) if 'denom' in self.param else 2000.0

    def start_requests(self):
        def f(val):
            if val > 255:
                val = 255
            head = hex(val)[2:]
            if len(head) == 1:
                head = '0' + head
            return head

        lower = int(self.param['lower'][0]) if 'lower' in self.param else None
        upper = int(self.param['upper'][0]) if 'upper' in self.param else None

        if lower is not None and upper is not None:
            i = lower
            step = int(self.param['slice'][0]) if 'slice' in self.param else 8
            while True:
                head = f(i)
                tail = f(i + step)
                yield Request(url='http://www.baidu.com', meta={'lower': head, 'upper': tail}, dont_filter=True)

                i += step
                if i > upper:
                    break
        else:
            yield Request(url='http://www.baidu.com', meta={'lower': None, 'upper': None}, dont_filter=True)

    def is_chn(self, text):
        """
        是否为中文
        判断算法：至少出现一个中文字符，并且只包含中文字符及简单ascii字符
        :param text:
        """
        has_chn = False
        for c in text:
            if not has_chn and ord(c) >= 0x4e00 and ord(c) <= 0x9fff:
                has_chn = True
            if ord(c) < 32 or (ord(c) > 126 and ord(c) < 0x4e00) or (ord(c) >= 0x9fff):
                return False

        return has_chn

    def is_eng(self, text):
        for c in text:
            if ord(c) < 32 or ord(c) > 126:
                return False
        return True

    def proc_etree(self, body):
        def f1(node):
            # 去掉可能的包含有蚂蜂窝字样的元素
            if node.text and u'蚂蜂窝' in node.text:
                return True, None
            else:
                return False, node

        def f2(node):
            from urlparse import urlparse

            if node.tag == 'a' and node.get('href'):
                href = node.get('href')
                c = urlparse(href)
                if (not c.scheme and not c.netloc) or 'mafengwo' in c.netloc:
                    del node.attrib['href']

            return False, node

        from utils import parse_etree
        from lxml import etree

        node = parse_etree(body, [f1, f2])
        return etree.tostring(node, encoding='utf-8').decode('utf-8')

    def parse_name(self, name):
        name = name.strip()
        term_list = []

        # 处理括号
        match = re.search(ur'([^\(\)]+)[\(（]([^\(\)]+)[\)）]', name)
        if match:
            term_list.extend([match.group(1), match.group(2)])
        if not term_list:
            term_list = [name]

        name_list = []
        for term in term_list:
            # 处理/的情况
            tmp = filter(lambda val: val,
                         [re.sub(r'\s+', ' ', tmp.strip(), flags=re.U) for tmp in re.split(r'/', term)])
            if not tmp:
                continue
            name_list.extend(tmp)

        # 名称推测算法：从前往后测试。
        # 第一个至少含有一个中文，且可能包含简单英语及数字的term，为zhName。
        # 第一个全英文term，为enName。
        # 第一个既不是zhName，也不是enName的，为localName

        # 优先级
        # zhName: zhName > enName > localName
        # enName: enName > localName
        # localName: localName

        zh_name = None
        en_name = None
        loc_name = None
        for tmp in name_list:
            tmp = tmp.strip()
            if not zh_name and self.is_chn(tmp):
                zh_name = tmp
            elif not en_name and self.is_eng(tmp):
                en_name = tmp
            elif not loc_name:
                loc_name = tmp

        result = {'locName': loc_name}
        if zh_name:
            result['zhName'] = zh_name
        elif en_name:
            result['zhName'] = en_name
        else:
            result['zhName'] = loc_name

        if en_name:
            result['enName'] = en_name
        else:
            result['enName'] = loc_name

        alias = {name.lower()}
        for tmp in name_list:
            alias.add(tmp.lower())
        result['zhName'] = name
        result['alias'] = list(alias)
        return result

    def parse(self, response):
        lower = response.meta['lower']
        upper = response.meta['upper']

        func_map = {'mdd': lambda: self.parse_mdd([lower, upper]),
                    'country': self.parse_country,
                    'vs': lambda: self.parse_poi('MafengwoVs', [lower, upper]),
                    'gw': lambda: self.parse_poi('MafengwoGw', [lower, upper]),
                    'hotel': lambda: self.parse_poi('MafengwoHotel', [lower, upper]),
                    'yl': lambda: self.parse_poi('MafengwoYl', [lower, upper]),
                    'cy': lambda: self.parse_poi('MafengwoCy', [lower, upper])
        }

        for k, v in func_map.items():
            if k in self.param:
                for entry in v():
                    yield entry

    def parse_geocode(self, response):
        item = response.meta['item']
        data = item['data']

        geocode_data = json.loads(response.body)
        if geocode_data['status'] not in ['OK', 'ZERO_RESULTS']:
            self.log('ERROR GEOCODING. STATUS=%s, URL=%s' % (geocode_data['status'], response.url))
        if geocode_data['status'] == 'OK':
            entry = geocode_data['results'][0]
            # 检查是否足够接近
            lng, lat = data['location']['coordinates']
            glat = entry['geometry']['location']['lat']
            glng = entry['geometry']['location']['lng']

            if utils.haversine(lng, lat, glng, glat) < 100:
                tmp = filter(lambda val: 'political' in val['types'] and 'country' not in val['types'],
                             entry['address_components'])
                if tmp:
                    c = tmp[0]
                    alias = set(data['alias']) if 'alias' in data else set([])
                    for key in ['short_name', 'long_name']:
                        alias.add(c[key].strip().lower())
                    data['alias'] = list(alias)

        lang = response.meta['lang']
        if not lang:
            yield item
        else:
            addr = re.search(r'address=(.+?)(&|\s*$)', response.url).group(1)
            lang_set = lang.pop()
            yield Request(url='http://maps.googleapis.com/maps/api/geocode/json?address=%s&sensor=false' % addr,
                          headers={'Accept-Language': lang_set}, callback=self.parse_geocode, dont_filter=True,
                          meta={'item': item, 'lang': lang})

    def parse_country(self):
        col = self.fetch_db_col('raw_data', 'MafengwoCountry', 'mongodb-crawler')

        for entry in col.find({}, {'id': 1, 'title': 1}):
            item = MafengwoProcItem()
            item['data'] = {'zhName': entry['title'], 'id': entry['id']}
            item['db_name'] = 'geo'
            item['col_name'] = 'Country'

            yield item

    def parse_poi(self, col_name, bound):
        col_raw = self.fetch_db_col('raw_data', col_name, 'mongodb-crawler')
        tot_num = col_raw.find({}).count()

        query = json.loads(self.param['query'][0]) if 'query' in self.param else {}
        if bound[0] is not None and bound[1] is not None:
            query['$where'] = 'this._id.str.substring(22)>="%s" && this._id.str.substring(22)<="%s"' % (
                bound[0], bound[1])
        cursor = col_raw.find(query)
        if 'limit' in self.param:
            cursor.limit(int(self.param['limit'][0]))

        self.log('Between %s and %s, %d records to be processed.' % (bound[0], bound[1], cursor.count()), log.INFO)
        for entry in cursor:
            data = {'enabled': True, 'zhName': entry['title'].strip()}

            data['alias'] = [data['zhName'].lower()]

            desc = None
            address = None
            traffic_info = None
            tel = None
            loc_name = None
            en_name = None
            details = []

            for info_entry in entry['desc']:
                txt_entry = info_entry['contents']
                proc_text = '\n'.join(filter(lambda val: val, [tmp.strip() for tmp in Selector(text=txt_entry).xpath(
                    '//p/descendant-or-self::text()').extract()]))

                if info_entry['name'] == u'简介':
                    desc = proc_text
                elif info_entry['name'] == u'地址':
                    address = proc_text
                elif info_entry['name'] == u'交通':
                    traffic_info = proc_text
                elif info_entry['name'] == u'电话':
                    tel = proc_text
                elif info_entry['name'] == u'当地名称':
                    loc_name = proc_text
                    details.append(u'%s：%s' % (info_entry['name'], proc_text))
                elif info_entry['name'] == u'英文名称':
                    en_name = proc_text
                    details.append(u'%s：%s' % (info_entry['name'], proc_text))
                else:
                    details.append(u'%s：%s' % (info_entry['name'], proc_text))

            # 热门程度
            data['commentCnt'] = entry['comment_cnt']
            # 计算hotness
            data['hotness'] = sum(map(lambda k: col_raw.find({k: {'$lt': entry[k]}}).count() / float(tot_num),
                                      ('comment_cnt', 'images_tot'))) / 2.0

            # 评分
            data['rating'] = float(entry['rating'])

            if desc:
                data['desc'] = desc
            elif details:
                data['desc'] = '\n\n'.join(details)
                details = None

            if address:
                data['address'] = address
            if traffic_info:
                data['trafficInfo'] = traffic_info
            if tel:
                data['tel'] = tel
            for tmp in [loc_name, en_name]:
                if not tmp:
                    continue
                alias_list = set(data['alias'])
                alias_list.add(tmp.lower())
                data['alias'] = list(alias_list)
            if details:
                data['details'] = '\n\n'.join(details)

            data['tags'] = []  # list(set(filter(lambda val: val, [tmp.lower().strip() for tmp in entry['tags']])))

            crumb_ids = []
            for crumb_entry in entry['crumb']:
                if isinstance(crumb_entry, int):
                    cid = crumb_entry
                else:
                    cid = int(re.search(r'travel-scenic-spot/mafengwo/(\d+)\.html', crumb_entry['url']).group(1))
                if cid not in crumb_ids:
                    crumb_ids.append(cid)
            data['crumbIds'] = crumb_ids

            data['source'] = {'mafengwo': {'id': entry['id']}}

            if 'lat' in entry and 'lng' in entry:
                data['location'] = {'type': 'Point', 'coordinates': [entry['lng'], entry['lat']]}

            item = MafengwoProcItem()
            item['data'] = data
            item['db_name'] = 'poi'

            if col_name == 'MafengwoVs':
                item['col_name'] = 'MfwViewSpotProc'
            elif col_name == 'MafengwoGw':
                item['col_name'] = 'MfwShoppingProc'
            elif col_name == 'MafengwoHotel':
                item['col_name'] = 'MfwHotelProc'
            elif col_name == 'MafengwoCy':
                item['col_name'] = 'MfwRestaurantProc'
            else:
                return

            # 获得对应的图像
            sig = '%s-%d' % (col_name, data['source']['mafengwo']['id'])
            col_raw_im = self.fetch_db_col('raw_data', 'MafengwoImage', 'mongodb-crawler')
            data['imageList'] = list(col_raw_im.find({'itemIds': sig}))

            self.resolve_targets(item)

            if 'bind-baidu' in self.param:
                yield self.gen_baidu_sug_req(item, 100, True)
            else:
                yield item

    def resolve_targets(self, item):
        data = item['data']

        col_mdd = self.fetch_db_col('geo', 'Locality', 'mongodb-general')
        col_country = self.fetch_db_col('geo', 'Country', 'mongodb-general')

        country_flag = False
        crumb_list = data.pop('crumbIds')
        crumb = []
        for cid in crumb_list:
            ret = col_mdd.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1})
            if not ret and not country_flag:
                ret = col_country.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1, 'code': 1})
                if ret:
                    # 添加到country字段
                    data['country'] = ret
                    for key in ret:
                        data['country'][key] = ret[key]
                    country_flag = True
            if ret:
                crumb.append(ret['_id'])
        data['targets'] = crumb

        # 从crumb的最后开始查找。第一个目的地即为city
        city = None
        for idx in xrange(len(crumb_list) - 1, -1, -1):
            cid = crumb_list[idx]
            ret = col_mdd.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1})
            if ret:
                city = {'_id': ret['_id']}
                for key in ['zhName', 'enName']:
                    if key in ret:
                        city[key] = ret[key]
                break

        if city:
            data['locality'] = city

    def parse_mdd(self, bound):
        col_raw_mdd = self.fetch_db_col('raw_data', 'MafengwoMdd', 'mongodb-crawler')
        tot_num = col_raw_mdd.count()
        col_raw_im = self.fetch_db_col('raw_data', 'MafengwoImage', 'mongodb-crawler')
        col_country = self.fetch_db_col('geo', 'Country', 'mongodb-general')

        query = json.loads(self.param['query'][0]) if 'query' in self.param else {}
        query['type'] = 'region'
        if bound[0] is not None and bound[1] is not None:
            query['$where'] = 'this._id.str.substring(22)>="%s" && this._id.str.substring(22)<="%s"' % (
                bound[0], bound[1])
        cursor = col_raw_mdd.find(query)
        if 'limit' in self.param:
            cursor.limit(int(self.param['limit'][0]))

        self.log('Between %s and %s, %d records to be processed.' % (bound[0], bound[1], cursor.count()), log.INFO)
        for entry in cursor:
            data = {}

            tmp = self.parse_name(entry['title'])
            if not tmp:
                self.log('Failed to get names for id=%d' % entry['id'], log.CRITICAL)
                continue

            data['enName'] = tmp['enName']
            data['zhName'] = tmp['zhName']
            alias = set([])
            # 去除名称中包含国家的
            for a in tmp['alias']:
                c = col_country.find_one({'alias': a}, {'_id': 1})
                if not c:
                    alias.add(a)
            data['alias'] = list(alias)

            desc = None
            travel_month = None
            time_cost = None

            def get_plain(body_list):
                """
                将body_list中的内容，作为纯文本格式输出
                """
                plain_list = []
                for body in body_list:
                    tmp = self.proc_etree(body)
                    if not tmp:
                        continue
                    sel = Selector(text=tmp)
                    plain_list.append('\n'.join(filter(lambda val: val, [tmp.strip() for tmp in sel.xpath(
                        '//p/descendant-or-self::text()').extract()])))
                return '\n\n'.join(plain_list) if plain_list else None

            def get_html(body_list):
                proc_list = []
                for body in body_list:
                    tmp = self.proc_etree(body)
                    if not tmp:
                        continue
                    proc_list.append(tmp)
                if proc_list:
                    return '<div>%s</div>' % '\n'.join(proc_list) if len(proc_list) > 1 else proc_list[0]
                else:
                    return None

            local_traffic = []
            remote_traffic = []
            misc_info = []
            activities = []
            specials = []

            for info_entry in entry['contents']:
                if info_entry['info_cat'] == u'概况' and info_entry['title'] == u'简介':
                    desc = get_plain(info_entry['details'])
                elif info_entry['info_cat'] == u'概况' and info_entry['title'] == u'最佳旅行时间':
                    travel_month = get_plain(info_entry['details'])
                elif info_entry['info_cat'] == u'概况' and info_entry['title'] == u'建议游玩天数':
                    time_cost = get_plain(info_entry['details'])
                elif info_entry['info_cat'] == u'内部交通':
                    tmp = get_html(info_entry['details'])
                    if tmp:
                        local_traffic.append({'title': info_entry['title'], 'desc': tmp})
                elif info_entry['info_cat'] == u'外部交通':
                    tmp = get_html(info_entry['details'])
                    if tmp:
                        remote_traffic.append({'title': info_entry['title'], 'desc': tmp})
                elif info_entry['info_cat'] == u'节庆':
                    tmp = get_html(info_entry['details'])
                    if tmp:
                        activities.append({'title': info_entry['title'], 'desc': tmp})
                elif info_entry['info_cat'] == u'亮点':
                    tmp = get_html(info_entry['details'])
                    if tmp:
                        specials.append({'title': info_entry['title'], 'desc': tmp})
                else:
                    # 忽略出入境信息
                    if info_entry['info_cat'] == u'出入境':
                        continue
                    tmp = get_html(info_entry['details'])
                    if tmp:
                        misc_info.append({'title': info_entry['title'], 'desc': tmp})

            if desc:
                data['desc'] = desc
            if travel_month:
                data['travelMonth'] = travel_month
            if time_cost:
                data['timeCostDesc'] = time_cost
            if local_traffic:
                data['localTraffic'] = local_traffic
            if remote_traffic:
                data['remoteTraffic'] = remote_traffic
            if misc_info:
                data['miscInfo'] = misc_info
            if activities:
                data['activities'] = activities
            if specials:
                data['specials'] = specials

            data['tags'] = list(set(filter(lambda val: val, [tmp.lower().strip() for tmp in entry['tags']])))

            # 热门程度
            if 'comment_cnt' in entry:
                data['commentCnt'] = entry['comment_cnt']
            if 'vs_cnt' in entry:
                data['visitCnt'] = entry['vs_cnt']

            # 计算hotness
            def hotness(key):
                if key not in entry:
                    return 0.5
                return col_raw_mdd.find({key: {'$lt': entry[key]}}).count() / float(tot_num)

            hotness_list = filter(lambda val: val, map(hotness, ('comment_cnt', 'images_tot', 'vs_cnt')))
            data['hotness'] = sum(hotness_list) / float(len(hotness_list)) if hotness_list else 0

            crumb_ids = []
            for crumb_entry in entry['crumb']:
                if isinstance(crumb_entry, int):
                    cid = crumb_entry
                else:
                    cid = int(re.search(r'travel-scenic-spot/mafengwo/(\d+)\.html', crumb_entry['url']).group(1))
                if cid not in crumb_ids:
                    crumb_ids.append(cid)
            data['crumbIds'] = crumb_ids

            data['source'] = {'mafengwo': {'id': entry['id']}}

            if 'lat' in entry and 'lng' in entry:
                data['location'] = {'type': 'Point', 'coordinates': [entry['lng'], entry['lat']]}

            # 获得对应的图像
            sig = 'MafengwoMdd-%d' % data['source']['mafengwo']['id']
            data['imageList'] = list(col_raw_im.find({'itemIds': sig}))

            item = MafengwoProcItem()
            item['data'] = data
            item['col_name'] = 'MfwLocalityProc'
            item['db_name'] = 'raw_data'

            if 'bind-baidu' in self.param:
                yield self.gen_baidu_sug_req(item, 400, False)
            else:
                yield item

    def gen_baidu_sug_req(self, item, proximity, poi):
        data = item['data']
        kw_list = []
        zh_name = data['zhName']
        kw_list.append(utils.get_short_loc(zh_name))
        if zh_name not in kw_list:
            kw_list.append(zh_name)
        for alias in data['alias']:
            if alias not in [tmp.lower() for tmp in kw_list]:
                kw_list.append(alias)
            alias = re.sub(ur'风?景区$', '', alias)
            if alias not in [tmp.lower() for tmp in kw_list]:
                kw_list.append(alias)

        keyword = kw_list[0]
        kw_list = kw_list[1:]
        req = self.baidu_sug_req(keyword, callback=lambda val: self.bind_baidu_scene(val, proximity, poi),
                                 meta={'item': item, 'kw_list': kw_list})
        self.log('Yielding %s for BaiduSugMixin. Remaining: %s' % (keyword, ', '.join(kw_list)), log.DEBUG)

        return req

    def bind_baidu_scene(self, response, proximity, poi):
        item = response.meta['item']
        data = item['data']
        source = data['source']
        lng, lat = data['location']['coordinates']

        if poi:
            ret = filter(lambda val: val['sname'].lower() in data['alias'] and
                                     val['type_code'] >= 6 and
                                     utils.haversine(val['lng'], val['lat'], lng, lat) < proximity,
                         self.parse_baidu_sug(response))
            tmp = filter(lambda val: val['sname'].lower() in data['alias'] and
                                     val['type_code'] == 5 and
                                     utils.haversine(val['lng'], val['lat'], lng, lat) < proximity,
                         self.parse_baidu_sug(response))
            ret.extend(tmp)
            tmp = filter(lambda val: val['sname'].lower() in data['alias'] and
                                     val['type_code'] == 4 and
                                     utils.haversine(val['lng'], val['lat'], lng, lat) < proximity,
                         self.parse_baidu_sug(response))
            ret.extend(tmp)
        else:
            ret = filter(lambda val: val['sname'].lower() in data['alias'] and
                                     utils.haversine(val['lng'], val['lat'], lng, lat) < proximity,
                         self.parse_baidu_sug(response))

        if ret:
            if len(ret) > 1:
                self.log('Duplicates found for: mafengwo_id: %d, url: %s' % (source['mafengwo']['id'],
                                                                             response.url), log.WARNING)
            baidu_mdd = ret[0]

            source['baidu'] = {'surl': ret[0]['surl'], 'id': ret[0]['sid']}
            self.log('Binding: baidu(%s) => mafengwo(%d, %s), surl=%s, sname=%s, parents=%s, type=%d' %
                     (baidu_mdd['sid'], source['mafengwo']['id'], data['zhName'], baidu_mdd['surl'], baidu_mdd['sname'],
                      baidu_mdd['parents'], baidu_mdd['type_code']), log.INFO)
            return item
        else:
            kw_list = response.meta['kw_list']
            if not kw_list:
                self.log(
                    'Baidu counterparts not found: id=%d, name=%s' % (source['mafengwo']['id'], data['zhName']),
                    log.INFO)
                return item

            keyword = kw_list[0]
            kw_list = kw_list[1:]
            req = self.baidu_sug_req(keyword, callback=lambda val: self.bind_baidu_scene(val, proximity, poi),
                                     meta={'item': item, 'kw_list': kw_list})
            self.log('Yielding %s for BaiduSugMixin. Remaining: %s' % (keyword, ', '.join(kw_list)), log.DEBUG)
            return req


class MafengwoProcPipeline(AizouPipeline, ProcImagesMixin):
    """
    蚂蜂窝
    """

    spiders = [MafengwoProcSpider.name]

    spiders_uuid = [MafengwoProcSpider.uuid]

    def __init__(self, param):
        super(MafengwoProcPipeline, self).__init__(param)

        self.def_hot = float(self.param['def-hot'][0]) if 'def-hot' in self.param else 0.3
        self.denom = float(self.param['denom'][0]) if 'denom' in self.param else 2000.0

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        col_name = item['col_name']
        if col_name == 'MfwLocalityProc':
            return self.process_mdd(item, spider)
        elif col_name in ['MfwViewSpotProc', 'MfwHotelProc', 'MfwShoppingProc', 'MfwRestaurantProc']:
            return self.process_poi(item, spider)
        elif col_name == 'MfwCountryProc':
            return self.process_country(item, spider)

    def process_country(self, item, spider):
        data = item['data']

        col = self.fetch_db_col('geo', 'Country', 'mongodb-general')
        ret = col.find_one({'alias': data['zhName']}, {'_id': 1})
        if ret:
            col.update({'_id': ret['_id']}, {'$set': {'source.mafengwo.id': data['id']}})

        return item

    def process_mdd(self, item, spider):
        data = item['data']
        col_name = item['col_name']
        db_name = item['db_name']

        col = self.fetch_db_col(db_name, col_name, 'mongodb-crawler')
        col_mdd = self.fetch_db_col('geo', 'Locality', 'mongodb-general')
        col_country = self.fetch_db_col('geo', 'Country', 'mongodb-general')

        crumb_list = filter(lambda val: val != data['source']['mafengwo']['id'], data.pop('crumbIds'))
        crumb = []
        country_flag = False
        for cid in crumb_list:
            if cid == 21536:
                # 中国需要额外处理
                ret = col_country.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1, 'code': 1})
                data['country'] = {}
                for key in ret:
                    data['country'][key] = ret[key]
                data['abroad'] = False
            else:
                ret = col_mdd.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1})
                if not ret and not country_flag:
                    ret = col_country.find_one({'source.mafengwo.id': cid},
                                               {'_id': 1, 'zhName': 1, 'enName': 1, 'code': 1})
                    if ret:
                        # 添加到country字段
                        data['country'] = {}
                        for key in ret:
                            data['country'][key] = ret[key]
                        country_flag = True

            if ret:
                crumb.append(ret)
                sa_entry = {'_id': ret['_id']}
                for tmp in ['zhName', 'enName']:
                    if tmp in ret:
                        sa_entry[tmp] = ret[tmp]
                data['superAdm'] = sa_entry

        if 'abroad' not in data:
            data['abroad'] = True

        data['locList'] = crumb

        src = data.pop('source')
        alias = data.pop('alias')
        image_list = data.pop('imageList')

        ops = {'$set': data}
        for key in src:
            ops['$set']['source.%s' % key] = src[key]
        ops['$addToSet'] = {'alias': {'$each': alias}}

        mdd = col.find_and_modify({'source.mafengwo.id': src['mafengwo']['id']}, ops, upsert=True, new=True,
                                  fields={'_id': 1, 'isDone': 1})
        images_formal = self.process_image_list(image_list, mdd['_id'])
        if ('isDone' not in mdd or not mdd['isDone']) and images_formal:
            col.update({'_id': mdd['_id']}, {'$set': {'images': images_formal[:10]}})

        return item

    def process_poi(self, item, spider):
        data = item['data']
        col_name = item['col_name']
        db_name = item['db_name']

        col = self.fetch_db_col(db_name, col_name, 'mongodb-crawler')

        src = data.pop('source')
        alias = data.pop('alias')
        image_list = data.pop('imageList')

        ops = {'$set': data}
        for key in src:
            ops['$set']['source.%s' % key] = src[key]
        ops['$addToSet'] = {'alias': {'$each': alias}}

        poi = col.find_and_modify({'source.mafengwo.id': src['mafengwo']['id']}, ops, upsert=True, new=True,
                                  fields={'_id': 1, 'isDone': 1})
        images_formal = self.process_image_list(image_list, poi['_id'])[:10]
        tmp = []
        for img in images_formal:
            tmp.append({key: img[key] for key in ['key', 'w', 'h'] if key in img})
        if ('isDone' not in poi or not poi['isDone']) and images_formal:
            col.update({'_id': poi['_id']}, {'$set': {'images': tmp}})

        return item


class MafengwoNoteSpider(AizouCrawlSpider):
    """
    马蜂窝目的地的抓取
    """

    name = 'mafengwo-note'
    uuid = '08c80cbf-fd94-4d95-9e4f-4745d20e12a9'

    def start_requests(self):
        col_raw = self.fetch_db_col('raw_data', 'MafengwoMdd', 'mongodb-crawler')
        query = json.loads(self.param['query'][0]) if 'query' in self.param else {}
        cursor = col_raw.find(query, {'id': 1})
        if 'limit' in self.param:
            cursor.limit(int(self.param['']))
        for entry in list(cursor):
            mdd_id = entry['id']
            yield Request(url='http://www.mafengwo.cn/yj/%d/1-0-1.html' % mdd_id, meta={'mdd': mdd_id, 'page': 1})

    def parse(self, response):
        mdd_id = response.meta['mdd']
        page_idx = response.meta['page']
        sel = Selector(response)

        def page_populator():
            """
            获得翻页的url
            :return:
            """
            try:
                page_hotel = sel.xpath('//div[@class="page-hotel"]')[0]
                last_href = page_hotel.xpath('./a[@class="ti last" and @href]/@href').extract()[0]
                last_page = int(re.search(r'1-0-(\d+)\.html', last_href).group(1))
                return ['http://www.mafengwo.cn/yj/%d/1-0-%d.html' % (mdd_id, p) for p in xrange(2, last_page + 1)]
            except (IndexError, TypeError):
                return []

        # 如果是第一页，则populate后面的页数
        if page_idx == 1:
            for url in page_populator():
                yield Request(url=url, meta={'mdd': mdd_id, 'page': page_idx})

        for node in sel.xpath('//div[@class="post-list"]/ul/li[contains(@class,"post-item")]'):
            cover = node.xpath('./div[@class="post-cover"]/a[@href]/img[@data-original]/@data-original').extract()[0]
            note_id = int(node.xpath('./div[@class="post-ding"]/a/@data-iid').extract()[0])
            vote_cnt = int(node.xpath('./div[@class="post-ding"]/a/@data-vote').extract()[0])

            title = node.xpath('./h2[contains(@class,"post-title")]/a/text()').extract()[0].strip()

            author_node = node.xpath('./div[@class="post-author"]/span[@class="author"]')[0]
            avatar = author_node.xpath('./a[@href]/img[@data-original]/@data-original').extract()[0]
            href = author_node.xpath('./a[@href]/@href').extract()[0]
            user_id = int(re.search(r'/u/(\d+)\.html', href).group(1))
            tmp = filter(lambda val: val, [tmp.strip() for tmp in author_node.xpath('./a[@href]/text()').extract()])
            nickname = tmp[0] if tmp else ''

            view_cnt = int(
                node.xpath('./span[@class="status"]/i[@class="icon_view"]/following-sibling::text()').extract()[0])
            comment_cnt = int(
                node.xpath('./span[@class="status"]/i[@class="icon_comment"]/following-sibling::text()').extract()[0])

            data = {'cover': cover, 'note_id': note_id, 'vote_cnt': vote_cnt, 'title': title, 'author_avatar': avatar,
                    'author_id': user_id, 'author_name': nickname, 'view_cnt': view_cnt, 'comment_cnt': comment_cnt,
                    'mdd': mdd_id}

            yield Request(url='http://www.mafengwo.cn/group/info.php?iid=%d&page=1' % note_id, callback=self.parse_note,
                          meta={'data': data})

    def parse_note(self, response):
        data = copy.deepcopy(response.meta['data'])

        sel = Selector(response)

        tmp = sel.xpath('//div[@class="vc_article"]')
        if tmp:
            article = tmp[0]
            data['note_type'] = 2
            data['contents'] = article.extract()
            data['post_id'] = data['note_id']
            item = MafengwoItem()
            item['data'] = data

            yield Request(
                url='http://www.mafengwo.cn/group/ajax_ginfo_ready.php?sAction=initGinfo&iId=%d' % data['mdd'],
                callback=self.parse_stat, meta={'item': item}, cookies={
                    'mafengwo': '9b148eb8ba78dd3aebe5826f70df326c_67590906_549ba2f7ef2cf3.'
                                '39042540_549ba2f7ef2e09.39878707'})
        else:
            data['type'] = 1

            for href in sel.xpath('//div[@class="paginator"]/a[@class="ti" and @href]/@href').extract():
                new_data = copy.deepcopy(data)
                yield Request(url=self.build_href(response.url, href), callback=self.parse_note,
                              meta={'data': new_data})

            for post_item in sel.xpath('//div[contains(@class,"post_main")]/div[@class="post_item"]'):
                tmp = post_item.xpath('./div[@class="post_info"]/@id').extract()
                if not tmp:
                    new_data = copy.deepcopy(data)
                    new_data['main_post'] = True
                    new_data['post_id'] = 'main-%d' % new_data['note_id']
                else:
                    new_data = {'main_post': False, 'note_id': data['note_id'], 'title': data['title'],
                                'post_id': int(re.search(r'reply_(\d+)', tmp[0]).group(1))}

                user_node = \
                    post_item.xpath('./div[@class="author_info"]/div[@class="avatar_box"]/div[@class="out_pic"]')[0]
                user_id = int(re.search(r'/u/(\d+)\.html', user_node.xpath('./a[@href]/@href').extract()[0]).group(1))
                avatar = user_node.xpath('./a[@href]/img[@src and @title]/@src').extract()[0]
                user_name = user_node.xpath('./a[@href]/img[@src and @title]/@title').extract()[0].strip()
                new_data['user_id'] = user_id
                new_data['user_avatar'] = avatar
                new_data['user_name'] = user_name

                new_data['contents'] = post_item.extract()
                item = MafengwoItem()
                item['data'] = new_data

                if new_data['main_post']:
                    yield Request(url='http://www.mafengwo.cn/group/ajax_ginfo_ready.php?sAction=initGinfo&iId=%d' %
                                      new_data['note_id'], callback=self.parse_stat, meta={'item': item},
                                  cookies={
                                      'mafengwo': '9b148eb8ba78dd3aebe5826f70df326c_67590906_549ba2f7ef2cf3.'
                                                  '39042540_549ba2f7ef2e09.39878707'})
                else:
                    yield item

    @staticmethod
    def parse_stat(response):
        item = response.meta['item']
        data = item['data']
        payload = json.loads(response.body)['payload']
        tmp = payload['share_pv']
        data['share_cnt'] = int(tmp) if tmp else 0
        tmp = payload['fav_total']
        data['favor_cnt'] = int(tmp) if tmp else 0
        yield item


class MafengwoNotePipeline(AizouPipeline):
    """
    蚂蜂窝
    """

    spiders = [MafengwoNoteSpider.name]

    spiders_uuid = [MafengwoNoteSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        col = self.fetch_db_col('raw_data', 'MafengwoNote', 'mongodb-crawler')
        data = item['data']
        col.update({'post_id': data['post_id']}, {'$set': data}, upsert=True)

        return item