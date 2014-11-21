# coding=utf-8
import json
import re
import math

from scrapy import Item, Request, Field, Selector, log

from spiders import AizouCrawlSpider, AizouPipeline
import utils


__author__ = 'zephyre'


class MafengwoItem(Item):
    data = Field()


class MafengwoSpider(AizouCrawlSpider):
    """
    马蜂窝目的地的抓取
    """

    name = 'mafengwo-mdd'
    uuid = '74f9b075-65f3-400d-b093-5bdbdb552e86'

    def __init__(self, *a, **kw):
        super(MafengwoSpider, self).__init__(*a, **kw)
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
        self.param = getattr(self, 'param', {})

        # 大洲的过滤
        if 'cont' in self.param:
            cont_filter = [int(tmp) for tmp in self.param['cont']]
        else:
            cont_filter = None

        for cont_id in self.cont_list:
            if cont_filter and cont_id not in cont_filter:
                continue
            yield Request(url='http://www.mafengwo.cn/jd/%d/' % cont_id)

    def get_region_list(self, response):
        sel = Selector(response)
        self_id = response.meta['id']
        ctype = response.meta['type']
        for node in sel.xpath('//dd[@id="region_list"]/a[@href]'):
            url = self.build_href(response.url, node.xpath('./@href').extract()[0])
            mdd_id = int(re.search(r'mafengwo\.cn/jd/(\d+)', url).group(1))
            if mdd_id != self_id:
                url = 'http://www.mafengwo.cn/travel-scenic-spot/mafengwo/%d.html' % mdd_id
                yield Request(url=url, callback=self.parse_mdd_home, meta={'type': ctype, 'id': mdd_id})

    def parse(self, response):
        self.param = getattr(self, 'param', {})

        # 地区的过滤
        if 'region' in self.param:
            region_list = [int(tmp) for tmp in self.param['region']]
        else:
            region_list = None

        for node in Selector(response).xpath('//dd[@id="region_list"]/a[@href]'):
            url = self.build_href(response.url, node.xpath('./@href').extract()[0])
            mdd_id = int(re.search(r'mafengwo\.cn/jd/(\d+)', url).group(1))
            if region_list and mdd_id not in region_list:
                continue

            url = 'http://www.mafengwo.cn/travel-scenic-spot/mafengwo/%d.html' % mdd_id
            yield Request(url=url, callback=self.parse_mdd_home, meta={'id': mdd_id})

    def parse_mdd_home(self, response):
        item = MafengwoItem()
        data = {'id': response.meta['id']}
        item['data'] = data

        crumb = self.get_crumb(response)
        data['crumb'] = crumb
        # 抓取导航栏中的目的地
        for crumb_url in [tmp['url'] for tmp in crumb]:
            mdd_id = int(re.search(r'travel-scenic-spot/mafengwo/(\d+).html', crumb_url).group(1))
            yield Request(url=crumb_url, callback=self.parse_mdd_home, meta={'id': mdd_id})

        sel = Selector(response)
        for node1 in sel.xpath('//div[contains(@class,"m-tags")]/div[@class="bd"]/div[@class="t-info"]/p'):
            span_list = node1.xpath('./span/text()').extract()
            if len(span_list) < 2 and len(span_list) > 0:
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

        loc_text = re.search(r'var\s+mdd_center[^;]+;', response.body).group()
        data['lat'] = float(re.search(r"lat:parseFloat\(\s*'([^']+)'", loc_text).group(1))
        data['lng'] = float(re.search(r"lng:parseFloat\(\s*'([^']+)'", loc_text).group(1))

        # 访问次数
        digits = [int(tmp) for tmp in sel.xpath(
            '//div[@class="num-been"]/div[@class="num-count"]/em[@data-number and @class="_j_rollnumber"]/@data-number').extract()]
        visit_cnt = 0
        for idx, d in enumerate(digits):
            visit_cnt += math.pow(10, len(digits) - 1 - idx) * d
        data['vs_cnt'] = int(visit_cnt)

        tags = sel.xpath(
            '//div[contains(@class,"m-tags")]/div[@class="bd"]/ul/li[@class="impress-tip"]/a[@href]/text()').extract()
        data['tags'] = list(set(filter(lambda val: val, [tmp.strip() for tmp in tags])))
        data['images_tot'] = int(sel.xpath('//div[@class="m-photo"]/a/em/text()').extract()[0])

        url = 'http://www.mafengwo.cn/jd/%d/gonglve.html' % data['id']
        yield Request(url=url, callback=self.parse_jd, meta={'item': item, 'id': data['id'], 'type': 'region'})

    def parse_poi_list(self, response):
        """
        解析页面内的poi列表
        :param response:
        :return:
        """
        poi_type = response.meta['poi_type']
        for href in Selector(response).xpath(
                '//ul[@class="poi-list"]/li[contains(@class,"item")]/div[@class="title"]//a[@href]/@href').extract():
            yield Request(self.build_href(response.url, href), meta={'poi_type': poi_type}, callback=self.parse_poi)

    def get_crumb(self, response):
        # 获得crumb
        crumb = []
        for node in Selector(response).xpath(
                '//div[@class="crumb"]/div[contains(@class,"item")]/div[@class="drop"]/span[@class="hd"]/a[@href]'):
            crumb_name = node.xpath('./text()').extract()[0].strip()
            crumb_url = self.build_href(response.url, node.xpath('./@href').extract()[0])
            match = re.search(r'travel-scenic-spot/mafengwo/(\d+)\.html', crumb_url)
            if not match:
                continue
            mdd_id = int(match.group(1))
            # 目标为洲的那些scrumb，不要抓取
            if mdd_id in self.cont_list:
                continue
            crumb.append({'name': crumb_name, 'url': crumb_url})
        return crumb

    def parse_jd(self, response):
        sel = Selector(response)

        ctype = response.meta['type']
        # 如果是目的地页面，其下的所有poi都为景点类型。否则，poi_type和ctype一致，比如都为gw, cy等
        poi_type = 'vs' if ctype == 'region' else ctype
        response.meta['poi_type'] = poi_type

        # 抓取景点页面中的
        results = self.parse_poi_list(response)
        if results:
            for entry in results:
                yield entry

        # poi列表的翻页
        for href in sel.xpath('//div[@class="page-hotel"]/a[@href]/@href').extract():
            yield Request(self.build_href(response.url, href), callback=self.parse_poi_list,
                          meta={'poi_type': poi_type})

        if ctype == 'region':
            # 抓取下级的region
            region_list = self.get_region_list(response)
            if region_list:
                for entry in region_list:
                    yield entry

            # 处理当前region
            item = response.meta['item']
            data = item['data']

            col_list = []
            for node in sel.xpath('//ul[@class="nav-box"]/li[contains(@class,"nav-item")]/a[@href]'):
                info_title = node.xpath('./text()').extract()[0]
                next_url = self.build_href(response.url, node.xpath('./@href').extract()[0])
                # 根据是否出现国家攻略来判断是否为国家
                if info_title == u'国家概况' or info_title == u'城市概况':
                    for info_node in node.xpath('..//dl/dt/a[@href]'):
                        info_url = self.build_href(response.url, info_node.xpath('./@href').extract()[0])
                        info_cat = info_node.xpath('./text()').extract()[0].strip()
                        col_list.append([info_url, self.parse_info, {'info_cat': info_cat}])

                    if info_title == u'国家概况':
                        data['type'] = 'country'
                    else:
                        data['type'] = 'region'
                elif info_title == u'购物':
                    yield Request(url=next_url, callback=self.parse_jd, meta={'type': 'gw'})
                elif info_title == u'娱乐':
                    yield Request(url=next_url, callback=self.parse_jd, meta={'type': 'yl'})
                elif info_title == u'美食':
                    yield Request(url=next_url, callback=self.parse_jd, meta={'type': 'cy'})

            if col_list:
                col_url, cb, meta = col_list[0]
                col_list = col_list[1:]
                meta['col_list'] = col_list
                meta['item'] = item
                yield Request(url=col_url, callback=cb, meta=meta)

    def parse_info(self, response):
        sel = Selector(response)
        item = response.meta['item']
        info_cat = response.meta['info_cat']
        data = item['data']

        if 'title' not in data:
            tmp = sel.xpath(
                '//div[contains(@class,"sub-nav")]/div[@class="mdd-title"]/span[@class="s-name"]/text()').extract()[
                0].strip()
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
        crumb = self.get_crumb(response)
        for crumb_url in [tmp['url'] for tmp in crumb]:
            mdd_id = int(re.search(r'travel-scenic-spot/mafengwo/(\d+).html', crumb_url).group(1))
            yield Request(url=crumb_url, callback=self.parse_mdd_home, meta={'type': 'region', 'id': mdd_id})

        title = sel.xpath('//div[contains(@class,"m-details")]/div[contains(@class,"title")]//h1/text()').extract()[
            0].strip()

        pid = int(re.search('mafengwo\.cn/poi/(\d+)\.html', response.url).group(1))

        loc_data = json.loads('{%s}' % re.search(r'window\.Env\s*=\s*\{(.+?)\}\s*;', response.body).group(1))
        lat = loc_data['lat']
        lng = loc_data['lng']

        score = float(
            sel.xpath('//div[@class="txt-l"]/div[@class="score"]/span[@class="score-info"]/em/text()').extract()[0]) / 5
        comment_cnt = int(
            sel.xpath('//div[@class="txt-l"]/div[@class="score"]/p[@class="ranking"]/em/text()').extract()[0])
        tmp = sel.xpath('//div[@class="pic-r"]/a[@href]/span[@class="pic-num"]/text()').extract()[0]
        photo_cnt = int(re.search(ur'共(\d+)张', tmp).group(1))

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

        data = {'id': pid, 'type': response.meta['poi_type'], 'title': title, 'crumb': crumb, 'rating': score,
                'comment_cnt': comment_cnt, 'images_tot': photo_cnt, 'desc': desc, 'lat': lat, 'lng': lng}
        item = MafengwoItem()
        item['data'] = data

        url = 'http://www.mafengwo.cn/mdd/ajax_photolist.php?act=getPoiPhotoList&poiid=%d&page=1' % pid
        yield Request(url=url, meta={'item': item, 'page': 1, 'act': 'getPoiPhotoList'}, callback=self.parse_photo,
                      errback=self.photo_err)

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
        else:
            return item

        col = self.fetch_db_col('raw_data', col_name, 'mongodb-crawler')
        db_data = col.find_one({'id': data['id']})
        if not db_data:
            db_data = {}

        if 'imageList' not in db_data:
            db_data['imageList'] = []
        images_set = set([tmp['url'] for tmp in db_data['imageList']])
        for key in data.keys():
            if key == 'imageList':
                for image_entry in data[key]:
                    if image_entry['url'] not in images_set:
                        images_set.add(image_entry['url'])
                        db_data['imageList'].append(image_entry)
            else:
                db_data[key] = data[key]
        col.save(db_data)
        return item


class MafengwoProcItem(Item):
    data = Field()
    col_name = Field()
    db_name = Field()


class MafengwoProcSpider(AizouCrawlSpider):
    """
    马蜂窝目的地的清洗
    """

    name = 'mafengwo-mdd-proc'
    uuid = '69d64c68-7602-4cb1-a319-1da2853cda67'

    def __init__(self, param, *a, **kw):
        super(MafengwoProcSpider, self).__init__(param, *a, **kw)
        self.col_dict = {}

    def start_requests(self):
        yield Request(url='http://www.baidu.com')

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
            if node.tag == 'a' and node.get('href') and 'mafengwo' in node.get('href'):
                return True, parse_etree(node[0], [f1, f2, f3])
            else:
                return False, node

        def f3(node):
            # 去掉不必要的class
            if node.get('class') and ('m-txt' in node.get('class') or 'm-img' in node.get('class')):
                del node.attrib['class']
            return False, node

        from utils import parse_etree
        from lxml import etree

        node = parse_etree(body, [f1, f2, f3])
        return etree.tostring(node, encoding='utf-8').decode('utf-8')

    def parse_name(self, name):
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
        for name in name_list:
            if not zh_name and self.is_chn(name):
                zh_name = name
            elif not en_name and self.is_eng(name):
                en_name = name
            elif not loc_name:
                loc_name = name

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

        alias = set([])
        for tmp in name_list:
            alias.add(tmp.lower())
        result['alias'] = list(alias)
        return result

    def parse(self, response):
        func_map = {'mdd': self.parse_mdd,
                    'country': self.parse_country,
                    'vs': self.parse_vs}

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
        col = utils.get_mongodb('raw_data', 'MafengwoCountry', profile='mongodb-crawler')

        for entry in col.find({}, {'id': 1, 'title': 1}):
            item = MafengwoProcItem()
            item['data'] = {'zhName': entry['title'], 'id': entry['id']}
            item['db_name'] = 'geo'
            item['col_name'] = 'Country'

            yield item

    def parse_vs(self):
        col_raw = utils.get_mongodb('raw_data', 'MafengwoVs', profile='mongodb-crawler')

        for entry in col_raw.find({}):
            data = {'abroad': True, 'enabled': True}

            tmp = self.parse_name(entry['title'])
            if not tmp:
                self.log('Failed to get names for id=%d' % entry['id'], log.CRITICAL)
                continue

            data['enName'] = tmp['enName']
            data['zhName'] = tmp['zhName']
            data['alias'] = tmp['alias']

            desc = None
            address = None
            traffic_info = None
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
                else:
                    details.append(u'%s：%s' % (info_entry['name'], proc_text))

            if desc:
                data['desc'] = desc
            elif details:
                data['desc'] = '\n\n'.join(details)
                details = None

            if address:
                data['address'] = address
            if traffic_info:
                data['trafficInfo'] = traffic_info
            if details:
                data['details'] = '\n\n'.join(details)

            data['tags'] = []  # list(set(filter(lambda val: val, [tmp.lower().strip() for tmp in entry['tags']])))

            image_list = []
            image_urls = set([])
            for img in entry['imageList']:
                url = img['url']
                if url in image_urls:
                    continue
                image_list.append({'url': url})
                image_urls.add(url)
            data['imageList'] = image_list

            crumb_ids = []
            for crumb_entry in entry['crumb']:
                cid = int(re.search(r'travel-scenic-spot/mafengwo/(\d+)\.html', crumb_entry['url']).group(1))
                if cid not in crumb_ids:
                    crumb_ids.append(cid)
            data['crumbIds'] = crumb_ids

            data['source'] = {'mafengwo':
                                  {'url': 'http://www.mafengwo.cn/poi/%d.html' % entry['id'],
                                   'id': entry['id']}}

            if 'lat' in entry and 'lng' in entry:
                data['location'] = {'type': 'Point', 'coordinates': [entry['lng'], entry['lat']]}

            item = MafengwoProcItem()
            item['data'] = data
            item['col_name'] = 'ViewSpot'
            item['db_name'] = 'poi'
            yield item

    def parse_mdd(self):
        col_name = 'MafengwoMdd'
        if col_name not in self.col_dict:
            self.col_dict[col_name] = utils.get_mongodb('raw_data', col_name, profile='mongodb-crawler')
        col_raw_mdd = self.col_dict[col_name]

        col_name = 'Country'
        if col_name not in self.col_dict:
            self.col_dict[col_name] = utils.get_mongodb('geo', col_name, profile='mongodb-general')
        col_country = self.col_dict[col_name]

        for entry in col_raw_mdd.find({'type': 'region'}):
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
                        local_traffic.append({'title': info_entry['title'], 'contents': tmp})
                elif info_entry['info_cat'] == u'外部交通':
                    tmp = get_html(info_entry['details'])
                    if tmp:
                        remote_traffic.append({'title': info_entry['title'], 'contents': tmp})
                else:
                    # 忽略出入境信息
                    if info_entry['info_cat'] == u'出入境':
                        continue
                    tmp = get_html(info_entry['details'])
                    if tmp:
                        misc_info.append({'title': info_entry['title'], 'contents': tmp})

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

            data['tags'] = list(set(filter(lambda val: val, [tmp.lower().strip() for tmp in entry['tags']])))

            if 'vs_cnt' in entry and entry['vs_cnt'] is not None:
                data['visitCnt'] = entry['vs_cnt']

            image_list = []
            image_urls = set([])
            for img in entry['imageList']:
                url = img['url']
                if url in image_urls:
                    continue
                image_list.append({'url': url})
                image_urls.add(url)
            data['imageList'] = image_list

            crumb_ids = []
            for crumb_entry in entry['crumb']:
                cid = int(re.search(r'travel-scenic-spot/mafengwo/(\d+)\.html', crumb_entry['url']).group(1))
                if cid not in crumb_ids:
                    crumb_ids.append(cid)
            data['crumbIds'] = crumb_ids

            data['source'] = {'mafengwo':
                                  {'url': 'http://www.mafengwo.cn/travel-scenic-spot/mafengwo/%d.html' % entry['id'],
                                   'id': entry['id']}}

            if 'lat' in entry and 'lng' in entry:
                data['location'] = {'type': 'Point', 'coordinates': [entry['lng'], entry['lat']]}

            item = MafengwoProcItem()
            item['data'] = data
            item['col_name'] = 'Destination'
            item['db_name'] = 'geo'

            # 尝试通过geocode获得目的地别名及其它信息
            addr = u''
            for idx in xrange(len(entry['crumb']) - 1, -1, -1):
                addr += u'%s,' % (entry['crumb'][idx]['name'])
            idx = addr.rfind(',')
            addr = addr[:idx] if idx > 0 else addr

            if 'skip-geocode' not in self.param:
                if addr and 'location' in data:
                    lang = ['en-US']
                    yield Request(
                        url=u'http://maps.googleapis.com/maps/api/geocode/json?address=%s&sensor=false' % addr,
                        headers={'Accept-Language': 'zh-CN'},
                        meta={'item': item, 'lang': lang},
                        callback=self.parse_geocode)
                else:
                    yield item
            else:
                yield item


class MafengwoProcPipeline(AizouPipeline):
    """
    蚂蜂窝
    """

    spiders = [MafengwoProcSpider.name]

    spiders_uuid = [MafengwoProcSpider.uuid]

    def __init__(self, param):
        super(MafengwoProcPipeline, self).__init__(param)

        self.def_hot = float(self.param['def-hot'][0]) if 'def-hot' in self.param else 0.3
        self.denom = float(self.param['denom'][0]) if 'denom' in self.param else 1000.0

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        col_name = item['col_name']
        if col_name == 'Destination' or col_name == 'Locality':
            return self.process_mdd(item, spider)
        elif col_name == 'ViewSpot':
            return self.process_vs(item, spider)
        elif col_name == 'Country':
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

        col = self.fetch_db_col(db_name, col_name, 'mongodb-general')
        col_mdd = self.fetch_db_col('geo', 'Destination', 'mongodb-general')
        col_country = self.fetch_db_col('geo', 'Country', 'mongodb-general')

        entry = col.find_one({'source.mafengwo.id': data['source']['mafengwo']['id']})
        if not entry:
            entry = {}

        crumb_list = data.pop('crumbIds')
        crumb = []
        super_adm = []
        for cid in crumb_list:
            ret = col_mdd.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1})
            if not ret:
                ret = col_country.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1, 'code': 1})
                if ret:
                    # 添加到country字段
                    data['country'] = {}
                    for key in ret:
                        data['country'][key] = ret[key]
            if ret:
                crumb.append(ret)
                sa_entry = {'id': ret['_id']}
                for tmp in ['zhName', 'enName']:
                    if tmp in ret:
                        sa_entry[tmp] = ret[tmp]
                super_adm.append(sa_entry)

        data['locList'] = crumb
        # data['superAdm'] = super_adm

        # 有几个字段具有天然的追加属性：alias, imageList
        # 其它都是覆盖型
        image_set = set([tmp['url'] for tmp in entry['imageList']]) if 'imageList' in entry else set([])
        alias_set = set(entry['alias']) if 'alias' in entry else set([])
        for k in data:
            if k == 'imageList':
                if k not in entry:
                    entry[k] = []
                for tmp in data[k]:
                    if tmp['url'] in image_set:
                        continue
                    entry[k].append(tmp)
                    image_set.add(tmp['url'])

            elif k == 'alias':
                if k not in entry:
                    entry[k] = []
                for tmp in data[k]:
                    if tmp in alias_set:
                        continue
                    entry[k].append(tmp)
                    alias_set.add(tmp)
            else:
                entry[k] = data[k]

        # 将visitCnt转换成hotness信息
        if 'visitCnt' in entry:
            entry['hotness'] = 1 - math.exp(-entry['visitCnt'] / self.denom)
        else:
            entry['hotness'] = self.def_hot

        col.save(entry)

        return item

    def process_vs(self, item, spider):
        data = item['data']
        col_name = item['col_name']
        db_name = item['db_name']

        col = self.fetch_db_col(db_name, col_name, 'mongodb-general')
        col_mdd = self.fetch_db_col('geo', 'Destination', 'mongodb-general')

        entry = col.find_one({'source.mafengwo.id': data['source']['mafengwo']['id']})
        if not entry:
            entry = {}

        crumb_list = data.pop('crumbIds')
        crumb = []
        for cid in crumb_list:
            ret = col_mdd.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1})
            if ret:
                crumb.append(ret['_id'])
        entry['targets'] = crumb
        entry['name'] = data['zhName']

        # 从crumb的最后开始查找。第一个目的地即为city
        city = None
        for idx in xrange(len(crumb_list) - 1, -1, -1):
            cid = crumb_list[idx]
            ret = col_mdd.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1})
            if ret:
                city = {'id': ret['_id'], '_id': ret['_id']}
                for key in ['zhName', 'enName']:
                    if key in ret:
                        city[key] = ret[key]
        if city:
            entry['city'] = city

        for k in data:
            entry[k] = data[k]

        entry['className'] = 'models.poi.ViewSpot'
        col.save(entry)

        return item
