# coding=utf-8
import json
import re
from bson import ObjectId

from scrapy import Item, Request, Field, Selector, log

from spiders import AizouCrawlSpider
import utils


__author__ = 'zephyre'


class MafengwoItem(Item):
    data = Field()


class MafengwoSpider(AizouCrawlSpider):
    """
    马蜂窝目的地的抓取
    """

    name = 'mafengwo-mdd'

    def __init__(self, *a, **kw):
        super(MafengwoSpider, self).__init__(*a, **kw)

    def start_requests(self):
        urls = [
            'http://www.mafengwo.cn/jd/52314/gonglve.html',  # 亚洲
            'http://www.mafengwo.cn/jd/10853/',  # 南极州
            'http://www.mafengwo.cn/jd/14701/',  # 大洋洲
            'http://www.mafengwo.cn/jd/14517/',  # 非洲
            'http://www.mafengwo.cn/jd/14383/',  # 欧洲
            'http://www.mafengwo.cn/jd/16406/',  # 南美
            'http://www.mafengwo.cn/jd/16867/',  # 北美
        ]
        return [Request(url=url) for url in urls]

    def parse(self, response):
        self.param = getattr(self, 'param', {})

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

        tags = sel.xpath(
            '//div[contains(@class,"m-tags")]/div[@class="bd"]/ul/li[@class="impress-tip"]/a[@href]/text()').extract()
        data['tags'] = list(set(filter(lambda val: val, [tmp.strip() for tmp in tags])))
        data['images_tot'] = int(sel.xpath('//div[@class="m-photo"]/a/em/text()').extract()[0])

        url = 'http://www.mafengwo.cn/jd/%d/gonglve.html' % data['id']
        yield Request(url=url, callback=self.parse_jd, meta={'item': item, 'type': 'region'})

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
            if not re.search(r'travel-scenic-spot/mafengwo/\d+\.html', crumb_url):
                continue
            crumb.append({'name': crumb_name, 'url': crumb_url})
        return crumb

    def parse_jd(self, response):
        sel = Selector(response)

        ctype = response.meta['type']
        # 如果是目的地页面，其下的所有poi都为景点类型。否则，poi_type和ctype一致，比如都为gw, cy等
        poi_type = 'vs' if ctype == 'region' else ctype
        response.meta['poi_type'] = poi_type

        # 继续抓取下级的region
        if ctype == 'region':
            for node in sel.xpath('//dd[@id="region_list"]/a[@href]'):
                url = self.build_href(response.url, node.xpath('./@href').extract()[0])
                mdd_id = int(re.search(r'mafengwo\.cn/jd/(\d+)', url).group(1))
                url = 'http://www.mafengwo.cn/travel-scenic-spot/mafengwo/%d.html' % mdd_id
                yield Request(url=url, callback=self.parse_mdd_home, meta={'type': ctype, 'id': mdd_id})

        results = self.parse_poi_list(response)
        if hasattr(results, '__iter__'):
            for entry in results:
                yield entry
        elif isinstance(results, Request):
            yield results

        # poi列表的翻页
        for href in sel.xpath('//div[@class="page-hotel"]/a[@href]/@href').extract():
            yield Request(self.build_href(response.url, href), callback=self.parse_poi_list,
                          meta={'poi_type': poi_type})

        if ctype == 'region':
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
                    elif info_title == u'城市概况':
                        data['type'] = 'region'
                    else:
                        self.log(u'Invalid MDD type: %s, %s' % (info_title, response.url), log.WARNING)
                        return

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


class MafengwoPipeline(object):
    spiders = [MafengwoSpider.name]

    def process_item(self, item, spider):
        data = item['data']
        item_type = data['type']

        col_name = None
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

        col = utils.get_mongodb('raw_data', col_name, profile='mongodb-crawler')
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

    def __init__(self, *a, **kw):
        self.param = {}
        super(MafengwoProcSpider, self).__init__(*a, **kw)

    def start_requests(self):
        self.param = getattr(self, 'param', {})
        yield Request(url='http://www.baidu.com')

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
            tmp = filter(lambda val: val, [tmp.strip() for tmp in re.split(r'/', term)])
            if not tmp:
                continue
            name_list.extend(tmp)
        if not name_list:
            return

        result = {'zhName': name_list[0], 'enName': name_list[0]}
        alias = set([])
        for tmp in name_list:
            alias.add(tmp.lower())
        result['alias'] = list(alias)
        return result

    def parse(self, response):
        mdd = 'mdd' in self.param
        vs = 'vs' in self.param

        if mdd:
            for entry in self.parse_loc():
                yield entry

        if vs:
            for entry in self.parse_vs():
                yield entry

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
        col_raw_mdd = utils.get_mongodb('raw_data', 'MafengwoMdd', profile='mongodb-crawler')

        for entry in col_raw_mdd.find({'type': 'region'}):
            data = {}

            tmp = self.parse_name(entry['title'])
            if not tmp:
                self.log('Failed to get names for id=%d' % entry['id'], log.CRITICAL)
                continue

            data['enName'] = tmp['enName']
            data['zhName'] = tmp['zhName']
            data['alias'] = tmp['alias']

            desc = None
            travel_month = None
            time_cost = None
            details = []
            traffic_info = []

            for info_entry in entry['contents']:
                # raw_text = Selector(text=info_entry['details']).xpath('//p/descendant-or-self::text()').extract()
                # proc_text = '\n'.join(filter(lambda val: val, [tmp.strip() for tmp in raw_text]))

                txt_list = []
                if 'details' not in info_entry and 'txt' in info_entry:
                    info_entry['details'] = [info_entry['txt']]
                for txt_entry in info_entry['details']:
                    txt_list.append(
                        '\n'.join(filter(lambda val: val, [tmp.strip() for tmp in Selector(text=txt_entry).xpath(
                            '//p/descendant-or-self::text()').extract()])))
                proc_text = '\n\n'.join(filter(lambda val: val, [tmp.strip() for tmp in txt_list]))

                # proc_text = '\n\n'.join(filter(lambda val: val, [tmp.strip() for tmp in [
                # Selector(text=txt).xpath('//p/descendant-or-self::text()').extract() for txt in
                # info_entry['details']]]))

                if info_entry['title'] == u'简介':
                    desc = proc_text
                elif info_entry['title'] == u'最佳旅行时间':
                    travel_month = proc_text
                elif info_entry['title'] == u'建议游玩天数':
                    time_cost = proc_text
                elif info_entry['info_cat'] == u'外部交通':
                    traffic_info.append(u'%s：%s' % (info_entry['title'], proc_text))
                else:
                    details.append(u'%s：%s' % (info_entry['title'], proc_text))

            if desc:
                data['desc'] = desc
            elif details:
                data['desc'] = '\n\n'.join(details)
                details = None
            if travel_month:
                data['travelMonth'] = travel_month
            if time_cost:
                data['timeCostDesc'] = time_cost
            if details:
                data['details'] = '\n\n'.join(details)
            if traffic_info:
                data['trafficInfo'] = '\n\n'.join(traffic_info)

            data['tags'] = list(set(filter(lambda val: val, [tmp.lower().strip() for tmp in entry['tags']])))

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
            yield item

    def parse_loc(self):
        col_raw_mdd = utils.get_mongodb('raw_data', 'MafengwoMdd', profile='mongodb-crawler')

        for entry in col_raw_mdd.find({'type': 'region'}):
            data = {}

            tmp = self.parse_name(entry['title'])
            if not tmp:
                self.log('Failed to get names for id=%d' % entry['id'], log.CRITICAL)
                continue

            data['enabled'] = True
            data['enName'] = tmp['enName']
            data['zhName'] = tmp['zhName']
            data['shortName'] = tmp['zhName']
            data['alias'] = tmp['alias']
            data['pinyin'] = []
            data['travelMonth'] = []
            data['level'] = 2
            data['rating'] = entry['rating'] if 'rating' in entry else 0
            data['sib'] = []
            data['abroad'] = True

            desc = None
            travel_month = None
            time_cost = None
            details = []
            traffic_info = []

            for info_entry in entry['contents']:
                # raw_text = Selector(text=info_entry['details']).xpath('//p/descendant-or-self::text()').extract()
                # proc_text = '\n'.join(filter(lambda val: val, [tmp.strip() for tmp in raw_text]))

                txt_list = []
                if 'details' not in info_entry and 'txt' in info_entry:
                    info_entry['details'] = [info_entry['txt']]
                for txt_entry in info_entry['details']:
                    txt_list.append(
                        '\n'.join(filter(lambda val: val, [tmp.strip() for tmp in Selector(text=txt_entry).xpath(
                            '//p/descendant-or-self::text()').extract()])))
                proc_text = '\n\n'.join(filter(lambda val: val, [tmp.strip() for tmp in txt_list]))

                # proc_text = '\n\n'.join(filter(lambda val: val, [tmp.strip() for tmp in [
                # Selector(text=txt).xpath('//p/descendant-or-self::text()').extract() for txt in
                # info_entry['details']]]))

                if info_entry['title'] == u'简介':
                    desc = proc_text
                elif info_entry['title'] == u'最佳旅行时间':
                    travel_month = proc_text
                elif info_entry['title'] == u'建议游玩天数':
                    time_cost = proc_text
                elif info_entry['info_cat'] == u'外部交通':
                    traffic_info.append(u'%s：%s' % (info_entry['title'], proc_text))
                else:
                    details.append(u'%s：%s' % (info_entry['title'], proc_text))

            if desc:
                data['desc'] = desc
            elif details:
                data['desc'] = '\n\n'.join(details)
                details = None
            # if travel_month:
            # data['travelMonth'] = travel_month
            if time_cost:
                data['timeCostDesc'] = time_cost
            if details:
                data['details'] = '\n\n'.join(details)
            if traffic_info:
                data['trafficInfo'] = '\n\n'.join(traffic_info)

            data['tags'] = list(set(filter(lambda val: val, [tmp.lower().strip() for tmp in entry['tags']])))

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
            item['col_name'] = 'Locality'
            item['db_name'] = 'geo'
            yield item


class MafengwoProcPipeline(object):
    """
    蚂蜂窝
    """

    spiders = [MafengwoProcSpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != MafengwoProcItem.__name__:
            return item

        col_name = item['col_name']
        if col_name == 'Destination' or col_name == 'Locality':
            return self.process_mdd(item, spider)
        elif col_name == 'ViewSpot':
            return self.process_vs(item, spider)

    def process_mdd(self, item, spider):
        data = item['data']
        col_name = item['col_name']
        db_name = item['db_name']
        col = utils.get_mongodb(db_name, col_name, profile='mongodb-general')
        col_mdd = utils.get_mongodb('geo', 'Destination', profile='mongodb-general')

        entry = col.find_one({'source.mafengwo.id': data['source']['mafengwo']['id']})
        if not entry:
            entry = {}

        crumb_list = data.pop('crumbIds')
        crumb = []
        super_adm = []
        for cid in crumb_list:
            ret = col_mdd.find_one({'source.mafengwo.id': cid}, {'_id': 1, 'zhName': 1, 'enName': 1})
            if ret:
                crumb.append(ret)
                sa_entry = {'id': ret['_id']}
                for tmp in ['zhName', 'enName']:
                    if tmp in ret:
                        sa_entry[tmp] = ret[tmp]
                super_adm.append(sa_entry)

        data['locList'] = crumb
        # data['superAdm'] = super_adm

        for k in data:
            entry[k] = data[k]

        entry['className'] = 'models.geo.Locality'
        entry['_id'] = ObjectId()
        col.save(entry)
        col = utils.get_mongodb('geo', 'Destination', profile='mongodb-general')
        col.save(entry)

        return item

    def process_vs(self, item, spider):
        data = item['data']
        col_name = item['col_name']
        db_name = item['db_name']
        col = utils.get_mongodb(db_name, col_name, profile='mongodb-general')
        col_mdd = utils.get_mongodb('geo', 'Destination', profile='mongodb-general')

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
