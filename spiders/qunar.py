# coding=utf-8
import copy
import json
import re
from scrapy import Item, Field, Request, Selector, log
from spiders import AizouCrawlSpider, AizouPipeline
from urlparse import urljoin
from datetime import datetime, timedelta

__author__ = 'zephyre'


class QunarItem(Item):
    data = Field()
    type = Field()


class QunarSpider(AizouCrawlSpider):
    """
    去哪儿
    """

    name = 'qunar'
    uuid = 'f2dd0538-6ec4-4d4b-8ece-20b66f8c8433'

    def __init__(self, *a, **kw):
        AizouCrawlSpider.__init__(self, *a, **kw)

        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--skip', type=int, default=0)
        parser.add_argument('--limit', type=int)
        parser.add_argument('--city', type=int, nargs='*')

        self.args, leftovers = parser.parse_known_args()

    def start_requests(self):
        yield Request(url='http://travel.qunar.com/place/', dont_filter=True)

    def parse(self, response):
        city_list = []
        if self.args.city:
            # 如果添加了city限制
            city_list = [{'id': city_id} for city_id in self.args.city]
        else:
            for node in response.selector.xpath('//dl[contains(@class,"listbox")]//ul[contains(@class,"list_item")]'
                                                '/li[contains(@class,"item")]/a[@href]'):
                href = node.xpath('./@href').extract()[0]
                # 获得拼音和id
                match = re.search(r'p-\w{2}(\d+)-(\w+)', href)
                loc_id = int(match.group(1))

                # 如果添加了city限制
                if self.args.city and loc_id not in self.args.city:
                    continue

                pinyin = match.group(2).lower().strip()
                name = node.xpath('./text()').extract()[0].strip()
                city_list.append({'id': loc_id, 'pinyin': pinyin, 'zh_name': name})

        for city in city_list:
            yield Request(url='http://travel.qunar.com/p-cs%d' % city['id'], callback=self.parse_city,
                          meta={'data': {'targets': copy.deepcopy([city])}})

    def parse_city(self, response):
        data = response.meta['data']

        tmpl = response.url + '-meishi-3-1-%d'
        url = tmpl % 1
        yield Request(url=url, callback=self.parse_poi_list, meta={'tmpl': tmpl, 'page': 1, 'type': 'dining',
                                                                   'data': data})

        tmpl = response.url + '-gouwu-3-1-%d'
        url = tmpl % 1
        yield Request(url=url, callback=self.parse_poi_list, meta={'tmpl': tmpl, 'page': 1, 'type': 'shopping',
                                                                   'data': data})

    def parse_poi_list(self, response):
        data = response.meta['data']
        tmpl = response.meta['tmpl']
        page = response.meta['page']
        poi_type = response.meta['type']

        if page == 1:
            # 访问其它页面
            page_list = []
            for tmp in response.selector.xpath('//div[@class="b_paging"]/a[@class="page" and @href]/text()').extract():
                try:
                    page_list.append(int(tmp))
                except ValueError:
                    continue
            if page_list:
                for idx in xrange(2, max(page_list) + 1):
                    url = tmpl % idx
                    yield Request(url=url, callback=self.parse_poi_list,
                                  meta={'tmpl': tmpl, 'page': idx, 'type': poi_type,
                                        'data': copy.deepcopy(data)})

        for idx, node in enumerate(response.selector.xpath('//div[@class="listbox"]/ul[contains(@class,"list_item")]'
                                                           '/li[@class="item" and @data-lat and @data-lng]')):
            lat = float(node.xpath('./@data-lat').extract()[0])
            lng = float(node.xpath('./@data-lng').extract()[0])
            href = node.xpath('.//a[@data-beacon="poi" and @href]/@href').extract()[0]
            url = urljoin(response.url, href)
            match = re.search(r'p-oi(\d+)-(\w+)', url)
            poi_id = int(match.group(1))
            pinyin = match.group(2).lower().strip()
            zh_name = node.xpath('.//a[@data-beacon="poi"]/span[@class="cn_tit"]/text()').extract()[0].strip()

            rating = float(node.xpath('.//div[@class="scorebox"]/span[@class="cur_score"]/text()').extract()[0]) / 5.0

            the_data = copy.deepcopy(data)
            the_data['lat'] = lat
            the_data['lng'] = lng
            the_data['poi_id'] = poi_id
            the_data['pinyin'] = pinyin
            the_data['zh_name'] = zh_name
            the_data['rating'] = rating
            the_data['ord'] = (page - 1) * 10 + idx

            yield Request(url=url, callback=self.parse_poi, meta={'data': the_data, 'type': poi_type})

    def parse_poi(self, response):
        data = response.meta['data']
        poi_type = response.meta['type']

        tmp = response.selector.xpath('//div[@class="m_scorebox"]//div[@class="time"]/text()').extract()
        if tmp:
            match = re.search(ur'(\d+)\s*元', tmp[0])
            if match:
                data['cost'] = int(match.group(1))

        tmp = response.selector.xpath('//div[contains(@class,"b_detail_summary")]'
                                      '/div[@class="e_db_content_box"]/p/text()').extract()
        if tmp:
            desc = tmp[0].strip()
        else:
            desc = ''
        data['desc'] = desc

        item = QunarItem()
        item['data'] = data
        item['type'] = poi_type
        yield item

        offset = 0
        tmpl = 'http://travel.qunar.com/place/api/poi/image?offset=%d&limit=50&poiId=%d'
        url = tmpl % (offset, data['poi_id'])
        yield Request(url=url, callback=self.parse_images,
                      meta={'poi_id': data['poi_id'], 'item_type': poi_type, 'offset': offset, 'tmpl': tmpl})

        tmpl = 'http://travel.qunar.com/place/api/html/comments/poi/%d?sortField=1&img=false&pageSize=25&page=%d'
        url = tmpl % (data['poi_id'], 1)
        yield Request(url=url, callback=self.parse_comments,
                      meta={'poi_id': data['poi_id'], 'item_type': poi_type, 'page': 1, 'tmpl': tmpl})

    def parse_images(self, response):
        try:
            data = json.loads(response.body)
        except ValueError:
            from urlparse import urlparse

            ret = urlparse(response.url)
            if ret.netloc == 'security.qunar.com' and 'proxy_middleware' in response.meta and 'proxy' in response.meta:
                # 注销相应的proxy
                proxy = response.meta['proxy']
                mw = response.meta['proxy_middleware']
                mw.deregister(proxy)
                self.log('Deregistering proxy: %s, %d proxies left.' % (proxy, len(mw.proxy_list)), log.WARNING)

                # 重新请求
                if 'redirect_urls' in response.meta and response.meta['redirect_urls']:
                    url = response.meta['redirect_urls'][0]
                    meta = {key: response.meta[key] for key in ['poi_id', 'item_type', 'offset', 'tmpl']}
                    yield Request(url=url, callback=self.parse_images, meta=meta, dont_filter=True)
            return

        offset = response.meta['offset']
        poi_type = response.meta['item_type']
        tmpl = response.meta['tmpl']
        poi_id = response.meta['poi_id']

        if offset == 0:
            # 如果有新的图像，继续请求
            page_num = data['totalCount'] / 50 + 1

            for page_idx in xrange(2, page_num + 1):
                new_offset = page_idx * 50
                url = tmpl % (new_offset, poi_id)
                yield Request(url=url, callback=self.parse_images,
                              meta={'poi_id': poi_id, 'item_type': poi_type, 'offset': new_offset, 'tmpl': tmpl})

        if data['data'] is None:
            data['data'] = []
        for idx, entry in enumerate(data['data']):
            url = entry['url']
            item = QunarItem()
            item['data'] = {'user_id': entry['userId'], 'user_name': entry['userName'], 'url': url,
                            'poi_id': response.meta['poi_id'], 'poi_type': response.meta['item_type'],
                            'image_id': entry['id'], 'ord': idx + offset}
            item['type'] = 'image'
            yield item

    def parse_comments(self, response):
        try:
            raw_data = json.loads(response.body)
        except ValueError:
            from urlparse import urlparse

            ret = urlparse(response.url)
            if ret.netloc == 'security.qunar.com' and 'proxy_middleware' in response.meta and 'proxy' in response.meta:
                # 注销相应的proxy
                proxy = response.meta['proxy']
                mw = response.meta['proxy_middleware']
                mw.deregister(proxy)
                self.log('Deregistering proxy: %s, %d proxies left.' % (proxy, len(mw.proxy_list)), log.WARNING)

                # 重新请求
                if 'redirect_urls' in response.meta and response.meta['redirect_urls']:
                    url = response.meta['redirect_urls'][0]
                    meta = {key: response.meta[key] for key in ['poi_id', 'item_type', 'page', 'tmpl']}
                    yield Request(url=url, callback=self.parse_comments, meta=meta, dont_filter=True)
            return

        poi_id = response.meta['poi_id']
        item_type = response.meta['item_type']
        page = response.meta['page']
        sel = Selector(text=raw_data['data'])
        node_list = sel.xpath('//ul[@id="comment_box"]/li[contains(@class,"e_comment_item") and @id]')

        from lxml import etree

        if not node_list:
            return

        for node in node_list:
            comment = {'comment_id': int(re.search(r'(\d+)$', node.xpath('./@id').extract()[0]).group(1)),
                       'poi_id': poi_id, 'poi_type': item_type}
            tmp = node.xpath('.//a[@data-beacon="comment_title"]/text()').extract()
            comment['title'] = tmp[0].strip() if tmp else ''
            tmp = node.xpath('.//span[@class="total_star"]/span[contains(@class,"cur_star")]/@class').extract()
            if tmp:
                tmp = re.search(r'star_(\d)$', tmp[0].strip())
                if tmp:
                    comment['rating'] = float(tmp.group(1)) / 5.0
            tmp = node.xpath('.//div[@class="e_comment_content"]')
            if tmp:
                comment['contents'] = ''.join(
                    etree.fromstring(tmp[0].extract(), parser=etree.HTMLParser()).itertext()).strip()

            tmp = node.xpath('.//div[@class="e_comment_usr"]/div[@class="e_comment_usr_pic"]'
                             '//img[@src]/@src').extract()
            img_src = tmp[0] if tmp else None
            tmp = node.xpath('.//div[@class="e_comment_usr"]/div[@class="e_comment_usr_name"]'
                             '/a[@href]/text()').extract()
            user_name = tmp[0] if tmp else None
            if user_name:
                comment['user_name'] = user_name

            for tmp in node.xpath('.//div[@class="e_comment_add_info"]/ul/li/text()').extract():
                try:
                    ts = datetime.strptime(tmp, '%Y-%m-%d')
                    comment['cTime'] = long(
                        (ts - timedelta(seconds=8 * 3600) - datetime.utcfromtimestamp(0)).total_seconds() * 1000)
                    break
                except ValueError:
                    pass

            item = QunarItem()
            item['data'] = comment
            item['type'] = 'comment'

            yield item

            if user_name and img_src:
                yield Request(url=img_src, callback=self.resolve_pic, meta={'item': item})

        page += 1
        tmpl = response.meta['tmpl']
        url = tmpl % (poi_id, page)
        yield Request(url=url, callback=self.parse_comments,
                      meta={'poi_id': poi_id, 'item_type': item_type, 'page': page, 'tmpl': tmpl})

    @staticmethod
    def resolve_pic(response):
        from hashlib import md5

        url = response.url
        new_url = re.sub(r'_r_\d+[^/]+$', '', url)
        item = response.meta['item']
        item['data']['avatar'] = new_url
        yield item

        # 返回图像
        item = QunarItem()
        item['data'] = {'url': new_url, 'image_id': md5(new_url).hexdigest()}
        item['type'] = 'image'
        yield item


class QunarPipeline(AizouPipeline):
    spiders = [QunarSpider.name]
    spiders_uuid = [QunarSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        data = item['data']

        if item['type'] == 'comment':
            col = spider.fetch_db_col('raw_qunar', 'Comment', 'mongodb-crawler')
            col.update({'comment_id': data['comment_id']}, {'$set': data}, upsert=True)
        elif item['type'] == 'image':
            col = spider.fetch_db_col('raw_qunar', 'Image', 'mongodb-crawler')
            col.update({'image_id': data['image_id']}, {'$set': data}, upsert=True)
        else:
            assert item['type'] in ['dining', 'shopping'], 'Invalid type: %s' % item['type']

        return item

