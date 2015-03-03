# coding=utf-8
import copy
import json
import re

from scrapy import Request, log, Item, Field, Selector

from spiders import AizouCrawlSpider, AizouPipeline
from utils.database import get_mongodb


__author__ = 'zephyre'


class DianpingItem(Item):
    data = Field()
    type = Field()


class DianpingSpider(AizouCrawlSpider):
    """
    抓取大众点评
    """

    name = 'dianping'
    uuid = '9473fbd6-1af1-4037-945e-83043a5f298d'

    def start_requests(self):
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--city', required=True, type=str, nargs='*')
        args, leftovers = parser.parse_known_args()
        city_list = set((unicode(tmp) for tmp in args.city))

        yield Request(url='http://www.dianping.com/citylist', callback=self.parse_city_list,
                      meta={'city_list': city_list})

    def parse_city_list(self, response):
        city_list = response.meta['city_list']

        city_candidates = {}
        for node in response.xpath(r'//*[contains(@class,"terms")]//a[@href and @onclick]'):
            if not re.search(r'pageTracker\._trackPageview', node.xpath('./@onclick').extract()[0]):
                continue

            href = node.xpath('./@href').extract()[0]
            url = self.build_href(response.url, href)
            match = re.search(r'^/(\w+)$', href)
            if not match:
                continue
            city_pinyin = match.group(1)

            tmp = list(filter(lambda v: v, (tmp.strip() for tmp in node.xpath('.//text()').extract())))
            if not tmp:
                continue
            city_name = tmp[0]
            city_candidates[city_pinyin] = {'city_name': city_name, 'city_pinyin': city_pinyin, 'url': url}

        for c in city_candidates.values():
            if c['city_name'] not in city_list and c['city_pinyin'] not in city_list:
                continue

            url = c.pop('url')
            yield Request(url=url, meta={'data': c}, callback=self.parse_city_main)

    def parse_city_main(self, response):
        main_url = response.url
        if main_url[-1] != '/':
            main_url += '/'

        food_url = main_url + 'food'
        yield Request(url=food_url, meta={'data': response.meta['data']}, callback=self.parse_city_food)

    @staticmethod
    def get_city_id(response):
        """
        获得city_id
        """
        for script in response.xpath('//script/text()').extract():
            match = re.search(r'_setCityId.+(\d+)', script)
            if not match:
                continue
            return int(match.group(1))

        return None

    def parse_city_food(self, response):
        """
        解析城市美食主页面，如：http://www.dianping.com/beijing/food
        """
        city_id = self.get_city_id(response)
        if not city_id:
            self.log('Unable to get city id for: %s' % response.url, level=log.WARNING)
            return

        m = copy.deepcopy(response.meta['data'])
        m['city_id'] = city_id
        yield Request(url='http://www.dianping.com/search/category/%d/10/g0r0' % city_id, meta={'data': m},
                      callback=self.parse_dining_search)

    def parse_dining_search(self, response):
        """
        解析美食的搜索页面，如：http://www.dianping.com/search/category/2/10/g0r0
        """
        for cat_node in response.xpath('//div[@id="classfy"]/a[@href]'):
            tmp = cat_node.xpath('./span/text()').extract()
            if not tmp:
                continue
            cat_name = tmp[0].strip()
            href = cat_node.xpath('./@href').extract()[0]
            match = re.search(r'/g(\d+)$', href)
            if not match:
                continue
            cat_id = int(match.group(1))
            url = self.build_href(response.url, href)
            if url[-1] == '/':
                url = url[:-1]
            m = copy.deepcopy(response.meta['data'])
            m['cat_name'] = cat_name
            m['cat_id'] = cat_id
            page = 1
            m['page'] = page
            url_template = url + 'p%d'
            m['url_template'] = url_template
            url = url_template % page
            yield Request(url=url, meta={'data': m}, callback=self.parse_dining_list)

    def dining_list_pagination(self, response):
        """
        处理搜索结果列表的翻页
        :return: Request类型的generator
        """
        page_list = []
        for page_text in response.xpath('//div[@class="page"]/a[@href and @data-ga-page]/@data-ga-page').extract():
            try:
                page_list.append(int(page_text))
            except ValueError:
                continue
        if page_list:
            url_template = response.meta['data']['url_template']
            max_page = sorted(page_list)[-1]
            for page in xrange(2, max_page + 1):
                m = copy.deepcopy(response.meta['data'])
                m['page'] = page
                yield Request(url=url_template % page, meta={'data': m}, callback=self.parse_dining_list)

    def parse_dining_list(self, response):
        """
        解析餐厅的搜索结果列表，如：http://www.dianping.com/search/category/2/10/g118p1
        """
        for req in self.dining_list_pagination(response):
            yield req

        for shop_node in response.xpath('//div[@id="shop-all-list"]/ul/li'):
            tmp = shop_node.xpath('.//div[@class="tit"]/a[@title and @href]')
            if not tmp:
                continue
            title = tmp[0].xpath('./@title').extract()[0].strip()
            href = tmp[0].xpath('./@href').extract()[0].strip()
            mean_price = None
            tmp = shop_node.xpath('.//div[@class="comment"]/a[@href and @class="mean-price"]/b/text()').extract()
            if tmp:
                match = re.search(r'(\d+)', tmp[0])
                if match:
                    mean_price = int(match.group(1))
            tmp = shop_node.xpath('.//div[@class="tag-addr"]/span[@class="addr"]/text()').extract()
            if tmp:
                addr = tmp[0].strip()
            else:
                addr = None
            tmp = shop_node.xpath('.//div[@class="pic"]/a[@href]/img[@data-src]/@data-src').extract()
            if tmp:
                image_src = tmp[0]
            else:
                image_src = None

            m = copy.deepcopy(response.meta['data'])
            m['title'] = title
            m['mean_price'] = mean_price
            m['addr'] = addr
            m['cover_image'] = image_src
            url = self.build_href(response.url, href)
            yield Request(url=url, meta={'data': m}, callback=self.parse_dining_details)

    def parse_dining_details(self, response):
        """
        解析餐厅的详情页面，如：http://www.dianping.com/shop/3578044
        """
        # 保证这是一个餐厅页面
        tmp = response.xpath('//div[@class="breadcrumb"]/a[@href]/text()').extract()
        if not tmp or u'餐厅' not in tmp[0]:
            return

        basic_info_node = response.xpath('//div[@id="basic-info"]')[0]

        match = re.search(r'shop/(\d+)', response.url)
        if not match:
            return
        shop_id = int(match.group(1))

        taste_rating = None
        env_rating = None
        service_rating = None

        def extract_rating(text):
            match2 = re.search(r'\d+\.\d+', text)
            if match2:
                return float(match2.group())
            else:
                return None

        for info_text in basic_info_node.xpath('.//div[@class="brief-info"]/span[@class="item"]/text()').extract():
            if info_text.startswith(u'口味'):
                taste_rating = extract_rating(info_text)
            elif info_text.startswith(u'环境'):
                env_rating = extract_rating(info_text)
            elif info_text.startswith(u'服务'):
                service_rating = extract_rating(info_text)

        tel = None
        tmp = basic_info_node.xpath('.//p[contains(@class,"expand-info") and contains(@class,"tel")]'
                                    '/span[@itemprop="tel"]/text()').extract()
        if tmp and tmp[0].strip():
            tel = tmp[0].strip()

        open_time = None
        tags = set([])
        desc = None
        for other_info_node in basic_info_node.xpath('.//div[contains(@class,"other")]/p[contains(@class,"info")]'):
            tmp = other_info_node.xpath('./span[@class="info-name"]/text()').extract()
            if not tmp:
                continue
            info_name = tmp[0]
            if info_name.startswith(u'营业时间'):
                tmp = other_info_node.xpath('./span[@class="item"]/text()').extract()
                if not tmp:
                    continue
                open_time = tmp[0].strip()
            elif info_name.startswith(u'分类标签'):
                tmp = other_info_node.xpath('./span[@class="item"]/a/text()').extract()
                for tag in tmp:
                    tags.add(tag.strip())
            elif info_name.startswith(u'餐厅简介'):
                tmp = other_info_node.xpath('./span[@class="item"]/text()').extract()
                if not tmp:
                    continue
                desc = tmp[0].strip()

        special_dishes = set([])
        tmp = response.xpath('//div[@id="shop-tabs"]/script/text()').extract()
        if tmp:
            sel = Selector(text=tmp[0])
            tmp = sel.xpath('//div[contains(@class,"shop-tab-recommend")]/p[@class="recommend-name"]'
                            '/a[@class="item" and @title]/@title').extract()
            special_dishes = set(filter(lambda v: v, [tmp.strip() for tmp in tmp]))

        m = copy.deepcopy(response.meta['data'])
        m['shop_id'] = shop_id
        m['taste_rating'] = taste_rating
        m['env_rating'] = env_rating
        m['service_rating'] = service_rating
        m['tel'] = tel
        m['open_time'] = open_time
        m['desc'] = desc
        m['special_dishes'] = special_dishes
        m['tags'] = tags

        template = 'http://www.dianping.com/ajax/json/shop/wizard/getReviewListFPAjax?' \
                   'act=getreviewfilters&shopId=%d&tab=all'
        yield Request(url=template % shop_id, meta={'data': m, 'proxy_switch_ctx': {'validator': self.json_validator}},
                      callback=self.parse_review_stat)

    @staticmethod
    def json_validator(response):
        try:
            data = json.loads(response.body)
            return data['code'] == 200 and 'msg' in data
        except (ValueError, KeyError):
            return False

    @staticmethod
    def parse_review_stat(response):
        """
        解析评论统计
        """
        data = json.loads(response.body)
        m = copy.deepcopy(response.meta['data'])
        m['reivew_stat'] = data['msg']

        item = DianpingItem()
        item['type'] = 'dining'
        item['data'] = m
        yield item


class DianpingPipeline(AizouPipeline):
    spiders = [DianpingSpider.name]
    spiders_uuid = [DianpingSpider.uuid]

    @staticmethod
    def process_item(item, spider):
        data = item['data']
        data['special_dishes'] = list(data['special_dishes'])
        data['tags'] = list(data['tags'])
        col = get_mongodb('raw_dianping', 'Dining', 'mongo-raw')
        col.update({'shop_id': data['shop_id']}, {'$set': data}, upsert=True)





