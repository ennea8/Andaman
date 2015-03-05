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

    参数：
    * --city: 指定需要抓取的城市
    * --region: 可选值：abroad（抓取国外数据），domestic（抓取国外数据）
    """

    name = 'dianping'
    uuid = '9473fbd6-1af1-4037-945e-83043a5f298d'

    @staticmethod
    def process_arguments():
        import argparse

        parser = argparse.ArgumentParser()
        parser.add_argument('--city', type=str, nargs='*')
        parser.add_argument('--region', choices=['abroad', 'domestic'])
        args, leftovers = parser.parse_known_args()
        city_list = set((unicode(tmp) for tmp in args.city)) if args.city else set([])
        region = args.region

        return {'city_list': city_list, 'region': region}

    def start_requests(self):
        yield Request(url='http://www.dianping.com/citylist', callback=self.parse_city_list)

    def parse_city_list(self, response):
        """
        处理城市列表，如：http://www.dianping.com/citylist
        """
        arguments = self.process_arguments()
        city_list = arguments['city_list']
        region = arguments['region']

        # 根据region判断是抓取国内还是国外的城市
        domestict_list, abroad_list = response.xpath('//ul[@id="divArea"]')[:2]
        if not region:
            region_list = [domestict_list, abroad_list]
        elif region == 'domestic':
            region_list = [domestict_list]
        else:
            region_list = [abroad_list]

        for region_node in region_list:
            for node in region_node.xpath(r'.//*[contains(@class,"terms")]'
                                          r'//a[@href and contains(@onclick,"pageTracker._trackPageview")]'):
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

                # 根据city_list进行筛选
                if city_list and (city_name not in city_list and city_pinyin not in city_list):
                    continue

                yield Request(url=url, meta={'data': {'city_name': city_name, 'city_pinyin': city_pinyin}},
                              callback=self.parse_city_main)

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
            match = re.search(r'_setCityId.+?(\d+)', script)
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

        # 获得city item
        item = DianpingItem()
        item['type'] = 'city'
        item['data'] = m
        yield item

        yield Request(url='http://www.dianping.com/search/category/%d/10/g0r0' % city_id, meta={'data': m},
                      callback=self.parse_dining_search)

    def parse_dining_search(self, response):
        """
        解析美食的搜索页面，如：http://www.dianping.com/search/category/2/10/g0r0
        """
        # 按照类别进行细分
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

            if u'其他' not in cat_name and u'其它' not in cat_name:
                m['cat_name'] = cat_name
                m['cat_id'] = cat_id

            page = 1
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
            yield Request(url=url, meta={'data': m}, callback=self.parse_dining_details, dont_filter=True)

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
                tmp = '\n'.join(filter(lambda v: v,
                                       (tmp.strip() for tmp in other_info_node.xpath('./text()').extract()))).strip()
                if not tmp:
                    continue
                desc = tmp

        special_dishes = set([])
        tmp = response.xpath('//div[@id="shop-tabs"]/script/text()').extract()
        if tmp:
            sel = Selector(text=tmp[0])
            tmp = sel.xpath('//div[contains(@class,"shop-tab-recommend")]/p[@class="recommend-name"]'
                            '/a[@class="item" and @title]/@title').extract()
            special_dishes = set(filter(lambda v: v, [tmp.strip() for tmp in tmp]))

        lat = None
        lng = None
        match = re.search(r'lng:(\d+\.\d+),lat:(\d+\.\d+)', response.body)
        if match:
            lng = float(match.group(1))
            lat = float(match.group(2))

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
        m['lat'] = lat
        m['lng'] = lng

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
    def add_to_set(data, key):
        """
        有一类key，比如tags这一类的，应该是set类型。所以，在处理这类数据的时候，应该采用$addToSet，而不是$set
        """
        try:
            elements = data.pop(key)
            if not elements:
                return []
            else:
                return list(set(elements))
        except KeyError:
            return []

    @staticmethod
    def process_dining_item(item, spider):
        data = item['data']

        add_set_ops = {}
        for key in ['special_dishes', 'tags']:
            elements = DianpingPipeline.add_to_set(data, key)
            if elements:
                add_set_ops[key] = {'$each': elements}

        col = get_mongodb('raw_dianping', 'Dining', 'mongo-raw')
        ops = {'$set': data}
        if add_set_ops:
            ops['$addToSet'] = add_set_ops
        col.update({'shop_id': data['shop_id']}, ops, upsert=True)
        return item

    @staticmethod
    def process_city_item(item, spider):
        data = item['data']
        col = get_mongodb('raw_dianping', 'City', 'mongo-raw')
        col.update({'city_id': data['city_id']}, {'$set': data}, upsert=True)
        return item

    @staticmethod
    def process_item(item, spider):
        if item['type'] == 'dining':
            return DianpingPipeline.process_dining_item(item, spider)
        elif item['type'] == 'city':
            return DianpingPipeline.process_city_item(item, spider)
        else:
            return item




