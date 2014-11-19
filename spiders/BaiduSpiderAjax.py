# coding=utf-8
import json

from scrapy import Selector, Request, Item, Field

from spiders import AizouCrawlSpider
import utils


class BaiduItem(Item):
    def __init__(self, *a, **kw):
        super(BaiduItem, self).__init__(*a, **kw)
        self['data'] = {}

    data = Field()


class BaiduSpider(AizouCrawlSpider):
    name = 'ajax_baidu_spider'

    def __init__(self, *a, **kw):
        super(BaiduSpider, self).__init__(*a, **kw)

    def start_requests(self):
        start_url = 'http://lvyou.baidu.com/scene/'
        yield Request(url=start_url, callback=self.parse_url)

    def parse_url(self, response):
        sel = Selector(response)
        sdata = sel.xpath('//ul[@id="J-head-menu"]/li/textarea/text()').extract()
        spot_data = (json.loads(tmp) for tmp in sdata)
        node_list = list()
        url_list = list()
        for node in spot_data:
            node_list.extend(tmp['sub'] for tmp in node)
        for tmp in node_list:
            url_list.extend([node['surl'] for node in tmp])
        for url in url_list[:1]:
            yield Request(url='http://lvyou.baidu.com/destination/ajax/jingdian?format=json&surl=%s&cid=0&pn=1' % url,
                          meta={'surl': url, 'page_idx': 1, 'item': BaiduItem()}, callback=self.parse)


    # 解析每一次请求的数据
    def parse(self, response):
        page_idx = response.meta['page_idx']
        curr_surl = response.meta['surl']
        item = response.meta['item']
        item_data = item['data']

        # 解析body
        json_data = json.loads(response.body)['data']

        # 抽取字段景点列表
        scene_list = [tmp['surl'] for tmp in json_data['scene_list']]
        next_surls = scene_list

        # 是否为第一页
        if page_idx == 1:
            # 整合url进行投递
            if 'relate_scene_list' in json_data and 'around_scene_list' in json_data:
                for key in ['relate_scene_list', 'around_scene_list']:
                    next_surls.extend([tmp['surl'] for tmp in json_data[key]])

            json_data.pop('scene_list')
            item['data'] = json_data

        for surl in next_surls:
            yield Request(url='http://lvyou.baidu.com/destination/ajax/jingdian?format=json&surl=%s&cid=0&pn=1' % surl,
                          meta={'surl': surl, 'page_idx': 1, 'item': BaiduItem()}, callback=self.parse)

        if 'scene_list' not in item_data:
            item_data['scene_list'] = []
        item_data['scene_list'].extend(scene_list)

        # 判断到达最后一页
        if not scene_list:
            yield item
        else:
            page_idx += 1
            yield Request(url='http://lvyou.baidu.com/destination/ajax/jingdian?format=json&surl=%s&cid=0&pn=%d' % (
                curr_surl, page_idx), callback=self.parse, meta={'item': item, 'surl': curr_surl, 'page_idx': page_idx})


class BaiduSpiderPipeline(object):
    spiders = [BaiduSpider.name]

    def process_item(self, item, spider):
        if type(item).__name__ != BaiduItem.__name__:
            return item

        col_loc = utils.get_mongodb('raw_data', 'BaiduAjax', profile='mongodb-crawler')
        data = {}
        if 'data' in item:
            data['data'] = item['data']
        col_loc.save(data)

        return item



