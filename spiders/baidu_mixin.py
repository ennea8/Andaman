# coding=utf-8
import json
import re
from scrapy import Request
import utils

__author__ = 'zephyre'


class BaiduSugMixin(object):
    """
    百度旅游输入提示的获取
    示例：http://lvyou.baidu.com/destination/ajax/sug?wd=%E5%B7%B4%E9%BB%8E&prod=lvyou_new&su_num=20
    """

    @staticmethod
    def baidu_sug_req(keyword, **kwargs):
        from urllib import quote_plus

        if isinstance(keyword, unicode):
            keyword = keyword.encode('utf-8')

        quoted = quote_plus(keyword)

        return Request(url='http://lvyou.baidu.com/destination/ajax/sug?wd=%s&prod=lvyou_new&su_num=20' % quoted,
                       **kwargs)

    @staticmethod
    def parse_baidu_sug(response):
        try:
            sug_text = json.loads(response.body)['data']['sug']
            sug = json.loads(sug_text)
            result = []
            for s in sug['s']:
                tmp = re.split(r'\$', s)
                entry = {'sname': tmp[0].strip(),
                         'parents': tmp[6].strip(),
                         'sid': tmp[8].strip(),
                         'surl': tmp[22].strip(),
                         'parent_sid': tmp[26].strip(),
                         'type_code': int(tmp[24])}

                mx = float(tmp[14])
                my = float(tmp[16])
                entry['lng'], entry['lat'] = utils.mercator2wgs(mx, my)

                result.append(entry)

            return result
        except (ValueError, KeyError):
            return None
