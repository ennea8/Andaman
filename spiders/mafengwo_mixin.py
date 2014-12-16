# coding=utf-8
import re

from scrapy import Request


__author__ = 'zephyre'


class MafengwoSugMixin(object):
    """
    蚂蜂窝输入提示的获取
    示例：http://www.mafengwo.cn/group/ss.php?key=%E5%B7%B4%E5%8E%98%E5%B2%9B
    """

    @staticmethod
    def mfw_sug_req(keyword, **kwargs):
        from urllib import quote_plus

        if isinstance(keyword, unicode):
            keyword = keyword.encode('utf-8')

        quoted = quote_plus(keyword)

        return Request(url='http://www.mafengwo.cn/group/ss.php?callback=j&key=%s' % quoted, **kwargs)

    @staticmethod
    def parse_mfw_sug(response):
        from urllib import unquote_plus

        rtext = unquote_plus(response.body[3:-2])

        # j('search://|mdd|/group/cs.php?t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=mdd&l=%2Ftravel-scenic-spot%2F
        # mafengwo%2F11124.html&d=%E4%BC%A6%E6%95%A6|ss-place|伦敦|英格兰|伦敦|search://|gonglve|/group/cs.php?t=
        # %E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=gonglve&l=%2Fgonglve%2Fmdd-11124.html&d=%E4%BC%A6%E6%95%A6%E6%97
        # %85%E6%B8%B8%E6%94%BB%E7%95%A5|ss-gonglve|伦敦旅游攻略|311730下载|伦敦旅游攻略|search://|gonglve|/group/cs.php?
        # t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=gonglve&l=%2Fgonglve%2Fzt-423.html&d=%E4%BC%A6%E6%95%A6%E5%B0%8F%E5
        # %BA%97%E6%94%BB%E7%95%A5|ss-gonglve|伦敦小店攻略|79716下载|伦敦小店攻略|search://|hotel|/group/cs.php?t=%E6%90
        # %9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=hotel&l=%2Fhotel%2F11124%2F&d=%E4%BC%A6%E6%95%A6%E9%85%92%E5%BA%97
        # |ss-hotel|伦敦酒店|2304间<i class="ico-new"></i>|伦敦酒店|search://|wenda|/group/cs.php?t=%E6%90%9C%E7%B4%A2
        # %E7%9B%B4%E8%BE%BE&p=wenda&l=%2Fwenda%2Farea-11124.html&d=%E4%BC%A6%E6%95%A6%E9%97%AE%E7%AD%94
        # |ss-ask|伦敦问答|143条|伦敦问答|search://|scenic|/group/cs.php?t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE
        # &p=scenic&l=%2Fjd%2F11124%2Fgonglve.html&d=%E4%BC%A6%E6%95%A6%E6%99%AF%E7%82%B9
        # |ss-scenic|伦敦景点|108个|伦敦景点|search://|tsms|/group/cs.php?t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=tsms
        # &l=%2Fcy%2F11124%2Ftese.html&d=%E4%BC%A6%E6%95%A6%E7%89%B9%E8%89%B2%E7%BE%8E%E9%A3%9F
        # |ss-cate|伦敦特色美食|5条|伦敦特色美食|search://|mdd|/group/cs.php?t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE
        # &p=mdd&l=%2Ftravel-scenic-spot%2Fmafengwo%2F60657.html&d=%E4%BC%A6%E6%95%A6%E5%BE%B7%E9%87%8C
        # |ss-place|伦敦德里|北爱尔兰|伦敦德里|search://|mdd|/group/cs.php?t=%E6%90%9C%E7%B4%A2%E7%9B%B4%E8%BE%BE&p=mdd
        # &l=%2Ftravel-scenic-spot%2Fmafengwo%2F134570.html&d=%E4%BC%A6%E6%95%A6%28%E5%AE%89%E5%A4%A7%E7%95%A5%E7%9C
        # %81%29|ss-place|伦敦(安大略省)|安大略省|伦敦(安大略省)|search://|user|/group/s.php?q=%E4%BC%A6%E6%95%A6&t=user
        # |ss-user|伦敦|274个|搜&quot;伦敦&quot;相关用户|search://|more|/group/s.php?q=%E4%BC%A6%E6%95%A6||伦敦||
        # 查看&quot;伦敦&quot;更多搜索结果')

        tmpl = {'mdd': {'title': r'\|mdd\|',
                        'id': r'/travel-scenic-spot/mafengwo/(\d+)\.html',
                        'name': r'&d=([^\|&]+)'},
                'vs': {'title': r'\|scenic\|',
                       'id': r'/poi/(\d+)\.html',
                       'name': r'&d=([^\|&]+)'}}

        title = response.meta['sug_type']
        if title not in tmpl:
            return []
        tmpl = tmpl[title]

        results = []
        for r in filter(lambda val: re.search(tmpl['title'], val), re.split(r'search://', rtext)):
            match = re.search(tmpl['id'], r)
            if not match:
                continue
            rid = int(match.group(1))
            name = re.search(tmpl['name'], r).group(1)
            results.append({'id': rid, 'name': name})

        return results


