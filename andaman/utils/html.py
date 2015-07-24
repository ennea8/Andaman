# coding=utf-8
import re

import scrapy


__author__ = 'zephyre'


def html2text(html):
    html = re.sub(r'\s*<br>\s*', '\n', html)
    tmp = scrapy.Selector(text=html).xpath('./descendant-or-self::text()').extract()
    return ''.join([v.strip() for v in tmp if v.strip()])


def parse_time(time_str, tz=8):
    from datetime import datetime, timedelta

    def shift_time(time_delta, time_base=None):
        """
        通过time_base - time_delta计算时间戳
        """
        if not time_base:
            time_base = datetime.utcnow()

        return long((time_base + time_delta - datetime.utcfromtimestamp(0)).total_seconds() * 1000)

    m = re.search(ur'(\d+)\s*秒前', time_str)
    if m:
        return shift_time(timedelta(seconds=int(m.group(1))))
    m = re.search(ur'(\d+)\s*分钟前', time_str)
    if m:
        return shift_time(timedelta(seconds=int(m.group(1)) * 60))
    m = re.search(ur'(\d+)\s*小时前', time_str)
    if m:
        return shift_time(timedelta(seconds=int(m.group(1)) * 3600))
    m = re.search(ur'(\d+)\s*天前', time_str)
    if m:
        return shift_time(timedelta(days=int(m.group(1))))

    fmt = '%y/%m/%d %H:%M'
    try:
        return long((datetime.strptime(time_str, fmt) - timedelta(seconds=tz * 3600) - datetime.utcfromtimestamp(
            0)).total_seconds() * 1000)
    except (UnicodeEncodeError, ValueError):
        pass

    fmt = '%Y-%m-%d %H:%M'
    try:
        return long((datetime.strptime(time_str, fmt) - timedelta(seconds=tz * 3600) - datetime.utcfromtimestamp(
            0)).total_seconds() * 1000)
    except (UnicodeEncodeError, ValueError):
        pass

    raise ValueError(u'Invalid time string: %s' % time_str)



