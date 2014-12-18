# coding=utf-8
import copy
import json
import re

from scrapy import Request, Item, Field

import conf
from spiders import AizouCrawlSpider, AizouPipeline


__author__ = 'zephyre'


class MergeItem(Item):
    data = Field()
    db = Field()
    col = Field()
    profile = Field()


class MergeSpider(AizouCrawlSpider):
    """
    数据结果的合并
    参数：
    src-db
    src-col
    src-profile
    dst-db
    dst-col
    dst-profile
    pk
    query
    limit
    """

    name = 'merge-proc'
    uuid = '6f40c817-166e-472b-b066-5df0be13dda3'

    def start_requests(self):
        # 处理配置文件
        if 'rule' in self.param:
            section = conf.global_conf.get(self.param['rule'][0], {})
            for key in section:
                self.param[key] = re.split(r'\s+', section.get(key))

        yield Request(url='http://www.baidu.com')

    @staticmethod
    def fetch_by_pk(entry, pk_desc):
        """
        给定主键描述，从entry中取得键值。比如：'source.mafengwo.id' => entry['source']['mafengwo']['id']
        :param entry:
        :param pk_desc:
        :return:
        """
        val = None
        try:
            for idx, pk_term in enumerate(pk_desc.split('.')):
                if idx == 0:
                    val = entry[pk_term]
                else:
                    val = val[pk_term]
            return val
        except KeyError:
            return

    def merge(self, src_entry, dst_entry):
        # 字段信息
        # 默认情况下append_field为所有字段
        append_fields = self.param['append-fields'] if 'append-fields' in self.param else src_entry.keys()
        overwrite_fields = self.param['overwrite-fields'] if 'overwrite-fields' in self.param else []
        set_fields = self.param['set-fields'] if 'set-fields' in self.param else []
        tedious_fields = self.param['tedious-fields'] if 'tedious-fields' in self.param else []

        result = copy.deepcopy(dst_entry) if dst_entry else {}
        # 保证目标的_id不变
        oid = dst_entry['_id']

        for f in overwrite_fields:
            if f not in src_entry or not src_entry[f]:
                continue
            result[f] = src_entry[f]

        for f in set_fields:
            if f not in src_entry or not src_entry:
                continue
            dst_set = result[f] if f in result else []
            for entry in src_entry[f]:
                if entry not in dst_set:
                    dst_set.append(entry)
            result[f] = dst_set

        for f in tedious_fields:
            if f not in src_entry or not src_entry[f]:
                continue
            if f not in result or (len(str(result[f])) < len(str(src_entry[f]))):
                result[f] = src_entry[f]

        for f in append_fields:
            if f not in src_entry or not src_entry[f] or f in result:
                continue
            result[f] = src_entry[f]

        result['_id'] = oid

        return result

    def parse(self, response):
        src_db = self.param['src-db'][0]
        src_col_name = self.param['src-col'][0]
        src_profile = self.param['src-profile'][0] if 'src-profile' in self.param else 'mongodb-general'
        dst_db = self.param['dst-db'][0]
        dst_col_name = self.param['dst-col'][0]
        dst_profile = self.param['dst-profile'][0] if 'dst-profile' in self.param else 'mongodb-general'

        src_col = self.fetch_db_col(src_db, src_col_name, src_profile)
        dst_col = self.fetch_db_col(dst_db, dst_col_name, dst_profile)

        query = json.loads(self.param['query'][0]) if 'query' in self.param else {}
        cursor = src_col.find(query)
        if 'limit' in self.param:
            cursor.limit(int(self.param['limit'][0]))

        for entry in cursor:
            # 举例：
            # src: {'source': {'mafengwo': {}}, {'baidu': {}}}
            # dst: {'source': {'mafengwo': {}}, {'baidu': {}}}
            # src_pk: 'source.baidu.id', dst_pk: 'source.mafengwo.id'
            # 需要在dst中，找到mafengwo.id和src匹配的项目(A)。
            # 然后根据src的baidu.id，在dst中找到与之相对的项目(B)。
            # 二者都需要匹配

            # 需要合并的目标
            merge_targets = {}

            for pk_desc in self.param['pk']:
                pk = self.fetch_by_pk(entry, pk_desc)
                if not pk:
                    continue
                cp = dst_col.find_one({pk_desc: pk})
                if cp:
                    merge_targets[cp['_id']] = cp

            item = MergeItem()
            item['db'] = dst_db
            item['col'] = dst_col
            item['profile'] = dst_profile

            if merge_targets:
                for t in merge_targets.values():
                    result = self.merge(entry, t)
                    item['data'] = result
                    yield item
            else:
                item['data'] = entry
                yield item


class MergePipeline(AizouPipeline):
    spiders = [MergeSpider.name]
    spiders_uuid = [MergeSpider.uuid]

    def process_item(self, item, spider):
        if not self.is_handler(item, spider):
            return item

        col = spider.fetch_db_col(item['db'], item['col'], profile=item['profile'])
        col.save(item['data'])