# coding=utf-8

__author__ = 'Administrator'

import pymongo
from lxml import sax
import re

def NewMongoDB(host, port, db_name, coll_name):
    conn = pymongo.MongoClient(host, port)
    db = conn[db_name]
    coll = db[coll_name]
    return coll

class PostHandler(sax.ElementTreeContentHandler):
    def startElementNS(self, ns_name, qname, attributes=None):

        from urlparse import urlparse

        attrs = getattr(attributes, '_attrs')
        # 去掉<a>中的href属性，即地点的连接,连其class一同去掉
        if qname == 'a':  # and attributes.has_key('href'):
            setattr(attributes, '_attrs', {})
            # a_attrs = {}
            # for key, value in attrs.items():
            #     if key[1] == 'href':
            #         ret = urlparse(value)
            #         if not ret.netloc or 'baidu' in ret.netloc:
            #             # remove links that point to baidu sites
            #             continue
            #     if key[1] == 'class':
            #         continue
            #     a_attrs[key] = value
            # setattr(attributes, '_attrs', a_attrs)

        # 只要属性中含有style,就去掉，包含img元素
        if (None, 'style') in attrs.keys():
            style_attrs = {}
            for key, value in attrs.items():
                if key[1] == 'style':
                        continue
                style_attrs[key] = value
            setattr(attributes, '_attrs', style_attrs)

        # 对所有div标签的class进行分类处理，这个做的规范一下，挺繁琐的：
        if qname == 'div':
            for key, value in attrs.items():
                if key[1] == 'class':
                    # 如果class == floor-content,则去掉所有其它属性
                    if attrs[key] == 'floor-content':
                        floor_class = {key: attrs[key]}
                        setattr(attributes, '_attrs', floor_class)
                    # 如果class ==path-basic clearfix,想只保留需要的元素，再说
                    if attrs[key] == 'path-basic clearfix':
                        abstract_class = {key: 'daily_abstract'}
                        setattr(attributes, '_attrs', abstract_class)
                    if attrs[key] == 'path-wrapper clearfix':
                        note_class = {key: 'daily_note'}
                        setattr(attributes, '_attrs', note_class)
                    if attrs[key] == 'path-mains':
                        path_class = {key: 'main_path'}
                        setattr(attributes, '_attrs', path_class)
                    # 如果class == html-content,改成note-content
                    if attrs[key] == 'html-content':
                        html_class = {key: 'note-content'}
                        setattr(attributes, '_attrs', html_class)

        # 对html-content中的部分进行处理
        # notes-photo-description-br的class属性去掉，其实是br最多只有class属性
        if qname == 'br':
            for key, value in attrs.items():
                if key[1] == 'class':
                    if attrs[key] == 'notes-photo-description-br':
                        setattr(attributes, '_attrs', {})

        # span元素的属性全部去掉
        if qname == 'span':
            setattr(attributes, '_attrs', {})

        # img元素只留下class属性和data-url
        if qname == 'img':
            img_attrs = {}
            for key, value in attrs.items():
                if key[1] == 'class':
                    img_attrs[key] = attrs[key]
                if key[1] == 'data-purl':
                    img_attrs[(None, 'img-purl')] = attrs[key]
            setattr(attributes, '_attrs', img_attrs)

        sax.ElementTreeContentHandler.startElementNS(self, ns_name, qname, attributes)

def get_html(data):
    from lxml import etree
    import lxml.sax

    # if not hasattr(body_list, '__iter__'):
    #     body_list = [body_list]

    # 用来存放处理好后的contents
    proc_contents = []

    # data为单条的Document,只会处理共中的raw_data和contents
    # 将原数据中的raw_data丢弃，只在原始collection中存储即可
    data.pop('raw_data')

    contents = data['contents']
    # 取出data中每一楼的内容
    for floor in contents:
        handler = PostHandler()
        # 每一楼的floor_id和floor_contents
        floor_id = floor['floor_id']
        floor_contents = floor['floor_contents']

        # 因为我们要把清洗后的数据用html渲染，所以清洗时必须用HTML的Parser
        tree = etree.fromstring(floor_contents, parser=etree.HTMLParser())
        div = tree.xpath("//div[@class='floor-content']")[0]
        # 删除<p class = 'post-author'...元素
        del_p = div.xpath("//p[@class='post-author']")[0]
        del_p.getparent().remove(del_p)
        # 生成新的ElementTree
        lxml.sax.saxify(div, handler)
        # 因为原数据为有序，故新生成的list中的内容也为有序
        proc_contents.append({'floor_id': floor_id,
                              'contents': etree.tostring(handler.etree, encoding='utf-8')})

    # 将原数据更新为处理好后的新数据
    data['contents'] = proc_contents

    return data

def SaveProcItem(proc_data):
    # 建立到TravelNote的连接，直接将数据写入DB
    coll = NewMongoDB('localhost', 27017, 'baidu-note', 'TravelNote')
    query = {'note_id': proc_data['note_id']}
    # 将具有同一note_id的floor信息进行合并
    coll.update(query, {'$set': proc_data}, upsert=True)

# 对于修改后的item，楼层内容应该是有序的，无需要再次排序
# # 对楼层内容的列表，按照楼层id进行排序
# def sort_contents(proc_list):
#     tmp_list = [x['floor_id'] for x in proc_list]
#     sort_id = sorted(tmp_list, key=lambda x: int(re.search('(\d+)', x).group()))
#     # 按照sort_id对floor_contents进行排序
#     sort_list = [z for y in sort_id for z in proc_list if z['floor_id'] == y]
#     return sort_list

if __name__ == "__main__":
    # 建立到BaiduNoteItem的coll,取其中的数据按需求进行清洗
    coll = NewMongoDB('localhost', 27017, 'baidu-note', 'BaiduNoteItem')
    data_list = coll.find()
    # 若直接生成处理好的数据列表太大，需要对每条Document单独处理
    for data in data_list:
        proc_data = get_html(data)
        # 向DB中插入处理好的contents
        SaveProcItem(proc_data)





