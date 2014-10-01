# -*- coding: UTF-8 -*-
__author__ = 'wdx'
import re
import lxml.html as html

import pymongo
import time
import json


def connect_db():
    client=pymongo.MongoClient('localhost',27017)
    db=client.raw_data
    collection=db.BaiduNote
    list=collection.find()

    db1=client.clean_data
    #x=list[1820]['contents']
    #print x[0]
    return list,db1

content_v=[]

def zhengze(part,db):
    #new_part={}
    content_list=[]
    content_m=part['contents']
    #part_u=part.decode('gb2312')
    if not content_m:
        return
    for i in range(len(content_m)):
        content=content_m[i].replace('<p','<img><p')
        #content=content.replace('%','i')
        content=content.replace('<div','<img><div')
        zz=re.compile(ur"<(?!img)[\s\S][^>]*>")#|(http://baike.baidu.com/view/)[0-9]*\.(html|htm)|(http://lvyou.baidu.com/notes/)[0-9a-z]*")
        content=zz.sub('',content)
        content_v=re.split('[<>]',content)
        content_list.extend(content_v)

    list_data = []


    for i in range(len(content_list)):
        part_c=re.compile(r'http[\s\S][^"]*.jpg')
        part1=part_c.search(content_list[i].strip())
        if part1:
            list_data.append(part1.group())
        elif (content_list[i].strip()=='')|(content_list[i].strip()=='img'):
            pass
        else:
            list_data.append(content_list[i].strip())


    #print part
    #test_part = {"authorId": part["authorId"]}
    new_part = {'authorId': part['uid'], '_from': None,'_to': None,'places':None,'lowerCost':None,'timeCost': None,
                 'praised':part['is_praised'],'good':part['is_good'],'guide':int(part['is_set_guide']),
                'authorName': part['uname'], 'viewCnt': int(part['view_count']), 'commentCnt': part['common_posts_count'],
                'title': part['title'], 'contents': list_data, 'url': part['url'], 'startTime': None,'month':None
                }



    if 'departure' in part:                   #出发地
        new_part['_from']=part['departure']

    if 'destinations' in part:                #目的地
        new_part['_to']=part['destinations']

    if 'places' in part:                      #路线
        new_part['places']=part['places']

    if 'lower_cost' in part:                 #最低价格
        new_part['lowerCost'] = part['lower_cost']
        if new_part['lowerCost'] == 0:
            new_part['lowerCost']=None

    if 'days' in part:                        #花费时间
        new_part['timeCost']=int(part['days'])
        if new_part['timeCost']==0:
            new_part['timeCost']=None

    if 'start_time' in part:      #出发时间
        new_part['startTime'] = int(part['start_time'])

    if 'month' in part:           #旅行月份
        new_part['month']=int(part['month'])



    #test_part=json.loads(test_part)
    db.New_BaiduNote.insert(new_part)
    return


def main():
    part,db=connect_db()
    i=0
    for m in range(1,28614):
        zhengze(part[m],db)
        i=i+1
        print i


if __name__ == "__main__":
    main()

