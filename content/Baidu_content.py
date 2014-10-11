# -*- coding: UTF-8 -*-
__author__ = 'wdx'
import re
import time
import lxml.html as html

import pymongo
import time
import datetime
import json


def connect_db():
    client=pymongo.MongoClient('localhost',27027)
    client1=pymongo.MongoClient('localhost',27017)
    db=client.raw_data
    db.authenticate('crawler','the4&flattop')
    collection=db.BaiduNote
    list=collection.find()

    db1=client1.plan

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
    new_part = {
                'id':part['_id'],
                'title': part['title'],
                'authorName': part['uname'],
                'authorAvatar':None,
                'publishDate':None,
                'favorCnt':part['recommend_count'],
                'commentCnt': part['common_posts_count'],
                'viewCnt': int(part['view_count']),
                'costLower':None,
                'costUpper':None,
                'costNorm':None,                            #旅行开支
                'days':None,
                'fromLoc': None,
                'toLoc': None,
                'summary':None,
                'contents': list_data,
                'startDate':None,
                'endDate':None,
                'source':'baidu',
                'sourceUrl': part['url'],
                'elite':False
                }
    if 'avatar_small' in part:
        if  part['avatar_small']:
            avatar_small='himg.bdimg.com/sys/portrait/item/%s.jpg' % part['avatar_small']
            new_part['authorAvatar']=avatar_small


    if 'create_time' in part:
        x = time.localtime(int(part['create_time']))
        publishDate=time.strftime('%Y-%m-%d',x)
        publishDate_v=re.split('[-]',publishDate)
        new_part['publishDate']=datetime.datetime(int(publishDate_v[0]),int(publishDate_v[1]),int(publishDate_v[2]))


    if 'lower_cost' in part:                 #最低价格
        new_part['costLower'] = part['lower_cost']
        if new_part['costLower'] == 0:
            new_part['costLower']=None

    if 'upper_cost' in part:
        new_part['costUpper'] = part['upper_cost']
        if new_part['costUpper'] == 0:
            new_part['costUpper']=None

    if 'days' in part:                        #花费时间
        new_part['days']=int(part['days'])
        if new_part['days']==0:
            new_part['days']=None

    if 'departure' in part:                   #出发地
        new_part['fromLoc']=part['departure']  #_from string

    if 'destinations' in part:                #目的地
        new_part['toLoc']=part['destinations'] #_to string

    if 'content' in part:
        new_part['summary']=part['content']

    if 'start_time' in part:
        x = time.localtime(int(part['start_time']))
        startDate=time.strftime('%Y-%m-%d',x)
        startDate_v=re.split('[-]',startDate)
        new_part['startDate']=datetime.datetime(int(startDate_v[0]),int(startDate_v[1]),int(startDate_v[2]))

    elite = part['is_good'] + part['is_praised'] + int(part['is_set_guide'])
    if elite>0:
        new_part['elite']=True
    else:
        new_part['elite']=False





    #test_part=json.loads(test_part)
    db.travelNote.save(new_part)
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

