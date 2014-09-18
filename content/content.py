__author__ = 'wdx'
import pymongo
import re
import lxml.html as html


import time
import json


def connect_db():
    client=pymongo.MongoClient('dev.lvxingpai.cn',27019)
    db=client.raw_notes
    collection=db.BaiduNote

    list=collection.find()
    x=list[1820]['contents']
    #print x[0]
    return x

def zhengze(part):
    #part_u=part.decode('gb2312')
    content=part[0].replace('<p','<img><p')
    content=content.replace('<div','<img><div')
    print content
    zz=re.compile(ur"<(?!img)[\s\S][^>]*>")#|(http://baike.baidu.com/view/)[0-9]*\.(html|htm)|(http://lvyou.baidu.com/notes/)[0-9a-z]*")
    content=zz.sub('',content)
    print content
    #content=content.replace('<','\n<')
    #content=content.replace('>','>\n')
    #content=content.replace('\n\n','\n')
    content_list=re.split('[<>]',content)
    #print content_list
    dic={}
    list=[]

    list_cot=collection.find()
    return list_cot,db

def zhengze(part,db):
    #new_part={}
    content_m=part['contents']
    #part_u=part.decode('gb2312')
    if not content_m:
        return
    content=content_m[0].replace('<p','<img><p')
    #content=content.replace('%','i')
    content=content.replace('<div','<img><div')
    zz=re.compile(ur"<(?!img)[\s\S][^>]*>")#|(http://baike.baidu.com/view/)[0-9]*\.(html|htm)|(http://lvyou.baidu.com/notes/)[0-9a-z]*")
    content=zz.sub('',content)
    content_list=re.split('[<>]',content)

    list_data=[]


    for i in range(len(content_list)):
        content_m=content_list[i].count('img ')
        if content_m>0:

            part=re.compile('http[\s\S][^"]*.jpg')
            part1=part.search(content_list[i])
            dic={'data':part1.group(),'type':'image'}
        elif (content_list[i]=='')|(content_list[i]=='img'):
            continue
        else:
            dic={'data':content_list[i],'type':'text'}
        list.append(dic)
        dic={}

    print content

    print list


    part=connect_db()
    if part:
        zhengze(part)
    print

    part_c=re.compile(r'http[\s\S][^"]*.jpg')
    part1=part_c.search(content_list[i].strip())
    if part1:
        list_data.append(part1.group())
    elif (content_list[i].strip()=='')|(content_list[i].strip()=='img'):
        continue
    else:
        list_data.append(content_list[i].strip())


    #print part
    test_part={"authorId":part["authorId"]}
    new_part={'authorId':part['authorId'],'from':None,
              'to':None,'timeCost':None,
              'authorName':part['authorName'],'viewCnt':part['viewCnt'],'commentCnt':part['replyCnt'],
              'title':part['title'],'contents':list_data,'url':part['url'],'cost':None,'startTime':None}
    try:
        new_part['cost']=part['cost']
    except:
        new_part['cost']=None
    try:
        new_part['from']=part['summary']['departure']
    except:
        new_part['from']=None
    try:
        new_part['to']=part['summary']['destinations']
        if new_part['to']:
            if (new_part['to'][0]==''):
                new_part['to']=part['summary']['places']
    except:
        new_part['to']=None

    try:
        new_part['startTime']=int(part['summary']['start_time'])
    except:
        startTime=part['startTime']
        part_s=re.compile(r'[^0-9]*')
        startTime=part_s.sub('',startTime)
        time_m=time.strptime(startTime,'%Y%m')
        time_m=int(time.mktime(time_m))
        new_part['startTime']=time_m

    try:
        new_part['timeCost']=part['timeCost']
    except:
        new_part['timeCost']=None

    #test_part=json.loads(test_part)
    db.New_BaiduNote.insert(new_part)
    return


def main():
    part,db=connect_db()
    i=0
    for m in range(3537,17747):
        zhengze(part[m],db)
        i=i+1
        print i

if __name__ == "__main__":

    main()

