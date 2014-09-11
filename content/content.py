__author__ = 'wdx'
import pymongo
import re
import lxml.html as html
import time

def connect_db():
    client=pymongo.MongoClient('dev.lvxingpai.cn',27019)
    db=client.raw_notes
    collection=db.BaiduNote
    list_cot=collection.find()
    return list_cot,db

def zhengze(part,db):
    new_part={}
    content_m=part['contents']
    #part_u=part.decode('gb2312')
    content=content_m[0].replace('<p','<img><p')
    content=content.replace('<div','<img><div')
    zz=re.compile(ur"<(?!img)[\s\S][^>]*>")#|(http://baike.baidu.com/view/)[0-9]*\.(html|htm)|(http://lvyou.baidu.com/notes/)[0-9a-z]*")
    content=zz.sub('',content)
    content_list=re.split('[<>]',content)

    dic={}
    list_data=[]

    for i in range(len(content_list)):
        content_m=content_list[i].count('img ')
        if content_m>0:
            part_c=re.compile(r'http[\s\S][^"]*.jpg')
            part1=part_c.search(content_list[i].strip())
            dic={'data':part1.group(),'type':'image'}
        elif (content_list[i].strip()=='')|(content_list[i].strip()=='img'):
            continue
        else:
            dic={'data':content_list[i].strip(),'type':'text'}
        list_data.append(dic)
        dic={}

    new_part={'authorId':part['authorId'],'from':part['fromLoc'],
              'to':part['toLoc'],'timeCost':part['timeCost'],
              'authorName':part['authorName'],'viewCnt':part['viewCnt'],'commentCnt':part['replyCnt'],
              'title':part['title'],'contents':list_data,'url':part['url'],'cost':None,'startTime':None}
    try:
        new_part['cost']=part['cost']
    except:
        new_part['cost']=None

    startTime=part['startTime']

    part_s=re.compile(r'[^0-9]*')
    startTime=part_s.sub('',startTime)
    time_m=time.strptime(startTime,'%Y%m')
    time_m=int(time.mktime(time_m))
    new_part['startTime']=time_m
    db.New_BaiduNote.save(new_part)

    return


def main():
    part,db=connect_db()
    for m in part:
        zhengze(m,db)

if __name__ == "__main__":

    main()

