__author__ = 'wdx'
import pymongo
import re
import lxml.html as html

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