# coding=utf-8
__author__ = 'wdx'
import pymongo
from numpy import *
import jieba
import par
import sys
reload(sys)
from math import *
import matplotlib.pyplot as plt
sys.setdefaultencoding("utf-8")

file = '/home/wdx/tags2.txt'
p_mus = 0.18  #博物馆
p_cul = 0.20  #文化博览
p_sec = 0.25  #风景山水
p_res = 0.20  #遗址故居
p_mov = 0.18  #运动娱乐
p_lei = 0.20  #休闲娱乐
p_rel = 0.20  #宗教寺院
p_cus = 0.20  #风土人情
p={'mus':p_mus,'cul':p_cul,'sec':p_sec,'res':p_res,'mov':p_mov,'lei':p_lei,'rel':p_rel,'cus':p_cus}
mus = ['博物馆','博物院']
cul = ['剧院','大学','文化园','美术馆','书馆','画廊','影视城']
sec = ['生态园','水库','保护区','峡谷','瀑布','草原','森林','湿地']
res = ['遗址','故居','墓','旧址','墓群','故里','烈士墓']
mov = ['漂流','雪场']
lei = ['度假村','山庄','度假区','动物园','剧场','游乐场']
rel = ['禅寺','教堂','清真寺','寺塔','佛寺','禅院','大寺']
cus = ['古镇','农庄','古城','民俗村','小镇','侗寨','村落']

def mongo_con():
    mongo_conn = pymongo.Connection('localhost',27017)
    col = mongo_conn.poi.ViewSpot
    parts = col.find()
    return parts,col


def load_tag(file):
    tags=[]
    f=open(file,'r')
    for line in f.readlines():
        if line.strip():
            tags.append(line.strip())
    f.close()
    return tags

def data_set():
    mongo_conn = pymongo.Connection('localhost',27017)
    col = mongo_conn.poi.ViewSpot
    words_list=[]
    vec_mus=[]
    vec_cul=[]
    vec_sec=[]
    vec_res=[]
    vec_mov=[]
    vec_lei=[]
    vec_rel=[]
    vec_cus=[]
    parts = col.find()
    for i in range(1,30740):
        print i
        if 'tags' in parts[i]:
            words_list.append(parts[i]['tags'])
        else:
            words_list.append([])
        words = jieba.cut(parts[i]['name'],cut_all=True)

        for w in words:
            word = w


        if word in mus:#博物馆
            vec_mus.append(1)
        else:
            vec_mus.append(0)

        if word in cul:#文化博览
            vec_cul.append(1)
        else:
            vec_cul.append(0)

        if word in sec:#风景山水
            vec_sec.append(1)
        else:
            vec_sec.append(0)

        if word in res:#遗址故居
            vec_res.append(1)
        else:
            vec_res.append(0)

        if word in mov: #运动娱乐
            vec_mov.append(1)
        else :
            vec_mov.append(0)

        if word in lei:#休闲娱乐
            vec_lei.append(1)
        else:
            vec_lei.append(0)

        if word in rel:#宗教寺院
            vec_rel.append(1)
        else:
            vec_rel.append(0)

        if word in cus:#风土人情
            vec_cus.append(1)
        else:
            vec_cus.append(0)
    vecs={'mus':vec_mus,'cul':vec_cul,'sec':vec_sec,'res':vec_res,'mov':vec_mov,'lei':vec_lei,'rel':vec_rel,'cus':vec_cus}


    return words_list,vecs

def set_word_vec(tags,inputdata):
    vecs=[]

    for words in inputdata:
        vec = [0]*len(tags)
        for word in words:
            if word in tags:
                vec[tags.index(word)] = 1
        vecs.append(vec)
    return vecs

def set_view_vec(tags,inputdata):

    vec = [0]*len(tags)
    for word in inputdata:
        if word in tags:
            vec[tags.index(word)] = 1
    return vec



def train(trainvec,trainclass):
    num_doc = len(trainvec)
    num_word = len(trainvec[0])
    #p_a = sum(trainclass)/float(num_doc)
    p1_mus = zeros(num_word)
    p1_cul = zeros(num_word)
    p1_sec = zeros(num_word)
    p1_res = zeros(num_word)
    p1_mov = zeros(num_word)
    p1_lei = zeros(num_word)
    p1_rel = zeros(num_word)
    p1_cus = zeros(num_word)

    p1_mus_denom = 0.0
    p1_cul_denom = 0.0
    p1_sec_denom = 0.0
    p1_res_denom = 0.0
    p1_mov_denom = 0.0
    p1_lei_denom = 0.0
    p1_rel_denom = 0.0
    p1_cus_denom = 0.0
    for i in range(num_doc):
        if trainclass['mus'][i] == 1:
            p1_mus += trainvec[i]
            p1_mus_denom += sum(trainvec[i])
        elif trainclass['cul'][i] == 1:
            p1_cul += trainvec[i]
            p1_cul_denom += sum(trainvec[i])
        elif trainclass['sec'][i] == 1:
            p1_sec += trainvec[i]
            p1_sec_denom += sum(trainvec[i])
        elif trainclass['res'][i] == 1:
            p1_res += trainvec[i]
            p1_res_denom += sum(trainvec[i])
        elif trainclass['mov'][i] == 1:
            p1_mov += trainvec[i]
            p1_mov_denom += sum(trainvec[i])
        elif trainclass['lei'][i] == 1:
            p1_lei += trainvec[i]
            p1_lei_denom += sum(trainvec[i])
        elif trainclass['rel'][i] == 1:
            p1_rel += trainvec[i]
            p1_rel_denom += sum(trainvec[i])
        elif trainclass['cus'][i] == 1:
            p1_cus += trainvec[i]
            p1_cus_denom += sum(trainvec[i])

    p1_mus_vec = p1_mus/p1_mus_denom
    p1_cul_vec = p1_cul/p1_cul_denom
    p1_sec_vec = p1_sec/p1_sec_denom
    p1_res_vec = p1_res/p1_res_denom
    p1_mov_vec = p1_mov/p1_mov_denom
    p1_lei_vec = p1_lei/p1_lei_denom
    p1_rel_vec = p1_rel/p1_rel_denom
    p1_cus_vec = p1_cus/p1_cus_denom

    p1_vec={'mus':p1_mus_vec,'sec':p1_sec_vec,'cul':p1_cul_vec,'res':p1_res_vec,'mov':p1_mov_vec,'lei':p1_lei_vec,'cus':p1_cus_vec,'rel':p1_rel_vec}
    return p1_vec

def classify(tags,p1_vec,p_a):

    parts,col=mongo_con()
    vec_classify,new_parts=pre_data_set(parts)
    for i in range(1,len(new_parts)):
        vec=set_view_vec(tags,vec_classify[i])
        if not vec:
            continue

        p1_mus = sum(vec*array(p1_vec['mus'])) * p_a['mus']
        p1_cul = sum(vec*array(p1_vec['cul'])) * p_a['cul']
        p1_sec = sum(vec*array(p1_vec['sec'])) * p_a['sec']
        p1_res = sum(vec*array(p1_vec['res'])) * p_a['res']
        p1_mov = sum(vec*array(p1_vec['mov'])) * p_a['mov']
        p1_lei = sum(vec*array(p1_vec['lei'])) * p_a['lei']
        p1_rel = sum(vec*array(p1_vec['rel'])) * p_a['rel']
        p1_cus = sum(vec*array(p1_vec['cus'])) * p_a['cus']

        p1=[p1_mus,p1_cul,p1_sec,p1_res,p1_mov,p1_lei,p1_rel,p1_cus]
        print p1
        p1=sorted(p1)
        if p1[-1] == 0:
            new_parts[i]['type']='其他'
        elif p1[-1] == p1_mus:
            new_parts[i]['type']='博物馆'
        elif p1[-1] == p1_sec:
            new_parts[i]['type']='风景山水'
        elif p1[-1] == p1_res:
            new_parts[i]['type']='遗址故居'
        elif p1[-1] == p1_mov:
            new_parts[i]['type']='运动时尚'
        elif p1[-1] == p1_lei:
            new_parts[i]['type']='休闲娱乐'
        elif p1[-1] == p1_cul:
            new_parts[i]['type']='文化博览'
        elif p1[-1] == p1_cus:
            new_parts[i]['type']='风土人情'
        elif p1[-1] == p1_rel:
            new_parts[i]['type']='宗教寺庙'
        else:
            new_parts[i]['type']='其他'
        col.save(new_parts[i])
        print new_parts[i]['type'],new_parts[i]['name']

def pre_data_set(parts):
    words_list=[]
    new_parts=[]
    type = mus + cul + sec + res + mov + lei + rel + cus
    for i in range(1000):
        print i
        words = jieba.cut(parts[i]['name'],cut_all=True)
        for w in words:
            word = w

        if word in type:
            continue

        if 'tags' in parts[i]:
            words_list.append(parts[i]['tags'])
        else:
            words_list.append([])
        new_parts.append(parts[i])
    return words_list,new_parts


def main():
    tags=load_tag(file)
    #tag_list,type_vecs=data_set()
    #vecs=set_word_vec(tags,tag_list)
    #p1=train(vecs,type_vecs)
    p1=par.p1
    classify(tags,p1,p)
    '''
    p0_sec,p1_sec=train(vecs,type_vecs['sec'])
    p0_cul,p1_cul=train(vecs,type_vecs['cul'])
    p0_res,p1_res=train(vecs,type_vecs['res'])
    p0_mov,p1_mov=train(vecs,type_vecs['mov'])
    p0_lei,p1_lei=train(vecs,type_vecs['lei'])
    p0_cus,p1_cus=train(vecs,type_vecs['cus'])
    '''
    #p1={'mus':p1_mus,'sec':p1_sec,'cul':p1_cul,'res':p1_res,'mov':p1_mov,'lei':p1_lei,'cus':p1_cus}

    #view_vecs=set_word_vec(tags,tag_list)
    #classify(view_vecs,p1,p)

def test():
    tags=load_tag(file)
    tag_list,type_vecs=data_set()
    vecs=set_word_vec(tags,tag_list)
    p1=train(vecs,type_vecs)
    plt.figure(111)
    x = range(1,len(p1['mus'])+1)
    plt.subplot(111)
    plt.plot(x,p1['mus'])
    plt.plot(x,p1['sec'])
    plt.plot(x,p1['cul'])
    plt.plot(x,p1['res'])
    plt.plot(x,p1['mov'])
    plt.plot(x,p1['lei'])
    plt.plot(x,p1['cus'])
    plt.plot(x,p1['rel'])
    plt.show()



def test1():
    tags=load_tag(file)
    tag_list,type_vecs=data_set()
    vecs=set_word_vec(tags,tag_list)
    p1=train(vecs,type_vecs)
    plt.figure(111)
    x = range(1,len(p1['mus'])+1)
    plt.scatter(x,p1['mus'])
    plt.scatter(x,p1['sec'])
    #plt.scatter(x,p1['cul'])
    #plt.scatter(x,p1['res'])
    #plt.scatter(x,p1['mov'])
    #plt.scatter(x,p1['lei'])
    #plt.scatter(x,p1['cus'])
    #plt.scatter(x,p1['rel'])
    plt.show()




if __name__ == '__main__':
    main()
    #test()







