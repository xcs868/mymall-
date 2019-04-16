import requests
import datetime
import json
import time
import sqlop
import redis
import hashlib
from config import *
from bs4 import BeautifulSoup

red = redis.Redis(host=redisC['host'], port=redisC['port'], password=redisC['password'], db=redisC['db'])
#db = sqlop.sqlOp()
url = 'https://pandao.ru/ajax/catalog'
w = red.spop(redisC['mymall_catid'])
#w='df70d5e3-8a34-4ed0-92dc-eb747f4e8b49'
page = 0
priceFrom=0
priceTo=10
qid=''
while True:
    print(page)
    if not w:
        break
    category =w 
    print(category)
    print(priceFrom)
    print(priceTo)
    payload = {
    'category':category,
    'page':page,
    'priceFrom':priceFrom,
    "priceTo":priceTo,
    'qid':qid
    }
    headers = {
    'Content-Type' : 'application/x-www-form-urlencoded'
    }
    r = requests.get(url, headers=headers, params=payload,timeout=20)
    info = r.json()
    output=info['output']
    soup = BeautifulSoup('<html>'+output+'</html>',features ='html.parser')
    html = soup.find_all(attrs={'class':'product-item'}) 
    #if not html:
    #    w = red.spop(redisC['mymall_catid'])
    #    page = 0
    #    qid=''
    #    continue
    add={}    
    for a in html:
        add['itemid']=a['data-id']
        print(add['itemid'])
        key=add['itemid']
        value=category
        red.hset(redisC['mymall_allitem'], key, value)   
    left=info['left']
    print(left)
    if left>0 :
        page = page+ 1
        qid=info['qs']['qid']
        w = category
        priceFrom=priceFrom
        priceTo=priceTo
        #if left = 1176:
    else:
        page = 0
        qid=''
        w = category
        priceFrom = priceFrom+ 10
        priceTo = priceTo+ 10
    if  priceTo >6900:
        w = red.spop(redisC['mymall_catid'])
        priceFrom = 0
        priceTo = 10
        page = 0
        qid=''
        
#笔记
#<li class="main-menu-opened-container">  拿取标签内所有名称  大类目
#class="filters-category-last"  拿取所有类目id  路径  去重存储  1681个
#发送请求 获取商品ID 页码数每次加一 剩余数量>=1176 价格缩小范围查找 去重  获取qid  传入qid 获取HTML 得到信息
#跳转 获取界面 
#加入数据表   mymall_catid  mymall_item  

