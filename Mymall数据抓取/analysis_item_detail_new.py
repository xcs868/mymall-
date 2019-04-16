from bs4 import BeautifulSoup
from config import *
import redis
#import sqlop
import re
import json
import time
import html
import asyncio
import pymongo as pm
from multiprocessing import Pool


def idtobigint(strid):
        hexid = ''
        if strid.find('-') != -1:
            arr = strid.split('-')
            hexid += str(arr[0][0:6])[2:]
            hexid += str(arr[4])[4:]
        else:
            hexid += strid[0:6]
            hexid += strid[-6:]
        print(hexid)
        return int(hexid, 16)

#tableName='adn_mymall_item'
#def start():
red = redis.Redis(host=redisC['host'], port=redisC['port'], password=redisC['password'], db=redisC['db'], decode_responses=True)
redD = redis.Redis(host=redisD['host'], port=redisD['port'], password=redisD['password'], db=redisD['db'])
#db = sqlop.sqlOp()

# 获取连接
client = pm.MongoClient('172.20.2.11', 27017)  # 端口号是数值型
# 连接数据库
mdb = client.mymall
mdb.authenticate("mymall", "UhUh'OO~.^")
# 获取集合
stb = mdb.item

def start():
    #await db.initConnect()
    while True:
        item_data = {}
        add = {}
        ret = red.spop('item_source_data')
        if not ret:
            print('等待数据到来')
            time.sleep(1)
            continue
        try:
            data = re.findall('let var_filter = (.*?);',ret)
        except:
            item_data = False
        if not data:
            continue
        try:
            item_data = json.loads(data[0])
        except Exception as e:
            print(e, '错误')
            continue
        if item_data:
            if isinstance(item_data, list):
                print(item_data)
            try:
                for attr in item_data.values():
                    data = []
                    if isinstance(attr, dict):
                        data = list(attr.values())[0]
                    else:
                        data = attr[0]
                    if isinstance(data, dict):
                        product_data = list(data.values())
                    else:
                        product_data = data
                    break
            except Exception as e:
                print(e, data)
                return
            # 打折前价格
            add['old_price'] = int(product_data[0]['price'])
            #当前价格
            add['price'] = int(product_data[0]['discountPrice'])
            #折扣
            add['discount'] = (add['old_price'] - add['price']) / add['old_price']*100
            #listing 评分
            listing_star = data = re.findall('<span class="mobile-sale-rating">(.*?)</span>',ret)
            if not listing_star[0]:
                add['listing_star'] = 0 
            else:
                add['listing_star'] = float(listing_star[0])
            products = re.findall('\'products\': ([\s\S]*?)\}\n',ret)
            try:
                product = eval(re.sub(r'\s+', ' ', products[0]))[0]
            except Exception as e:
                print(e, products[0])
                continue
            cat_info = product['category'].split('/')
            #类目
            if len(cat_info)>2:
                add['catid'] = cat_info[0]
                add['secid'] = cat_info[1]
                add['thrid'] = cat_info[2]
            elif len(cat_info)>1:
                add['catid'] = cat_info[0]
                add['secid'] = cat_info[1]
                add['thrid'] = ''
            else:
                add['catid'] = cat_info[0]
                add['secid'] = ''
                add['thrid'] = ''    
            add['ItemID']= product['id']
            add['id']=idtobigint(product['id'])
            #店铺名 
            shopname = re.findall('<div class="cell-title">(.*?)</div>',ret)
            add['shopname'] = shopname[0]
            #标题
            add['title'] = product['name']
            #售出单量
            sales = re.findall(r'<div class="number-orders">(.*?)</div>',ret)
            add['salenum'] = int(sales[0].split(' ')[0])
            #评论数量
            comment = re.findall(r'<div class="product-tab-link active">(.*?)</div>',ret)
            comment_num = re.findall(r"\d+\.?\d*",comment[0])
            add['comment_num'] = int(comment_num[0])
        else:
            try:
                products = re.findall('\'products\': ([\s\S]*?)\}\n',ret)
                try:
                    product = eval(re.sub(r'\s+', ' ', products[0]))[0]
                except Exception as e:
                    print(e, products[0])
                    continue
                cat_info = product['category'].split('/')
                 #类目
                if len(cat_info)>2:
                    add['catid'] = cat_info[0]
                    add['secid'] = cat_info[1]
                    add['thrid'] = cat_info[2]
                elif len(cat_info)>1:
                    add['catid'] = cat_info[0]
                    add['secid'] = cat_info[1]
                    add['thrid'] = ''
                else:
                    add['catid'] = cat_info[0]
                    add['secid'] = ''
                    add['thrid'] = ''        
                add['ItemID']= product['id']
                add['id']=idtobigint(product['id'])
                #标题
                add['title'] = product['name']
                #价格
                add['price'] = int(product['price'])
                #正则匹配需要的数据所在的字符串    
                datastr = re.findall('<ul class="stars-rating">([\s\S]*?)<ul class="stars-rating">',ret)     
                #折扣前价格   700  #折扣
                old_price_text = re.findall('<p class="old-price">(.*?)</p>',datastr[0])
                if not old_price_text:
                    add['old_price'] = 0 
                    add['discount']=0
                else:    
                    oldprice_str = re.findall(r"\d+\.?\d*",old_price_text[0])
                    add['old_price'] = int(oldprice_str[0])
                    add['discount'] = (add['old_price'] - add['price']) / add['old_price']*100
                #listing 评分  467
                listing_star = re.findall('<span class="mobile-sale-rating">(.*?)</span>',datastr[0])
                if not listing_star[0]:
                    add['listing_star'] = 0 
                else:
                    add['listing_star'] = float(listing_star[0]) 
                #店铺名  551
                shopname = re.findall('<div class="cell-title">(.*?)</div>',datastr[0])
                add['shopname'] = shopname[0]
                #售出单量  469
                sales = re.findall('<div class="number-orders">(.*?)</div>',datastr[0])
                salenum = re.findall(r"\d+\.?\d*",sales[0])
                add['salenum'] = int(salenum[0])
                 #评论数量 
                comment = re.findall('<div class="product-tab-link active">(.*?)</div>',datastr[0])
                comment_num = re.findall(r"\d+\.?\d*",comment[0])
                add['comment_num'] = int(comment_num[0])
            except Exception as a:
                print(a, '错误')       
        #存入数据表
        #await db.M('adn_mymall_item').addOrUpdate(add,'id')
        stb.update({'ItemID':add['ItemID']}, {'$set':add}, upsert=True)
        addinfo={}
        addinfo['name']='mymall'
        addinfo['data']=add
        json_info=json.dumps(addinfo)
        #print(json_info)
        #格式存入redis
        redD.sadd(redisD['hot_his_data'],json_info)



p = Pool(2)
for i in range(2):
    p.apply_async(start)
p.close()
p.join()
