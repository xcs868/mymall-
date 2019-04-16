import urllib.request
from bs4 import BeautifulSoup
from config import *
import redis

red = redis.Redis(host=redisC['host'], port=redisC['port'], password=redisC['password'], db=redisC['db'])
def Html(url):
    html = urllib.request.urlopen(url)
    return html.read().decode('utf-8')

FIELDS = Html(r'https://pandao.ru/category/zhenskaya-odezhda?priceTo=0.05&sort=price_desc')
soup = BeautifulSoup(FIELDS)
info = soup.find_all(attrs={'class':'filters-category-last'}) 
add={}
for a in info:
    add['category']=a['data-id']
    add['category_path']=a['data-path']
    print(add)
    key=add['category']
    value=add['category_path']
    red.hset(redisC['mymall_category'], key, value)
# text= td.attrs('data-id')
# print(text)

    