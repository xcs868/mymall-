from bs4 import BeautifulSoup
from config import *
import redis
import re
import json
import time,asyncio
from aiohttp import ClientSession
import base64
import sys
import os

#tableName='adn_mymall_item'

#def start():
red = redis.Redis(host=redisC['host'], port=redisC['port'], password=redisC['password'], db=redisC['db'])
redD = redis.Redis(host=redisD['host'], port=redisD['port'], password=redisD['password'], db=redisD['db'])

async def get(url):
    # print ('start --------url',n)
    async with ClientSession() as session:
        async with session.get(url) as resp:
            return await resp.text()

def restart_program():
    python = sys.executable
    os.execl(python, python, * sys.argv)

async def start():
    for n in range(500):
        #数据改路径 从数据表中拿取
        w = red.spop(redisC['mymall_itemid'])
        if not w:
            print('等待数据到来')
            time.sleep(1)
            continue
        print(w)
        url ='https://pandao.ru/product/'+w.decode()
        #print(url)
        try:
            ret = await get(url)
        except:
            red.sadd(redisC['mymall_itemid'], base64.b64encode(w.decode()))
            continue
        if 'Ошибка 404' in ret:
            print('不存在')
            continue
        while True:
            if red.scard('item_source_data') < 1000:
                red.sadd('item_source_data', ret)
                break
            else:
                print('队列数据已满 等待~~~')
                time.sleep(1)
loop = asyncio.get_event_loop()
tasks=[]
for n in range(500):
    task=asyncio.ensure_future(start())
    #task.add_done_callback(callable)
    tasks.append(task)
loop.run_until_complete(asyncio.wait(tasks))
restart_program()

