import requests
import datetime
import json
import time
import sqlop
import redis
from config import *

red = redis.Redis(host=redisC['host'], port=redisC['port'], password=redisC['password'], db=redisC['db'])
w = red.hgetall(redisC['mymall_category'])
for catid, category_name in w.items():
    print(catid)
    red.sadd(redisC['mymall_catid'], catid)
       
                   







