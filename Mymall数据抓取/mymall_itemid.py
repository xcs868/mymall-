import requests
import datetime
import json
import time
import sqlop
import redis
import getcat
from config import *
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

red = redis.Redis(host=redisC['host'], port=redisC['port'], password=redisC['password'], db=redisC['db'])
db=getcat.sqlOp()
w = red.hgetall(redisC['mymall_allitem'])
for itemid, catid in w.items():
    print(itemid)
    add={}
    add['id']=idtobigint(itemid.decode())
    add['ItemID']=itemid.decode()
    db.M('items').addOrUpdate(add, 'id')







