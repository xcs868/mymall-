#!/usr/bin/env python
#encoding=utf-8

import redis
from config import *
import sqlopblock

class Init:

    def __init__(self):
        self.red = redis.Redis(host=redisC['host'], port=redisC['port'], password=redisC['password'], db=redisC['db'])
        self.db = sqlopblock.sqlOp({'ip':'172.20.0.46', 'User':'root', 'Pwd':'HV2018win', 'Db':'Mymall', 'Port':3306})
    
    def start(self):
        total = self.db.M('items').count()
        i = 0
        while True:
            ids = self.db.M('items').limit(str(i)+", 5000").field('ItemID').select()
            if not ids:
                break
            for one in ids:
                i += 1
                print(i, total)
                self.red.sadd(redisC['mymall_itemid'], str(one['ItemID']))

if __name__ == '__main__':
    i = Init()
    i.start()

