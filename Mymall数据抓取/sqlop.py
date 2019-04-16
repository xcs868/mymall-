# 连接数据库

import sys
import time
import aiomysql
import asyncio
import pymysql

class sqlOp:
    
    ip = "172.20.**.**"
    User = "root"
    Pwd = "**"
    Db = "mymall"
    Port = 8066
    tableName = ""        # 表名
    whereStr = ""        # 条件
    fieldStr = "*"        # 查询的字段
    limitStr = ""        # limit
    orderStr = ""        # order
    groupStr = ""        # group
    
    def __init__(self):
        #self.initConnect()    # 初始化数据库连接
        self.sqlInit()        # 初始化字符串
    
    async def initConnect(self):
        try:
            self.pool.ping()
        except:    
            while 1:
                # 打开数据库连接
                try:
                    self.pool = await aiomysql.create_pool(host=self.ip, user=self.User, password=self.Pwd, db=self.Db, port=self.Port, charset = 'utf8mb4', autocommit=True, cursorclass=aiomysql.cursors.DictCursor)
                    break
                except:
                    print('重新连接数据库~~~')
                    time.sleep(10)
    
    def sqlInit(self):
        self.tableName = ""        # 表名
        self.whereStr = ""        # 条件
        self.fieldStr = "*"        # 查询的字段
        self.limitStr = ""        # limit
        self.orderStr = ""        # order
        self.groupStr = ""        # group
    
    def M(self, tableName):
        self.tableName = tableName
        return self
        
    def where(self, whereStr):
        if whereStr == "":
            self.whereStr = whereStr
        else:
            self.whereStr = "where "+whereStr
        return self
        
    def field(self, fieldStr):
        if fieldStr != "":
            self.fieldStr = fieldStr
        return self
    
    def order(self, orderStr):
        if orderStr != "":
            self.orderStr = "order by "+orderStr
        return self

    def group(self, groupStr):
        if groupStr != "":
            self.groupStr = "group by "+groupStr
        return self
    
    def limit(self, limitStr):
        if limitStr != "":
            self.limitStr = 'limit '+limitStr
        return self
        
    async def find(self):
        sql = "select %s from %s %s %s %s %s" % (self.fieldStr, self.tableName, self.whereStr, self.orderStr, self.groupStr, "limit 0,1")
        
        ret = await self.query(sql)    # 执行SQL语句
        self.sqlInit()        # 初始化字符串
        if ret:
            if len(ret[1]):
                # 返回查询到的data
                return ret[1][0]
            else:
                return ret[0]
        else:
            return ret
        
    async def select(self):
        sql = "select %s from %s %s %s %s %s" % (self.fieldStr, self.tableName, self.whereStr, self.groupStr, self.orderStr, self.limitStr)
        #print(sql)
        ret = await self.query(sql)    # 执行SQL语句
        self.sqlInit()        # 初始化字符串
        if ret:
            if len(ret[1]):
                # 返回查询到的data
                return ret[1]
            else:
                return ret[0]
        else:
            return ret
    
    async def count(self):
        sql = "select count(*) from %s %s" % (self.tableName, self.whereStr)
        
        ret = await self.query(sql)    # 执行SQL语句
        self.sqlInit()        # 初始化字符串
        if ret:
            if len(ret[1]):
                # 返回查询到的data
                return ret[1][0][0]
            else:
                return ret[0]
        else:
            return ret
        
    async def getTableAll(self, tableName):
        sql = "select * from " + tableName
        
        # 执行SQL语句
        ret = await self.query(sql)
        if ret:
            if len(ret[1]):
                # 返回查询到的data
                return ret[1]
            else:
                return 0
        else:
            return ret
    
    async def query(self, sql):        # sql语句执行
        #print(sql)
        ret = {}
        try:
            async with self.pool.acquire() as conn:
                async with conn.cursor() as cur:
                    # 使用execute方法执行SQL语句
                    ret[0] = await cur.execute(sql)
                    # 提交到数据库执行
                    #self.db.commit()
                    ret[1] = await cur.fetchall()
                    #print(ret)
        except (AttributeError, aiomysql.OperationalError) as e:
            await self.initConnect()
            print("Error:%s %s SQL:[%s]" % (str(self.tableName),str(e.args),sql))
            ret = False
            # 发生错误时回滚
            #self.db.rollback()
        except aiomysql.Error as e:
            await self.initConnect()
            print("Error:%s %s SQL:[%s]" % (str(self.tableName),str(e.args),sql))
            ret = False
            #self.db.rollback()
        return ret
        
    async def add(self, data = {}):
        field = []
        values = []
        for key in data:
            field.append("`"+key+"`")
            values.append("'"+str(pymysql.escape_string(str(data[key])))+"'")
            
        sql = 'INSERT INTO `%s` (%s) VALUES (%s)' % (self.tableName, ','.join(field), ','.join(values))
        #print(sql)
        ret = await self.query(sql)    # sql语句执行
        self.sqlInit()        # 初始化字符串
        if ret:
            # 返回查询到的data
            #return self.cursor.lastrowid
            return 1
        else:
            return ret
    async def addOrUpdate(self, data = {}, primary_key = ''):
        field = []
        values = []
        set = []
        for key in data:
            field.append("`"+key+"`")
            values.append("\""+pymysql.escape_string(str(data[key]).replace("'", "\\'"))+"\"")
            if key == primary_key:
                continue
            set.append(key+"=\""+pymysql.escape_string(str(data[key]))+"\"")
            
        sql = 'INSERT INTO `%s` (%s) VALUES (%s) ON DUPLICATE KEY UPDATE %s' % (self.tableName, ','.join(field), ','.join(values), ','.join(set))
        #print(sql)
        ret = await self.query(sql)    # sql语句执行
        self.sqlInit()        # 初始化字符串
        if ret:
            return ret[0]
        else:
            return ret
        
    async def save(self, save = {}):
        saves = []
        for key in save:
            saves.append(str(key)+"='"+pymysql.escape_string(str(save[key]))+"'")
            
        sql = 'UPDATE %s SET %s %s' % (self.tableName, ','.join(saves), self.whereStr)
        ret = await self.query(sql)    # sql语句执行
        self.sqlInit()        # 初始化字符串
        if ret:
            return ret[0]
        else:
            return ret
    
    async def delete(self):
        sql = 'delete from %s %s' % (self.tableName, self.whereStr)
        
        ret = await self.query(sql)    # sql语句执行
        self.sqlInit()        # 初始化字符串
        if ret:
            return ret[0]
        else:
            return ret

    async def sqlClose(self):
        # 关闭数据库连接
        await self.db.close()
        
#u = sqlOp()
#u.sqlClose()

