# encoding=utf8
import pymysql as ps
import pymysql
import logging
import configparser
import datetime


class MysqlHelper:
    def __init__(self, conf='default'):
        # 读取mysql配置
        # 读取配置
        cf = configparser.ConfigParser()
        cf.read('./config/mysql.ini')
        self.host = cf.get(conf, 'hostname')
        self.database = cf.get(conf, 'database')
        self.user = cf.get(conf, 'username')
        self.password = cf.get(conf, 'password')
        self.port = cf.get(conf, 'hostport')
        self.charset = cf.get(conf, 'charset')

        # 添加的set语句
        self.set = ""
        # 分页信息
        self.limit = ""
        # 数据库实例
        self.db = None
        # 数据库连接
        self.curs = None
        # 默认自动提交
        self.commit_state = True

    # 数据库连接
    def open(self):
        if self.db is None:
            # 创建数据库实例
            self.db = ps.connect(host=self.host, user=self.user, password=self.password, database=self.database,
                                 charset=self.charset, cursorclass=pymysql.cursors.DictCursor)

        if self.curs is None:
            # 建立连接
            self.curs = self.db.cursor()

    # 数据库关闭
    def close(self):
        if not self.curs is None:
            self.curs.close()

        if not self.db is None:
            self.db.close()

    # 数据增删改
    def execute(self, sql, params=None, lastId=False):
        self.open()
        try:
            num = self.curs.execute(sql, params)
            if self.commit_state:
                print("commit")
                self.db.commit()
            if lastId:
                return self.curs.lastrowid
            return num
        except Exception as result:
            logging.error(result)
            logging.error(sql)
            logging.error(params)
            # self.db.rollback()
            raise result
        finally:
            self.clearMysql()

    # 数据查询
    def find(self, sql, params=None):
        self.open()
        try:
            result = self.curs.execute(sql, params)
            return result
        except Exception as result:
            logging.error(result)
            logging.error(params)
            raise result

    # 数据查询
    def query(self, sql, params=None):
        self.open()
        try:
            self.curs.execute(sql, params)
            result = self.curs.fetchall()
            return result
        except Exception as result:
            logging.error(result)
            logging.error(params)
            raise result
        finally:
            self.clearMysql()

    def addSet(self, set):
        set = "," + set
        self.set = set

    def getSet(self, data={}):
        '''
        select * from a where cc = 0 and dd=0
        :param data: 条件字典
        :return:sql条件 string
        '''
        # 参数值
        param = []
        set_sql = " set "

        if data:
            for key in data:
                if len(param) != 0:
                    set_sql += ", "
                set_sql += key
                set_sql += "="
                set_sql += "%s "
                param.append(data[key])
        else:
            return '', []
        return set_sql, param

    def getWhere(self, where={}):
        '''
        select * from a where cc = 0 and dd=0
        :param where: 条件字典
        :return:sql条件 string
        '''
        # 参数值
        param = []
        where_sql = " where "

        if where:
            for key in where:
                if len(param) != 0:
                    where_sql += "and "
                where_sql += key
                where_sql += "="
                where_sql += "%s "
                param.append(where[key])
        else:
            return '', []

        return where_sql, param

    def count(self, table_name, where={}):
        '''
       :param where:条件     字典
       :param table_name:表名    string
       :return: int
       '''
        where, param = self.getWhere(where)
        # 处理条件
        sql = "select count(1) count from " + table_name + where
        data = self.query(sql, param)
        return data[0]['count']

    def row(self, table_name, filed=['*'], where={}):
        '''
        :param filed:查询字段   list
        :param where:条件     字典
        :param table_name:表名    string
        :return:
        '''
        # 处理 filed
        filed = ",".join(filed)
        where, param = self.getWhere(where)
        # 处理条件
        sql = "select " + filed + " from " + table_name + where + " LIMIT 1"
        data = self.query(sql, param)
        return data[0]

    def select(self, table_name, filed=['*'], where={}):
        '''
        :param filed:查询字段   list
        :param where:条件     字典
        :param table_name:表名    string
        :return:
        '''
        # 处理 filed
        filed = ",".join(filed)
        where, param = self.getWhere(where)
        # 处理条件
        sql = "select " + filed + " from " + table_name + where + self.limit
        data = self.query(sql, param)

        return data

    def update(self, data, where, table_name):
        '''
        更新数据data为字典
        :param data:{'id':12,'name':'fujie'}
        :param where:{'id':12,'name':'fujie'}
        :return:
        '''
        field = []
        str = []
        values = []
        sql = "UPDATE " + table_name
        set, param1 = self.getSet(data)
        where, param2 = self.getWhere(where)
        # 拼接sql
        set += self.set
        sql += set + where
        values.extend(param1)
        values.extend(param2)
        return self.execute(sql, values)

    def insert(self, data, table_name,lastId=True):
        '''
        插入数据data为字典
        :param data:{'id':12,'name':'fujie'}
        :return:
        '''
        field = []
        str = []
        values = []
        sql = "INSERT INTO " + table_name
        for key in data:
            field.append(key)
            str.append("%s")
            values.append(data[key])
        # + "(id,name) VALUES(1,'ff');"
        sql += "(" + ",".join(field) + ") VALUES(" + ",".join(str) + ");"
        return self.execute(sql, values, lastId)

    def updateOrInsert(self, data, where, table_name):
        '''
        更新数据或插入数据(数据不存在)
        :param data:{'id':12,'name':'fujie'}
        :param where:{'id':12,'name':'fujie'}
        :return:
        '''
        result = ""
        count = self.count(table_name, where)
        if count == 0:
            result = self.insert(data, table_name)
        else:
            result = self.update(data, where, table_name)
        return result

    def begin_transaction(self):
        self.commit_state = False

    def commit(self):
        '''
        提交事务 并且设置为自动提交
        :return:
        '''
        self.db.commit()
        self.commit_state = True

    def rollback(self):
        self.db.rollback()

    def setLimit(self, page=1, limit=10):
        s_limit = (page - 1) * limit
        # print type(str(s_limit))
        self.limit = "LIMIT " + bytes(s_limit) + "," + bytes(limit)

    def clearMysql(self):
        self.limit = ""
        self.set = ""


if __name__ == '__main__':
    mysql = MysqlHelper()

    # query_sql = """
    #             SELECT
    #             id,path
    #         FROM
    #             game_img
    #         WHERE
    #             type='img'
    #         AND is_del = 1
    #         ;
    #             """;
    # select = [1, 2]
    # update = mysql.update({"is_follow": 1}, {'unionid': "jifashfios"}, "user_weichat")
    # data = mysql.select("xd_auth_group")
    # print(data)
    # print(mysql.insert({'proid': 1, 'title': 'Seashell', 'type': "3"},"xd_product_norms_copy1"))

    mysql.begin_transaction()
    datas = [
        {"age": 7},
        {"age": 7},
        {"age": 7},
        {"age": 7},
        {"age": 7},
        {"age": "dassa"},
    ]

    for data in datas:
        mysql.insert(data, "test")
    mysql.rollback()
    # print type(update)
    # print update
    # data = {
    #     "flag": 3,
    #     "pv": 54564,
    #     "type": "action",
    #     "updated": datetime.datetime.now(),
    # }
    #
    # where = {
    #     "flag": 3,
    #     "type": "action",
    # }
    # result = mysql.updateOrInsert(data, where, "s_goods_pv")
    #
    # print result
