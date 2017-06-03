# -*- coding: utf-8 -*-

from configparser import ConfigParser
import pymssql

"""
Data Fetcher for qq information
"""


class DataFetcher:
    def __init__(self):
        conf = ConfigParser()
        conf.read('qqweb.config')
        self.host = str(conf.get('db_config', 'db_host')).strip()
        self.user = str(conf.get('db_config', 'db_user')).strip()
        self.pwd = str(conf.get('db_config', 'db_pwd')).strip()

    def __connect(self, db):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not db:
            raise (NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=db, charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def get_users_by_group(self, group):
        """
        Get group users by group id
        :param group:
        :return:
        """
        # 1234567
        group_seg = 100
        group_prefix = int(group[:-5])
        db_prefix = int(group_prefix / group_seg) + 1
        db_name = 'GroupData' + str(db_prefix)
        cur = self.__connect(db_name)
        table_name = 'Group' + str(int(group_prefix) + 1)
        print("Querying %s->%s" % (db_name, table_name))
        query_sql = 'select distinct QQNum from %s where QunNum = %s' % (table_name, group)
        cur.execute(query_sql)
        res_list = cur.fetchall()
        uin_list = [uin for uin, in res_list]
        # 查询完毕后必须关闭连接
        self.conn.close()
        return uin_list

    def get_group_by_uin(self, uin):
        """
        Get all groups by qq number uin
        :param uin:
        :return:
        """
        cur = self.__connect('QQNumInfo')
        prefix = uin[:3]
        table_name = 'QQNumber' + prefix
        query_sql = 'select distinct QunNum from %s where QQNumber = %s' % (table_name, uin)
        cur.execute(query_sql)
        res_list = cur.fetchall()
        qun_list = [qun for qun, in res_list]
        # 查询完毕后必须关闭连接
        self.conn.close()
        return qun_list


if __name__ == '__main__':
    fetcher = DataFetcher()
    print(fetcher.get_users_by_group('100200024'))
