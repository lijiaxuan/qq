# -*- coding:utf-8 -*-

import pymssql

import pyreBloom
import redis
import os
from neo4j_helper import GraphDBHelper


class DBHelper:
    def __init__(self, host, user, pwd, db):
        self.host = host
        self.user = user
        self.pwd = pwd
        self.db = db
        self.max_cnt = 1000000000
        self.false_positive_rate = 0.01
        self.default_table_set = {'dtproperties'}
        self.redis_host = 'localhost'
        self.redis_port = 6379
        self.redis_db = 0
        self.user_filter = pyreBloom.pyreBloom('user_filter',
                                               self.max_cnt,
                                               self.false_positive_rate,
                                               host=self.redis_host,
                                               port=self.redis_port,
                                               db=self.redis_db)
        self.graph_helper = GraphDBHelper()

    def get_tables(self):
        """
        Get tables in current database
        :return:
        """
        cur = self.__connect()
        cur.execute("SELECT Name FROM SysObjects Where XType='U' ORDER BY Name")
        res_list = cur.fetchall()
        final_list = list()
        for res, in res_list:
            if res not in self.default_table_set:
                final_list.append(res)
        self.conn.close()
        return final_list

    def append_entities(self):
        """
        Get user list from tables
        :return:
        """
        table_list = self.get_tables()
        write_batch = 100000
        table_list = table_list[:10]
        for table in table_list:
            edge_file_path = os.path.join(self.graph_helper.import_path, 'edge_data.csv')
            edge_file = open(edge_file_path, 'w')
            edge_file.writelines('from,to,nick,age,gender,auth\n')
            write_tag = 0
            print('Appending entities from table %s...' % table)
            edges = self.get_edges(table)
            print('Loading data completed...')
            cur_qq_list = list()
            cur_group_set = set()
            # QQNum, Nick, Age, Gender, Auth, QunNum
            for qq_num, nick, age, gender, auth, qun_num in edges:
                write_tag += 1
                if write_tag % write_batch == 0:
                    print("Current batch %d" % (int(write_tag / write_batch),))
                qq_num = str(qq_num)
                qun_num = str(qun_num)
                if not self.user_filter.contains(qq_num):
                    self.user_filter.add(qq_num)
                    cur_qq_list.append(qq_num)
                if qun_num not in cur_group_set:
                    cur_group_set.add(qun_num)
                cur_line = ','.join([qq_num, qun_num, nick.encode('utf-8'), str(age), str(gender), str(auth)]) + '\n'
                edge_file.writelines(cur_line)
            edge_file.flush()
            edge_file.close()
            print('Appending %d users from %s' % (len(cur_qq_list), table))
            self.graph_helper.add_users(cur_qq_list)
            cur_groups = list(cur_group_set)
            print('Appending %d groups from %s' % (len(cur_groups), table))
            self.graph_helper.add_groups(cur_groups)
            print('Appending %d edges from %s' % (write_tag, table))
            self.graph_helper.add_edges()

    def get_distinct_users(self, table):
        """
        get distinct users
        :return:
        """
        cur = self.__connect()
        query_sql = "select distinct QQNum from " + table
        cur.execute(query_sql)
        res_result = cur.fetchall()
        final_result = list()
        for res, in res_result:
            final_result.append(res)
        self.conn.close()
        return final_result

    def get_group_info(self, table):
        """
        get group information
        :return:
        """
        cur = self.__connect()
        query_sql = "select QunNum, MastQQ, CreateDate, Title, Class, QunText from " + table
        cur.execute(query_sql)
        res_result = cur.fetchall()
        self.conn.close()
        return res_result

    def get_distinct_groups(self, table):
        """
        get distinct groups
        :param table:
        :return:
        """
        cur = self.__connect()
        query_sql = "select distinct QunNum from " + table
        cur.execute(query_sql)
        res_result = cur.fetchall()
        final_result = list()
        for res, in res_result:
            final_result.append(res)
        self.conn.close()
        return final_result

    def get_edges(self, table):
        """
        Get the relationship between qq numbers and qun numbers
        :param table:
        :return:
        """
        cur = self.__connect()
        query_sql = 'select QQNum, Nick, Age, Gender, Auth, QunNum from ' + table
        cur.execute(query_sql)
        res_list = cur.fetchall()
        self.conn.close()
        return res_list

    def __connect(self):
        """
        得到连接信息
        返回: conn.cursor()
        """
        if not self.db:
            raise (NameError, "没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host, user=self.user, password=self.pwd, database=self.db, charset="utf8")
        cur = self.conn.cursor()
        if not cur:
            raise (NameError, "连接数据库失败")
        else:
            return cur

    def exec_query(self, sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段

        调用示例：
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__connect()
        cur.execute(sql)
        resList = cur.fetchall()

        # 查询完毕后必须关闭连接
        self.conn.close()
        return resList

    def exec_non_query(self, sql):
        """
        执行非查询语句

        调用示例：
            cur = self.__GetConnect()
            cur.execute(sql)
            self.conn.commit()
            self.conn.close()
        """
        cur = self.__connect()
        cur.execute(sql)
        self.conn.commit()
        self.conn.close()


def main():
    ms = DBHelper(host="219.245.186.247", user="sa", pwd="freemind1992", db="GroupData1")
    ms.append_entities()
    """
    groups = ms.get_distinct_groups("Group10")
    final_group_result = [str(group) for group in groups]
    print('Appending groups...')
    ms.graph_helper.add_groups(final_group_result)
    ms = DBHelper(host="219.245.186.247", user="sa", pwd="freemind1992", db="QunInfo1")
    groups = ms.get_group_info("QunTest")
    ms.graph_helper.update_group_info(groups)
    """


if __name__ == '__main__':
    main()
