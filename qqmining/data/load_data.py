# -*- coding:utf-8 -*-

import pymssql

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

    def get_group_dbs(self):
        """
        Get group databases
        :return:
        """
        cur = self.__connect()
        cur.execute("Select Name FROM Master.dbo.SysDatabases order by Name")
        res_list = cur.fetchall()
        final_list = list()
        for res, in res_list:
            final_list.append(res)
        self.conn.close()
        return final_list

    def append_edges(self):
        """
        Get user list from tables
        :return:
        """
        table_list = self.get_tables()
        write_batch = 100000
        for table in table_list:
            edge_file_path = os.path.join(self.graph_helper.import_path, 'edge_data.csv')
            edge_file = open(edge_file_path, 'w')
            edge_file.writelines('from,to,nick,age,gender\n')
            write_tag = 0
            print('Appending entities from table %s...' % table)
            edges = self.get_edges(table)
            print('Loading data completed...')
            # QQNum, Nick, Age, Gender, QunNum
            for qq_num, nick, age, gender, qun_num in edges:
                write_tag += 1
                if write_tag % write_batch == 0:
                    print("Current batch %d" % (int(write_tag / write_batch),))
                cur_line = ','.join([str(qq_num), str(qun_num), nick, str(age), str(gender)]) + '\n'
                edge_file.writelines(cur_line)
            edge_file.flush()
            edge_file.close()
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
        query_sql = 'select QQNum, Nick, Age, Gender, QunNum from ' + table
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

    def get_group_info(self, table_name):
        """
        get group information
        """
        cur = self.__connect()
        query_sql = 'select QunNum, CreateDate, Title, QunText from ' + table_name
        cur.execute(query_sql)
        res_list = cur.fetchall()
        self.conn.close()
        return res_list

    def get_hotel_data(self):
        cur = self.__connect()
        query_sql = 'SELECT [Name],[CtfTp],[CtfId],[Gender],[Birthday],[Mobile],[Email] FROM kaifang_data'
        cur.execute(query_sql)
        res_list = cur.fetchall()
        self.conn.close()
        return res_list

    def load_pwd_data(self):
        """
        loading password data from database
        """
        # TODO:Password Insertion at 2385 batch
        print('Loading password data...')
        cur = self.__connect()
        pwd_sql = "SELECT username,password,password_md5,email,source,gender,bday FROM leak_data_whole"
        cur.execute(pwd_sql)
        tag = 0
        pwd_list = list()
        batch_size = 100000
        for username, password, md5, email, source, gender, bday in cur:
            cur_pwd = {'username': username,
                       'password': password,
                       'password_md5': md5,
                       'email': email,
                       'source': source,
                       'pwd_gender': gender,
                       'bday': bday}
            pwd_list.append(cur_pwd)
            tag += 1
            if tag % batch_size == 0:
                print('Inserting passwords at %d batch' % int(tag / batch_size))
                self.graph_helper.add_batch_pwds(pwd_list)
                pwd_list.clear()
        if len(pwd_list) != 0:
            self.graph_helper.add_batch_pwds(pwd_list)
        cur.close()


def main():
    ms = DBHelper(host="219.245.186.247", user="sa", pwd="freemind1992", db="leak")
    ms.load_pwd_data()
    """
    ms = DBHelper(host="219.245.186.247", user="sa", pwd="freemind1992", db="leak")
    hotel_data = ms.get_hotel_data()
    print("Hotel data %d records..." % len(hotel_data))
    ms.graph_helper.add_hotel_info(hotel_data)
    """
    """
    qun_prefix = 'GroupData'
    for qun_index in range(1, 12):
        cur_qun_db = qun_prefix + str(qun_index)
        print('Processing db %s' % cur_qun_db)
        ms = DBHelper(host="219.245.186.249", user="sa", pwd="qq_2015", db=cur_qun_db)
        ms.append_edges()
        tables = ms.get_tables()
        for table in tables:
            print('Getting group information from %s' % table)
            groups = ms.get_group_info(table)
            print('Inserting %d groups...' % len(groups))
            ms.graph_helper.update_group_info(groups)
    """
    # conn = pymssql.connect(host="219.245.186.249", user="sa", password="qq_2015", database="QunInfo1", charset="utf8")
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
