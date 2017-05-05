# -*- coding: utf-8 -*-

import cx_Oracle
import datetime
from configparser import ConfigParser
from neo4j_helper import GraphDBHelper
import os

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


class OracleHelper:
    def __init__(self):
        conf = ConfigParser()
        conf.read('data.config')
        self.oracle_host = conf.get('oracle_config', 'oracle_host')
        self.oracle_port = conf.get('oracle_config', 'oracle_port')
        self.oracle_username = conf.get('oracle_config', 'oracle_username')
        self.oracle_pwd = conf.get('oracle_config', 'oracle_pwd')
        self.oracle_sid = conf.get('oracle_config', 'oracle_sid')
        # 202.117.54.201:1521/orcl
        self.conn_str = ''.join([self.oracle_host, ':', self.oracle_port, '/', self.oracle_sid])

    def get_tables(self):
        # db = cx_Oracle.connect("scott", "qqdata2015", "202.117.54.201:1521/orcl")
        db = cx_Oracle.connect(self.oracle_username, self.oracle_pwd, self.conn_str)
        table_query_sql = "select TABLE_NAME from all_tab_comments where TABLE_NAME like 'QQ%' order by TABLE_NAME"
        cursor = db.cursor()
        cursor.execute(table_query_sql)
        res_list = cursor.fetchall()
        res_list = [res for res, in res_list]
        db.close()
        return res_list

    def get_user_attributes(self, table_name):
        """
        Get user attributes from given table name
        :param table_name:
        :return:
        """
        print('Getting user info from %s...' % table_name)
        db = cx_Oracle.connect(self.oracle_username, self.oracle_pwd, self.conn_str)
        query_sql = "select distinct QQNUMBER, NAME, AGE, SIGMA3, GENDER from " + table_name
        cursor = db.cursor()
        cursor.execute(query_sql)
        res_list = cursor.fetchall()
        db.close()
        return res_list

    def get_group_edu_info(self):
        """
        Get group education information from table
        :return:
        """
        db = cx_Oracle.connect(self.oracle_username, self.oracle_pwd, self.conn_str)
        query_sql = 'select QUNNUM, LABEL, SCHOOL from "qunClass"'
        cursor = db.cursor()
        cursor.execute(query_sql)
        res_list = cursor.fetchall()
        db.close()
        return res_list


if __name__ == '__main__':
    oracle_helper = OracleHelper()
    print('Loading group education information...')
    result = oracle_helper.get_group_edu_info()
    print('Batch inserting data to neo4j...')
    graph_helper = GraphDBHelper()
    graph_helper.update_group_edu_info(result)
