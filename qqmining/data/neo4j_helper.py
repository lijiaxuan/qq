# -*- coding:utf-8 -*-

import json
import os
from configparser import ConfigParser

from neo4j.v1 import GraphDatabase, basic_auth

"""
Neo4j helper
"""


class GraphDBHelper:
    """
    Graph Database Helper
    """

    def __init__(self):
        conf = ConfigParser()
        conf.read('data.config')
        neo4j_host = conf.get('neo4j_config', 'neo4j_host')
        neo4j_port = conf.get('neo4j_config', 'neo4j_port')
        neo4j_username = conf.get('neo4j_config', 'neo4j_username')
        neo4j_pwd = conf.get('neo4j_config', 'neo4j_pwd')
        # TODO:Verify whether this can be set using neo4j.v1 library
        self.neo4j_db_path = conf.get('neo4j_config', 'neo4j_db_path')
        self.driver = GraphDatabase.driver('bolt://' + neo4j_host + ':' + neo4j_port,
                                           auth=basic_auth(neo4j_username, neo4j_pwd))
        # driver = GraphDatabase.driver('bolt://localhost:7687',auth=basic_auth('neo4j', 'batnos'))
        self.import_path = os.path.join(self.neo4j_db_path, 'import')
        if not os.path.exists(self.import_path):
            os.mkdir(self.import_path)

    def add_users(self, users):
        """
        Add users to the graph database
        :return:
        """
        print('setting user data...')
        user_result = []
        batch_size = 100000
        tag = 0
        failed = 0
        for user in users:
            try:
                user_result.append({'uin': user})
                tag += 1
                if tag % batch_size == 0:
                    print('Inserting users at %d batch' % int(tag / batch_size))
                    self.add_batch_users(user_result)
                    user_result = list()
            except Exception as ex:
                # print(user)
                failed += 1
        if len(user_result) != 0:
            self.add_batch_users(user_result)
        print('Failed to parse %d profiles in total...' % failed)
        print('creating index on user...')
        # creating indexes
        session = self.driver.session()
        index_query = 'CREATE INDEX ON :QQ(uin)'
        session.run(index_query)
        session.close()

    def add_batch_users(self, user_result):
        """
        Insert batch users into graph db
        :param user_result:
        :return:
        """
        session = self.driver.session()
        user_nodes = {'users': user_result}
        query = 'UNWIND { users } AS map CREATE (n:QQ) SET n = map'
        session.run(query, parameters=user_nodes)
        session.close()

    def add_groups(self, groups):
        """
        Add groups to the graph database
        :return:
        """
        print('setting group data...')
        group_result = []
        batch_size = 100000
        tag = 0
        failed = 0
        for group in groups:
            try:
                group_result.append({'gid': group})
                tag += 1
                if tag % batch_size == 0:
                    print('Inserting groups at %d batch' % int(tag / batch_size))
                    self.add_batch_groups(group_result)
                    group_result = list()
            except Exception as ex:
                print(group)
                failed += 1
        if len(group_result) != 0:
            self.add_batch_groups(group_result)
        print('Failed to parse %d groups in total...' % failed)
        print('creating index on group...')
        # creating indexes
        session = self.driver.session()
        index_query = 'CREATE INDEX ON :Group(gid)'
        session.run(index_query)
        session.close()

    def add_batch_groups(self, group_result):
        """
        Insert batch users into graph db
        :param user_result:
        :return:
        """
        session = self.driver.session()
        group_nodes = {'groups': group_result}
        query = 'UNWIND { groups } AS map CREATE (g:Group) SET g = map'
        session.run(query, parameters=group_nodes)
        session.close()

    def add_edges(self):
        """
        add edges to the graph
        :return:
        """
        print('inserting edges...')
        link_query = 'USING PERIODIC COMMIT 5000 ' \
                     'LOAD CSV WITH HEADERS FROM "file:///edge_data.csv" AS line ' \
                     'MATCH (a:QQ {uin: line.from}),(b:Group { gid: line.to}) ' \
                     'CREATE (a)-[:Qun { nick:line.nick,age:line.age, gender:line.gender,auth:line.auth }]->(b)'
        session = self.driver.session()
        session.run(link_query)
        session.close()

    def update_group_info(self, group_info):
        """
        Update Group Information
        :param group_info:
        :return:
        """
        group_data = list()
        for qun, admin, create_date, title, group_class, desc in group_info:
            cur_group = {'gid': str(qun),
                         'admin': str(admin),
                         'create_date': create_date,
                         'title': title,
                         'class': group_class,
                         'desc': desc}
            group_data.append(cur_group)
        group_result = {'group_data': group_data}
        session = self.driver.session()
        update_group_query = 'WITH {group_data} AS pairs ' \
                             'UNWIND pairs AS p ' \
                             'MERGE (g:Group {gid:p.gid}) ON MATCH ' \
                             'SET g.admin = p.admin,' \
                             'g.create_date = p.create_date,' \
                             'g.title = p.title,' \
                             'g.class = p.class,' \
                             'g.desc = p.desc'
        session.run(update_group_query, parameters=group_result)
        session.close()

    def update_group_edu_info(self, group_edu_info):
        """
        Update Group Education Information
        :param group_edu_info:
        :return:
        """
        group_data = list()
        for qun, label, school in group_edu_info:
            cur_group = {'gid': str(qun),
                         'label': label,
                         'school': school,
                         'edu_tag': 1
                         }
            group_data.append(cur_group)
        group_result = {'group_data': group_data}
        session = self.driver.session()
        update_group_query = 'WITH {group_data} AS pairs ' \
                             'UNWIND pairs AS p ' \
                             'MERGE (g:Group {gid:p.gid}) ON MATCH ' \
                             'SET g.label = p.label,' \
                             'g.school = p.school,' \
                             'g.edu_tag = p.edu_tag'
        session.run(update_group_query, parameters=group_result)
        session.close()

    def update_user_info(self, user_info):
        """
        Update Group Information
        :param group_info:
        :return:
        """
        user_data = list()
        for uin, name, age, sigma1, sigma2, sigma3, gender in user_info:
            cur_user = {'uin': str(uin),
                        'name': name,
                        'age': age,
                        'sigma1': sigma1,
                        'sigma2': sigma2,
                        'sigma3': sigma3,
                        'gender': gender}
            user_data.append(cur_user)
        user_result = {'user_data': user_data}
        session = self.driver.session()
        update_user_query = 'WITH {user_data} AS pairs ' \
                            'UNWIND pairs AS p ' \
                            'MERGE (g:QQ {uin:p.uin}) ON MATCH ' \
                            'SET g.name = p.name,' \
                            'g.sigma1 = p.sigma1,' \
                            'g.sigma2 = p.sigma2,' \
                            'g.sigma3 = p.sigma3,' \
                            'g.gender = p.gender ' \
                            'ON CREATE ' \
                            'SET g.name = p.name,' \
                            'g.sigma1 = p.sigma1,' \
                            'g.sigma2 = p.sigma2,' \
                            'g.sigma3 = p.sigma3,' \
                            'g.gender = p.gender '
        session.run(update_user_query, parameters=user_result)
        session.close()

    def build_graph(self):
        """
        Build graph by adding users and edges
        :return:
        """
        self.clear_data()
        self.add_users()
        self.add_edges()

    def clear_data(self):
        """
        Clear data from current database
        :return:
        """
        print('clearing edge data...')
        edge_delete = 'MATCH p=()-[r:Qun]->() delete p;'
        session = self.driver.session()
        session.run(edge_delete)
        session.close()
        print('clearing user data...')
        user_delete = 'MATCH (u:QQ) delete u;'
        session = self.driver.session()
        session.run(user_delete)
        session.close()
        print('clearing group data...')
        group_delete = 'MATCH (g:Group) delete g;'
        session = self.driver.session()
        session.run(group_delete)
        session.close()


if __name__ == '__main__':
    print('hello')
    # graph_helper = GraphDBHelper()
    # graph_helper.build_graph()