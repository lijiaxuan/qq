# -*- coding:utf-8 -*-

import json
import logging
import datetime
import os
import pickle
import random
from configparser import ConfigParser
import dpkt
import math
from elasticsearch import Elasticsearch

from flask import Flask, render_template, request, url_for, redirect, session, send_from_directory
from qqlib.utils.qq import QQ
from elasticsearch_dsl import Search, Q

if __name__ == '__main__':
    from data_fetcher import DataFetcher
else:
    from .data_fetcher import DataFetcher

ALLOWED_EXTENSIONS = set(['png', 'jpg', 'pcap', 'doc', 'docx'])
es = Elasticsearch([{'host': '219.245.186.69', 'port': 9200}])
fetcher = DataFetcher()

NODE_INDEX = 'neo4j-index-node'
RELATIONSHIP_INDEX = 'neo4j-index-relationship'

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='qq_monitor.log',
                    filemode='w')

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = os.path.join(os.getcwd(), 'uploads')
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024


def allowed_file(filename):
    file_extension = filename.rsplit('.', 1)[1].lower()
    return '.' in filename and file_extension in ALLOWED_EXTENSIONS


@app.route('/network', methods=['GET', 'POST'])
def network():
    if request.method == 'GET':
        return render_template('network.html', tag_result=None, social_result=None)
    else:
        file = request.files['network-file']
        if not os.path.exists(app.config['UPLOAD_FOLDER']):
            os.makedirs(app.config['UPLOAD_FOLDER'])
        if file and allowed_file(file.filename):
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], file.filename))
        tag_result = list()
        tag_result.append(['http://www.baidu.com', u'搜索'])
        tag_result.append(['http://www.sina.com', u'娱乐'])
        social_result = list()
        social_result.append(['百度', 'qingyuanxingsi@163.com'])
        social_result.append(['微信', 'qingyuanxingsi'])
        return render_template('network.html', tag_result=tag_result, social_result=social_result)


@app.route('/qq', methods=['POST'])
def qq():
    qq_num = request.values.get('qq_num', '')
    conf = ConfigParser()
    conf.read('qqweb.config')
    qq_uin = str(conf.get('qq_config', 'qq_uin')).strip()
    qq_pwd = str(conf.get('qq_config', 'qq_pwd')).strip()
    state_dict = {0: u'正常',
                  1: u'爬取QQ号登录需要验证码，请更换登录QQ号！',
                  2: u'爬取QQ号被冻结，请更换登录QQ号!',
                  3: u'爬取QQ号登录失败，请更换登录QQ号!',
                  4: u'主动退出'}
    qq_helper = QQ(qq_uin, qq_pwd, max_page=5, nohup=True, wait=False)
    print('Monitoring login...')
    qq_helper.monitor_login()
    if qq_helper.login_tag == 0:
        tag, profile = qq_helper.profile(qq_num)
        """
        tag = 0
        user_profile = {"uin": 1341801774,
                        "is_famous": 0,
                        "famous_custom_homepage": 0,
                        "nickname": "浮 華 一 玍",
                        "emoji": [],
                        "spacename": "浮 華 一 玍的空间",
                        "desc": "",
                        "signature": "",
                        "avatar": "http://b123.photo.store.qq.com/psb?/V115MMLI42hzHw/3rL4Z1WdZwEt17l0vF.lSQpf9Uyd3wsGoTGAqrXsq.o!/b/dDqHVkmFDgAA&amp;bo=kADiAAAAAAABAFU!",
                        "sex_type": 0,
                        "sex": 2,
                        "animalsign_type": 0,
                        "constellation_type": 0,
                        "constellation": 9,
                        "age_type": 0,
                        "age": 24,
                        "islunar": 0,
                        "birthday_type": 0,
                        "birthyear": 1993,
                        "birthday": "01-14",
                        "bloodtype": 0,
                        "address_type": 0,
                        "country": "中国",
                        "province": "陕西",
                        "city": "西安",
                        "home_type": 0,
                        "hco": "",
                        "hp": "",
                        "hc": "",
                        "marriage": 1,
                        "career": "",
                        "company": "",
                        "cco": "",
                        "cp": "",
                        "cc": "",
                        "cb": "",
                        "mailname": "",
                        "mailcellphone": "",
                        "mailaddr": "",
                        "qzworkexp": [],
                        "qzeduexp": [],
                        "ptimestamp": 1483580293}
        profile = json.dumps(user_profile)
        """
        friends = set()
        if tag == 1:
            result, friends = qq_helper.friends(qq_num)
            print(friends)
        result = {'state': 0,
                  'tip': '',
                  'profile': profile,
                  'friends': ','.join(list(friends))}
    else:
        print(state_dict[qq_helper.login_tag])
        result = {'state': qq_helper.login_tag,
                  'tip': state_dict[qq_helper.login_tag]}
    return json.dumps(result)


@app.route('/search', methods=['GET', 'POST'])
def search():
    if request.method == 'GET':
        return render_template('search.html', user_result=None, search=False)
    else:
        age_interval_dict = {'-1': (0, 100),
                             '0': (0, 18),
                             '1': (18, 22),
                             '2': (23, 26),
                             '3': (27, 35),
                             '4': (35, 100)}
        uin = request.form['uin']
        uin = uin.strip()
        name = request.form['name']
        name = name.strip()
        gender = request.form['gender']
        age = request.form['age']
        age_filter = True
        gender_filter = True
        # 男 1
        # 女 0
        if gender == u'不限':
            gender_filter = False
        elif gender == u'男':
            gender_tag = 0
        else:
            gender_tag = 1
        if age == '-1':
            age_filter = False
        else:
            age_low, age_high = age_interval_dict[age]
        s = Search().using(es)

        if len(uin) != 0:
            s = s.query("match", uin=uin)

        if len(name) != 0:
            s = s.query("match", name=name)

        if gender_filter:
            s = s.query("match", gender=gender_tag)

        if age_filter:
            s = s.filter('range', age={'gte': age_low, 'lt': age_high})

        response = s.execute()

        final_user_result = list()
        for hit in s:
            uuid = hit.meta.id
            final_user_result.append([uuid, hit.uin, hit.age])

        return render_template('search.html', user_result=final_user_result, search=True)


@app.route('/graph', methods=['POST'])
def graph():
    uin = request.values.get('uin', '')
    graph_result = {
        'nodes': [],
        'links': []
    }
    groups = fetcher.get_group_by_uin(uin)
    uin_set = set()
    for group in groups:
        cur_group_name = 'G' + str(group)
        res = es.search(index=NODE_INDEX, doc_type='Group', body={'query': {'match': {'gid': group}}})
        hits = res['hits']['total']
        if hits != 0:
            group_result = res['hits']['hits'][0]['_source']
            if 'edu_tag' in group_result:
                graph_result['nodes'].append({'name': cur_group_name,
                                              'category': 1,
                                              'value': str(group),
                                              'symbol': 'roundRect'
                                              })
            else:
                graph_result['nodes'].append({'name': cur_group_name,
                                              'category': 1,
                                              'value': str(group),
                                              'symbol': 'diamond'
                                              })
        group_users = fetcher.get_users_by_group(str(group))
        for group_user in group_users:
            if group_user not in uin_set:
                graph_result['nodes'].append({'name': str(group_user), 'category': 0, 'value': str(group_user)})
                uin_set.add(group_user)
            graph_result['links'].append({'source': cur_group_name, 'target': str(group_user)})
    return json.dumps(graph_result)


@app.route('/user_detail', methods=['GET'])
def user_details():
    uin = request.args.get('uin')
    uuid = request.args.get('uuid')
    user_detail = es.get(index=NODE_INDEX, doc_type='QQ', id=uuid)
    user_info = {}
    if user_detail['found']:
        user_info = user_detail['_source']
        if 'name' not in user_info:
            user_info['name'] = ''
        gender_tag = user_info['gender']
        if gender_tag == 0:
            user_info['gender'] = u'男'
        elif gender_tag == 1:
            user_info['gender'] = u'女'
        else:
            user_info['gender'] = u'未知'
    user_info['likes'] = u'羽毛球'
    user_info['hometown'] = u'陕西省西安市'
    # get education information
    groups = fetcher.get_group_by_uin(uin)
    edu_result = list()
    for group_index, group in enumerate(groups):
        res = es.search(index=NODE_INDEX, doc_type='Group', body={'query': {'match': {'gid': group}}})
        hits = res['hits']['total']
        if hits != 0:
            group_result = res['hits']['hits'][0]['_source']
            if 'edu_tag' in group_result:
                if 'label' not in group_result:
                    group_result['label'] = ''
                if 'school' not in group_result:
                    group_result['school'] = ''
                edu_result.append(group_result)

    qq_email = uin + '@qq.com'
    # get password information
    source_dict = {
        'renren': u'人人网',
        'csdn': 'CSDN',
        'duduniu': u'嘟嘟牛',
        '126': u'126邮箱',
        'kaixin': u'开心网',
        'ys168': u'永硕E盘',
        'zhenai': u'真爱网',
        'tianya': u'天涯社区',
        '163': u'163邮箱',
        'qq': 'QQ'
    }
    qq_email = 'weeny_84@hotmail.com'
    pwd_result = es.search(index=NODE_INDEX, doc_type='Pwd', body={'query': {'match': {'email': qq_email}}})
    pwd_hits = pwd_result['hits']['total']
    final_pwd_result = list()
    if pwd_hits != 0:
        pwd_hits_info = pwd_result['hits']['hits']
        for pwd_hit_info in pwd_hits_info:
            pwd_source = pwd_hit_info['_source']
            user_email = pwd_source['email']
            if user_email == qq_email:
                pwd_src = pwd_source['source']
                if pwd_src in source_dict:
                    pwd_source['source'] = source_dict[pwd_src]
                else:
                    pwd_source['source'] = ''
                final_pwd_result.append(pwd_source)
            else:
                break

    # get hotel result
    # test email
    qq_email = 'clyzy12358@163.com'
    hotel_result = es.search(index=NODE_INDEX, doc_type='Hotel', body={'query': {'match': {'email': qq_email}}})
    hotel_hits = hotel_result['hits']['total']
    final_hotel_result = list()
    if hotel_hits != 0:
        hotel_hits_info = hotel_result['hits']['hits']
        for hotel_hit_info in hotel_hits_info:
            hotel_source = hotel_hit_info['_source']
            hotel_email = hotel_source['email']
            if hotel_email == qq_email:
                hotel_gender = hotel_source['hotel_gender']
                if hotel_gender == 'M':
                    hotel_source['hotel_gender'] = u'男'
                elif hotel_gender == 'F':
                    hotel_source['hotel_gender'] = u'女'
                else:
                    hotel_source['hotel_gender'] = u'不详'
                final_hotel_result.append(hotel_source)
            else:
                break
    return render_template('user_details.html',
                           user_info=user_info,
                           edu_info=edu_result,
                           pwd_info=final_pwd_result,
                           hotel_info=final_hotel_result)


@app.route('/crawl')
def crawl():
    return render_template('crawl.html')


@app.route('/password', methods=['GET', 'POST'])
def password():
    if request.method == 'GET':
        return render_template('password.html', pwd_result=None, search=False)
    else:
        username = request.form['username']
        username = username.strip()
        email = request.form['email']
        email = email.strip()
        source = request.form['source']
        source = source.strip()
        s = Search(using=es, index=NODE_INDEX, doc_type='Pwd')

        if len(username) != 0:
            s = s.query("match", username=username)

        if len(email) != 0:
            s = s.query("match", email=email)

        if len(source) != 0:
            s = s.query("match", source=source)

        source_dict = {
            'renren': u'人人网',
            'csdn': 'CSDN',
            'duduniu': u'嘟嘟牛',
            '126': u'126邮箱',
            'kaixin': u'开心网',
            'ys168': u'永硕E盘',
            'zhenai': u'真爱网',
            'tianya': u'天涯社区',
            '163': u'163邮箱',
            'qq': 'QQ'
        }

        response = s.execute()

        final_pwd_result = list()
        for hit in s:
            # uuid = hit.meta.id
            pwd_source = ''
            if hit.source in source_dict:
                pwd_source = source_dict[hit.source]
            else:
                print(hit.source)
            final_pwd_result.append((hit.username, hit.email, hit.password, hit.password_md5, pwd_source))
        return render_template('password.html', pwd_result=final_pwd_result, search=True)


if __name__ == '__main__':
    app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
    app.config['SESSION_TYPE'] = 'filesystem'
    app.run(debug=True, host='0.0.0.0', port=8081)
