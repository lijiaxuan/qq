# -*- coding:utf-8 -*-

import json
import logging
import datetime
import os
import pickle
from configparser import ConfigParser

from flask import Flask, render_template, request, url_for, redirect, session
from qqlib.utils.qq_constants import QQConstants
from qqlib.utils.qq import QQ

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='qq_monitor.log',
                    filemode='w')

app = Flask(__name__)


@app.route('/network')
def network():
    return render_template('network.html')


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
    """
    print('Monitoring login...')
    qq_helper.monitor_login()
    print(qq_helper.login_tag)
    """
    qq_helper.login_tag = 1
    if qq_helper.login_tag == 0:
        # tag, profile = qq_helper.profile(qq_num)
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
        friends = set()
        friends.add('302713508')
        friends.add('1531474354')
        """
        if tag == 0:
            result, friends = qq_helper.friends(qq_num)
            print(friends)
        """
        result = {'state': 0,
                  'tip': '',
                  'profile': profile,
                  'friends': ','.join(list(friends))}
    else:
        result = {'state': qq_helper.login_tag,
                  'tip': state_dict[qq_helper.login_tag]}
    return json.dumps(result)


@app.route('/search')
def search():
    return render_template('search.html')


@app.route('/password')
def password():
    return render_template('password.html')


@app.route('/crawl')
def crawl():
    return render_template('crawl.html')


if __name__ == '__main__':
    # app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
    # app.config['SESSION_TYPE'] = 'filesystem'
    app.run(debug=True, host='0.0.0.0', port=8081)
