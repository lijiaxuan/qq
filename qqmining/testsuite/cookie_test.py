# -*- coding: utf-8 -*-

import requests

qq_cookies = {
    'p_uin': 'o1531474354',
    'p_skey': '8G7pbBoHR*wnsNBYK48Bc3nGPyo4VyMjln8MqGDU6wo_'
}

sess = requests.session()
sess.cookies.clear()
sess.cookies.update(qq_cookies)
"""
profile_url = 'https://h5.qzone.qq.com/proxy/domain/base.qzone.qq.com/cgi-bin/user/cgi_userinfo_get_all?' \
              'uin=1341801774&vuin=1531474354&fupdate=1&rd=0.11844673947293383&g_tk=487292132&' \
              'qzonetoken=cd897a4f5bea8a0ad527dae2f058d6077ca0532b8' \
              'dhd2520742b03f69d580e36251ce8e7b5056ec9008d623c0e6' \
              'ae4d844e27f7067990cd493154bb0215092ad4237a76'
"""
profile_url = 'https://h5.qzone.qq.com/proxy/domain/base.qzone.qq.com/cgi-bin/user/cgi_userinfo_get_all?' \
              'uin=1341801774&vuin=1531474354&fupdate=1&rd=0.11844673947293383&g_tk=487292132'
r = sess.get(profile_url)
print(r.text)
