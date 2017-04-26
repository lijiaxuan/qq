# -*- coding: utf-8 -*-

import base64
import binascii
import datetime
import hashlib
import json
import logging
import os
import platform
import re
import sys
import tempfile
import time
from profile import Profile

import requests
import rsa
from PIL import Image

if __name__ == '__main__':
    import tea
    from qq_constants import QQConstants
    from mailbox import MailBoxHandler
else:
    from . import tea
    from .qq_constants import QQConstants
    from .mailbox import MailBoxHandler

# 账户类型
# -4 未开通空间
# -3 账号举报
# -2 爬取出错
# -1 不存在
# 0 存在，但无权限访问
# 1 存在，且可正常访问
pattern = re.compile(r'g_userProfile = ({[\s\S]*?})')
callback_pattern = re.compile('_Callback\(([\s\S]*?)\)')
filter_pattern = re.compile('^\d{5,11}$')
sub_pattern = re.compile('[\r\n|\+]')

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='qq_crawler.log',
                    filemode='w')
qq_logger = logging.getLogger('qq_logger')
if __name__ == '__main__':
    mailbox_handler = MailBoxHandler()
    qq_logger.addHandler(mailbox_handler)


class QQ:
    appid = 549000912
    urlLogin = 'http://xui.ptlogin2.qq.com/cgi-bin/xlogin'
    urlCheck = 'http://check.ptlogin2.qq.com/check'
    urlCap = 'http://captcha.qq.com/cap_union_show'
    urlImg = 'http://captcha.qq.com/getimgbysig'
    urlSubmit = 'http://ptlogin2.qq.com/login'
    chat_url = 'http://taotao.qq.com/cgi-bin/emotion_cgi_msglist_v6'

    def __init__(self, qq='', pwd='', storage_helper=None, store_json=False,
                 max_page=sys.maxsize, query_time_out=None,
                 nohup=True):
        self.sys = platform.system()
        if self.sys == 'Windows':
            self.userAgent = 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.87 Safari/537.36'
        if self.sys == 'Linux':
            self.userAgent = 'Mozilla/5.0 (X11; Linux x86_64; rv:38.0) Gecko/20100101 Firefox/38.0'
        self.qq = qq
        self.pwd = pwd
        self.nickname = None
        self.vcode = None
        self.session = None
        self.login_cookie = None
        self.vcodeShow = 0
        self.loginSig = ''
        self.g_tk = None
        self.nohup = nohup
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.requests = requests.Session()
        self.max_page = max_page
        self.store_json = store_json
        self.storage_helper = storage_helper
        self.query_time_out = query_time_out
        self.proxy = None
        self.p_skey = None
        self.p_uin = None
        self.pt4_token = None
        self.cnt = 0
        self.busy_warning = 0
        self.max_busy_warning = 3
        self.uin = None
        self.login_tag = QQConstants.qq_stage_failure
        self.re_login = False
        self.captcha_dir = os.getcwd()
        python_env = sys.version
        if python_env.startswith('2'):
            self.is_python2 = True
        else:
            self.is_python2 = False

    def cookie_login(self, p_skey, p_uin):
        """
        Login using user entered cookies
        :param p_skey:
        :param p_uin:
        :return:
        """
        self.p_skey = p_skey
        self.p_uin = p_uin
        qq_cookies = {'p_skey': p_skey,
                      'p_uin': p_uin
                      }
        self.requests.cookies.update(qq_cookies)
        self.g_tk = self.gtk()

    def monitor_login(self):
        """
        Prepare login: get pt_login_sig
        :return:
        """
        self.login_tag = QQConstants.qq_stage_failure
        # clear session cookies
        self.requests.cookies.clear_session_cookies()
        par = {
            'proxy_url': 'http://qzs.qq.com/qzone/v6/portal/proxy.html',
            'daid': 5,
            'hide_title_bar': 1,
            'low_login': 0,
            'qlogin_auto_login': 1,
            'no_verifyimg': 1,
            'link_target': 'blank',
            'appid': self.appid,
            'style': 22,
            'target': 'self',
            's_url': 'http://qzs.qq.com/qzone/v5/loginsucc.html?para=izone',
            'pt_qr_app': '手机QQ空间',
            'pt_qr_link': 'http://z.qzone.com/download.html',
            'self_regurl': 'http://qzs.qq.com/qzone/v6/reg/index.html',
            'pt_qr_help_link': 'http://z.qzone.com/download.html',
        }
        r = self.requests.get(self.urlLogin, params=par)
        if 'pt_login_sig' in r.cookies:
            self.loginSig = r.cookies['pt_login_sig']
        self.check()

    def check(self):
        par = {
            'regmaster': '',
            'pt_tea': 1,
            'pt_vcode': 1,
            'uin': self.qq,
            'appid': self.appid,
            'js_ver': 10153,
            'js_type': 1,
            'login_sig': self.loginSig,
            'u1': 'http://qzs.qq.com/qzone/v5/loginsucc.html?para=izone',
            'r': '0.4459484001584795'
        }
        r = self.requests.get(self.urlCheck, params=par)
        li = re.findall('\'(.*?)\'', r.text)
        self.vcode = li[1]
        self.uin = li[2]
        if li[0] == '1':
            self.vcodeShow = 1
            if self.nohup or self.re_login:
                self.login_tag = QQConstants.qq_stage_captcha
                qq_logger.warning('[QQ]%s CAPTCHA needed,I choose death!' % self.qq)
                return
            self.getVerifyCode()
        else:
            self.session = li[3]
        self.login()

    def fromhex(self, s):
        # Python 3: bytes.fromhex
        return bytes(bytearray.fromhex(s))

    def getEncryption(self):
        puk = rsa.PublicKey(int(
            'F20CE00BAE5361F8FA3AE9CEFA495362'
            'FF7DA1BA628F64A347F0A8C012BF0B25'
            '4A30CD92ABFFE7A6EE0DC424CB6166F8'
            '819EFA5BCCB20EDFB4AD02E412CCF579'
            'B1CA711D55B8B0B3AEB60153D5E0693A'
            '2A86F3167D7847A0CB8B00004716A909'
            '5D9BADC977CBB804DBDCBA6029A97108'
            '69A453F27DFDDF83C016D928B3CBF4C7',
            16
        ), 3)
        # uin is the bytes of QQ number stored in unsigned long (8 bytes)
        salt = self.uin.replace(r'\x', '')
        h1 = hashlib.md5(self.pwd.encode()).digest()
        s2 = hashlib.md5(h1 + self.fromhex(salt)).hexdigest().upper()
        rsaH1 = binascii.b2a_hex(rsa.encrypt(h1, puk)).decode()
        rsaH1Len = hex(len(rsaH1) // 2)[2:]
        hexVcode = binascii.b2a_hex(self.vcode.upper().encode()).decode()
        vcodeLen = hex(len(hexVcode) // 2)[2:]
        l = len(vcodeLen)
        if l < 4:
            vcodeLen = '0' * (4 - l) + vcodeLen
        l = len(rsaH1Len)
        if l < 4:
            rsaH1Len = '0' * (4 - l) + rsaH1Len
        pwd1 = rsaH1Len + rsaH1 + salt + vcodeLen + hexVcode
        saltPwd = base64.b64encode(
            tea.encrypt(self.fromhex(pwd1), self.fromhex(s2))
        ).decode().replace('/', '-').replace('+', '*').replace('=', '_')
        return saltPwd
        """
        # Python3 Version
        e = int(self.qq).to_bytes(8, 'big')
        o = hashlib.md5(self.pwd.encode())
        r = bytes.fromhex(o.hexdigest())
        p = hashlib.md5(r + e).hexdigest()
        a = binascii.b2a_hex(rsa.encrypt(r, puk)).decode()
        s = hex(len(a) // 2)[2:]
        l = binascii.hexlify(self.vcode.upper().encode()).decode()
        c = hex(len(l) // 2)[2:]
        c = '0' * (4 - len(c)) + c
        s = '0' * (4 - len(s)) + s
        salt = s + a + binascii.hexlify(e).decode() + c + l
        return base64.b64encode(
            tea.encrypt(bytes.fromhex(salt), bytes.fromhex(p))
        ).decode().replace('/', '-').replace('+', '*').replace('=', '_')
        """

    def login(self):
        d = self.requests.cookies.get_dict()
        if 'ptvfsession' in d:
            self.session = d['ptvfsession']
        par = {
            'action': '2-0-1450538632070',
            'aid': self.appid,
            'daid': 5,
            'from_ui': 1,
            'g': 1,
            'h': 1,
            'js_type': 1,
            'js_ver': 10153,
            'login_sig': self.loginSig,
            'p': self.getEncryption(),
            'pt_randsalt': 0,
            'pt_uistyle': 32,
            'pt_vcode_v1': self.vcodeShow,
            'pt_verifysession_v1': self.session,
            'ptlang': 2052,
            'ptredirect': 0,
            't': 1,
            'u': self.qq,
            'u1': 'http://qzs.qq.com/qzone/v5/loginsucc.html?para=izone',
            'verifycode': self.vcode
        }
        r = self.requests.get(self.urlSubmit, params=par)
        logging.info(r.text)
        if u'登录成功' not in r.text:
            self.login_tag = QQConstants.qq_stage_failure
            if u'恢复正常使用' in r.text:
                qq_logger.warning("[QQ]QQ %s needed to be recovered..." % self.qq)
                self.login_tag = QQConstants.qq_stage_recover
            qq_logger.warning('[QQ]QQ %s Login in failed.' % self.qq)
        else:
            li = re.findall('http://[^\']+', r.text)
            if len(li):
                self.urlQzone = li[0]
            self.g_tk = self.gtk()
            visit_ok = self.visit_qzone()
            if visit_ok:
                self.login_tag = QQConstants.qq_stage_running
                self.pt4_token = self.requests.cookies.get_dict()['pt4_token']
                logging.info('Login in successful.')
            else:
                self.login_tag = QQConstants.qq_stage_failure
                logging.info('[QZone]Login in failed')

    def likes(self, qq_num, proxy=None):
        """
        Get the interests of the given qq number
        :param qq_num:
        :return:
        """
        headers = {
            'User-Agent': self.userAgent
        }
        interest_url = 'http://page.qq.com/cgi-bin/profile/interest_get'
        par = {
            'uin': qq_num,
            'vuin': self.qq,
            'flag': 1,
            'rd': 0.8213985140901059,
            'fupdate': 1,
            'g_tk': self.g_tk
        }
        try:
            r = self.requests.get(interest_url, headers=headers, params=par, proxies=proxy,
                                  timeout=self.query_time_out)
            return r.text, True
        except Exception as ex:
            logging.error('Failed to fetch interests %s' % qq_num)
            logging.error(ex)
            return '', False

    def friends2(self, query, proxy=None):
        """
        Fetch friends from visitors
        :param query:
        :param proxy:
        :return:
        """
        headers = {
            'User-Agent': self.userAgent,
            'Referer': 'http://cm.qzs.qq.com/qzone/app/mood_v6/html/index.html'
        }
        visitor_url = 'https://h5s.qzone.qq.com/proxy/domain/g.qzone.qq.com/cgi-bin/friendshow/cgi_get_visitor_simple'
        params = {
            'uin': query,
            'mask': 2,
            'g_tk': self.g_tk,
            'page': 1,
            'fupdate': 1
        }
        try:
            r = self.requests.get(visitor_url, params=params, headers=headers,
                                  proxies=proxy, timeout=QQConstants.qq_friends_time_out)
        except Exception as ex:
            logging.error('Failed to get visitor info')
            return 0, set()
        matches = callback_pattern.findall(r.text)
        friends = list()
        if matches:
            try:
                json_text = json.loads(matches[0])
            except Exception as ex:
                return 1, set()
            msg = json_text['message']
            if msg == 'succ':
                for sample in json_text['data']['items']:
                    friend = sample['uin']
                    friends.append(friend)
                return 1, set(friends)
            elif msg == u'抱歉，您没有权限访问。':
                return 0, set()
            else:
                print(r.text)
                return 0, set()
        else:
            print(r.text)
            return 0, set()

    def visit_qzone(self):
        """
        Visit qzone to set cookies properly
        :return:
        """
        headers = {
            'User-Agent': self.userAgent
        }
        try:
            self.requests.get(self.urlQzone, headers=headers, proxies=self.proxy)
        except:
            logging.info('[QQ]Visiting qzone failed')
            return False
        cookies = self.requests.cookies.get_dict()
        # self.p_skey = cookies['p_skey']
        # self.pt4_token = cookies['pt4_token']
        # self.p_uin = cookies['p_uin']
        logging.info('[QQ]Visiting qzone:cookie set properly')
        return True

    def getVerifyCode(self):
        par = {
            'clientype': 2,
            'uin': self.qq,
            'aid': self.appid,
            'cap_cd': self.vcode,
            'pt_style': 32,
            'rand': 0.5994133797939867,
        }
        r = self.requests.get(self.urlCap, params=par)
        vsig = re.findall('g_vsig = "([^"]+)"', r.text)[0]
        par = {
            'clientype': 2,
            'uin': self.qq,
            'aid': self.appid,
            'cap_cd': self.vcode,
            'pt_style': 32,
            'rand': 0.5044117101933807,
            'sig': vsig
        }
        r = self.requests.get(self.urlImg, params=par)
        tmp = tempfile.mkstemp(dir=self.captcha_dir, suffix='.jpg')
        os.write(tmp[0], r.content)
        os.close(tmp[0])
        # Windows specific
        # os.startfile(tmp[1])
        print(tmp[1])
        logging.info('[QQ]Verify code image path:%s' % tmp[1])
        if self.sys == 'Windows':
            im = Image.open(tmp[1])
            im.show()
        if not self.is_python2:
            vcode = input('Verify code: ')
        else:
            vcode = raw_input('Verify code: ')
        os.remove(tmp[1])
        # return vcode
        par = {
            'clientype': 2,
            'uin': self.qq,
            'aid': self.appid,
            'cap_cd': self.vcode,
            'pt_style': 32,
            'rand': '0.9467124678194523',
            'capclass': 0,
            'sig': vsig,
            'ans': vcode,
        }
        urlVerify = 'http://captcha.qq.com/cap_union_verify_new'
        r = self.requests.get(urlVerify, params=par)
        js = r.json()
        self.vcode = js['randstr']
        self.session = js['ticket']

    def gtk(self):
        d = self.requests.cookies.get_dict()
        hash = 5381
        s = ''
        if 'p_skey' in d:
            s = d['p_skey']
        elif 'skey' in d:
            s = d['skey']
        for c in s:
            hash += (hash << 5) + ord(c)
        return hash & 0x7fffffff

    def get_qq_info(self, qq):
        """
        Get whole qq information for the given qq number
        :param qq:
        :return:
        """
        try:
            account_type, profile = self.profile(qq)
        except Exception as ex:
            return QQConstants.qq_other_tag, '{}', set()
        friend_set = set()
        if account_type == 1:
            friend_set, ok = self.friends(qq)
        return account_type, profile, friend_set

    def logout(self):
        """
        logout from qq server
        :return:
        """
        logout_url = 'http://ptlogin2.qq.com/logout'
        params = {
            'pt4_token': self.pt4_token,
            'deep_logout': 1
        }
        try:
            r = self.requests.get(logout_url, params=params)
        except Exception as ex:
            logging.warning(r.text)
            logging.warning('Logout failed %s' % self.qq)
        logging.info('Logout succeed %s...' % self.qq)

    def profile(self, qq, proxy=None):
        """
        Get user information
        :param qq:
        :param proxy:
        :return:
        """
        headers = {
            'User-Agent': self.userAgent
        }
        profile_url = 'http://base.s21.qzone.qq.com/cgi-bin/user/cgi_userinfo_get_all'
        par = {
            'uin': qq,
            'vuin': self.qq,
            'fupdate': 1,
            'rd': 0.4750982090668201,
            'g_tk': self.g_tk
        }
        try:
            r = self.requests.get(profile_url, headers=headers, params=par, timeout=self.query_time_out)
        except Exception as ex:
            logging.warning('Profile fetch failed[Network]:%s' % qq)
            logging.warning(ex)
            return QQConstants.qq_other_tag, '{}'
        matches = callback_pattern.findall(r.text)
        if matches:
            try:
                json_text = json.loads(matches[0])
            except Exception as ex:
                return QQConstants.qq_normal_tag, matches[0]
            code = json_text['code']
            msg = json_text['message']
            logging.info(msg)
            if code == 0:
                return QQConstants.qq_normal_tag, json.dumps(json_text['data'])
            else:
                if msg == u'您无权访问':
                    return QQConstants.qq_limit_tag, '{}'
                elif msg == u'非法操作':
                    return QQConstants.qq_no_tag, '{}'
                elif msg == u'服务器繁忙，请稍候再试。':
                    time.sleep(QQConstants.manager_busy_sleep)
                    return QQConstants.qq_other_tag, '{}'
                elif msg == u'请先登录':
                    """
                    self.re_login = True
                    qq_logger.warning('[QQ][Profile]%s Wait to login again...' % self.qq)
                    time.sleep(QQConstants.manager_kickout_sleep)
                    self.monitor_login()
                    if self.login_tag != QQConstants.qq_stage_running:
                        return QQConstants.qq_stop_tag, '{}'
                    else:
                        return QQConstants.qq_other_tag, '{}'
                    """
                    return QQConstants.qq_stop_tag, '{}'
                elif msg == u'对不起，您的操作太频繁，请稍后再试。':
                    time.sleep(60 * 2)
                    self.busy_warning += 1
                    if self.busy_warning <= self.max_busy_warning:
                        return QQConstants.qq_other_tag, '{}'
                    else:
                        qq_logger.info('%s:I\'m out of anger now, go away!' % self.qq)
                        return QQConstants.qq_stop_tag, '{}'
                else:
                    logging.warning(r.text)
                    time.sleep(30)
                    return QQConstants.qq_other_tag, '{}'
        else:
            logging.warning(r.text)
            return QQConstants.qq_other_tag, '{}'

    @staticmethod
    def parse_profile(encoded):
        profile = Profile()
        # TODO:Add comments
        profile.uin = encoded['uin']
        # nickname
        profile.nickname = encoded['nickname']
        # spacename
        profile.spacename = encoded['spacename']
        profile.desc = encoded['desc']
        profile.signature = encoded['signature']
        profile.avatar = encoded['avatar']
        profile.sex_type = encoded['sex_type']
        profile.sex = encoded['sex']
        profile.animalsign_type = encoded['animalsign_type']
        profile.animalsign = encoded['animalsign']
        profile.constellation_type = encoded['constellation_type']
        profile.constellation = encoded['constellation']
        profile.age_type = encoded['age_type']
        profile.age = encoded['age']
        profile.islunar = encoded['islunar']
        profile.birthday_type = encoded['birthday_type']
        profile.birthyear = encoded['birthyear']
        profile.birthday = encoded['birthday']
        profile.bloodtype = encoded['bloodtype']
        profile.address_type = encoded['address_type']
        profile.country = encoded['country']
        profile.province = encoded['province']
        profile.city = encoded['city']
        profile.home_type = encoded['home_type']
        profile.hco = encoded['hco']
        profile.hp = encoded['hp']
        profile.hc = encoded['hc']
        profile.marriage = encoded['marriage']
        profile.lover = encoded['lover']
        profile.career = encoded['career']
        profile.company = encoded['company']
        profile.compaddr_type = encoded['compaddr_type']
        profile.cco = encoded['cco']
        profile.cp = encoded['cp']
        profile.cc = encoded['cc']
        profile.cb = encoded['cb']
        profile.mailname = encoded['mailname']
        profile.mailcellphone = encoded['mailcellphone']
        profile.mailaddr = encoded['mailaddr']
        profile.like_number = encoded['like_number']
        return profile

    def friends(self, qq, proxy=None):
        """
        Get friends for the given qq number
        if success, return 1
        if exception or failure meet, return 0
        if kicked out, return 2
        :param qq:
        :return:
        """
        headers = {
            'User-Agent': self.userAgent,
            'Referer': 'http://cm.qzs.qq.com/qzone/app/mood_v6/html/index.html'
        }
        page_num = 20
        par = {
            'uin': qq,
            'ftype': 0,
            'sort': 0,
            'pos': 0,
            'num': page_num,
            'replynum': 100,
            'g_tk': self.g_tk,
            'code_version': 1,
            'format': 'json',
            'need_private_comment': 1
        }
        try:
            r = self.requests.get(self.chat_url, params=par,
                                  proxies=proxy, timeout=QQConstants.qq_friends_time_out)
            json_response = r.json()
        except Exception as ex:
            logging.warning('Friends first fetch failed[Network]:%s' % qq)
            logging.warning(ex)
            return 0, None
        msg = json_response['message']
        if msg == u'对不起,主人设置了保密,您没有权限查看':
            return 1, set()
        if 'total' not in json_response:
            if msg == u'使用人数过多，请稍后再试':
                # sleep for ten minutes
                time.sleep(QQConstants.qq_long_sleep)
                return 0, set()
            else:
                qq_logger.warning('[QQ]%s Friends:SERIOUS WARNING...' % self.qq)
                time.sleep(QQConstants.manager_kickout_sleep)
                self.re_login = True
                logging.warning(r.text)
                self.monitor_login()
                if self.login_tag != QQConstants.qq_stage_running:
                    return 2, set()
                else:
                    return 0, set()
        else:
            total = json_response['total']
            pages = int((total + page_num - 1) / page_num)
            # fix bug
            if pages == 0:
                return 1, set()
            friends = self.parse_json(json_response, qq, 1, pages)
            for index in range(1, pages):
                if index + 1 <= self.max_page:
                    friends.extend(self.next_page(qq, index, pages, proxy=proxy))
            friend_set = set(friends)
            if qq in friend_set:
                friend_set.remove(qq)
            return 1, friend_set

    def next_page(self, qq, index, pages, proxy=None):
        """
        get next page
        :param qq:
        :param index:
        :param pages:
        :return:
        """
        par = {
            'uin': qq,
            'ftype': 0,
            'sort': 0,
            'pos': 20 * index,
            'num': 20,
            'replynum': 100,
            'g_tk': self.g_tk,
            'code_version': 1,
            'format': 'json',
            'need_private_comment': 1
        }
        try:
            r = self.requests.get(self.chat_url, params=par,
                                  proxies=proxy, timeout=QQConstants.qq_friends_time_out)
            json_response = r.json()
            friends = self.parse_json(json_response, qq, index + 1, pages)
            return friends
        except Exception as ex:
            # print(r.text)
            logging.warning(ex)
            logging.error('Page %d/%d failed for qq %s' % (index + 1, pages, qq))
            return []

    def parse_json(self, json_response, qq, index, pages):
        """
        Parse json to get the friend list
        :param json_response:
        :param qq:
        :param index:
        :param pages:
        :return:
        """
        logging.info('Parsing page %d/%d' % (index, pages))
        if self.store_json:
            assert self.storage_helper
            self.storage_helper.store_json(qq, index, json_response)
        friends = []
        if 'mentioncount' in json_response:
            return []
        else:
            denial_message = u'对不起,主人设置了保密,您没有权限查看'
            msg = json_response['message']
            if msg == denial_message:
                return []
            if 'msglist' not in json_response:
                return []
            msgs = json_response['msglist']
            if msgs is None:
                return []
            for msg in msgs:
                if 'commentlist' in msg:
                    commentList = msg['commentlist']
                    for comment in commentList:
                        uin = str(comment['uin'])
                        if re.match(filter_pattern, uin):
                            friends.append(uin)
            return friends


if __name__ == '__main__':
    # whole cookies
    # {'pt_local_token': '-1223588271', 'pt_serverip': 'de6a0aab3d3f2b6b',
    # 'ptvfsession': '8f7daee83de80377ba717d33d67e6efe9cb2d079e1ff6fad0ac4b6a4558878606606da855ee578b44e9686a60058530340a15ae77424617e',
    # 'ptdrvs': 'XTb0ufgfO-Tuny3scWZXUPW5YcqZ5RmYm2K*5PVOj5s_', 'supertoken': '441109485',
    # 'ptnick_1531474354': 'e99d92e58e9fe8a18ce6809d',
    # 'pt4_token': 'd5wW5T44nXy*Q8gV6NdENVO8jRUFN5pod2y2YOvzJBQ_',
    # 'RK': 'Cg0ii7JXbN',
    # 'superkey': 'l6i70zEoqGai7uvlw7UoXyjdUy6vMFgl1SxV6k6RoVM_',
    # 'u_1531474354': '@ZuaigFRIa:1465718689:1465718689:e99d92e58e9fe8a18ce6809d:1',
    # 'pt_login_sig': 'Gz6qepVqEcbLUz8Sqel8AhZOKQcA-M0L2Uh02TNbpO-QxECvCvIzxZhxyP-k1ioR',
    # 'uin': 'o1531474354',
    # 'p_skey': 'Ecp7Mh-LqJ6w4AogPIC9g*1qJlTvzeUxdozEkw18BTw_',
    # 'pt2gguin': 'o1531474354', '
    # ptcz': '854a09fb11e582e6622e9ab1b1d15f92ae8a7f0293b473f99e6ba022dfa27ce7',
    # 'pt_clientip': '81990a765d47fa2f', 'confirmuin': '0',
    # 'uikey': '02ff1e45a66a11476bc783ed2f9950169b3eec7e77047194e47f62a445f5344d',
    # 'ptisp': 'cm', 'skey': '@ZuaigFRIa', 'p_uin': 'o1531474354', 'ETK': '',
    # 'superuin': 'o1531474354'}
    # qq = QQ()
    # qq.cookie_login('obb73SAUhv65CiK-5JF-L1UvfoWM-*PCyFvqRQt6Jm8_','o1531474354')
    """
    qq_cookies = {'p_skey': 'obb73SAUhv65CiK-5JF-L1UvfoWM-*PCyFvqRQt6Jm8_',
                  'p_uin': 'o1531474354'
                  }
    qq.requests.cookies.update(qq_cookies)
    qq.g_tk = qq.gtk()
    print(qq.profile('920328364'))
    """
    qq = QQ(qq='2125598231', pwd="bm508687", nohup=False)
    qq.monitor_login()
    print(qq.profile('1101042571'))
    # qq.logout()
    # print(qq.profile('1181081979'))
