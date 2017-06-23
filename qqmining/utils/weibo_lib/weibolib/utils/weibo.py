# encoding=utf-8
# ----------------------------------------
# 语言：Python2.7
# 日期：2017-05-01
# 作者：九茶<http://blog.csdn.net/bone_ace>
# 功能：破解四宫格图形验证码，登录m.weibo.cn
# ----------------------------------------

import time
import random
import StringIO
from PIL import Image
from math import sqrt
from ims import ims
import re
import requests
from selenium import webdriver
from selenium.webdriver.remote.command import Command
from selenium.webdriver.common.action_chains import ActionChains
import os
import sys

reload(sys)
sys.setdefaultencoding('utf8')

PIXELS = []


class weiboInfo:
    follow_ids = []
    fan_ids = []

    def __init__(self, weibo='13467408430', pwd='aogan571'):
        self.chromedriver = "C:\Program Files (x86)\Google\Chrome\Application\chromedriver.exe"
        os.environ["webdriver.chrome.driver"] = self.chromedriver
        self.browser = webdriver.Chrome(self.chromedriver)
        self.browser.set_window_size(1050, 840)
        self.login(weibo, pwd)
        self.uid = ''

    def login(self, weibo, pwd):
        self.browser.get('https://passport.weibo.cn/signin/login?entry=mweibo&r=https://weibo.cn/')
        time.sleep(1)
        name = self.browser.find_element_by_id('loginName')
        psw = self.browser.find_element_by_id('loginPassword')
        login = self.browser.find_element_by_id('loginAction')
        name.send_keys(weibo)  # 测试账号
        psw.send_keys(pwd)
        login.click()
        ttype = self.getType(self.browser)  # 识别图形路径
        print('Result: %s!' % ttype)
        self.draw(self.browser, ttype)  # 滑动破解
        time.sleep(8)

    def getExactly(self, im):
        """ 精确剪切"""
        imin = -1
        imax = -1
        jmin = -1
        jmax = -1
        row = im.size[0]
        col = im.size[1]
        for i in range(row):
            for j in range(col):
                if im.load()[i, j] != 255:
                    imax = i
                    break
            if imax == -1:
                imin = i

        for j in range(col):
            for i in range(row):
                if im.load()[i, j] != 255:
                    jmax = j
                    break
            if jmax == -1:
                jmin = j
        return (imin + 1, jmin + 1, imax + 1, jmax + 1)

    def getType(self, browser):
        """ 识别图形路径 """
        ttype = ''
        time.sleep(3.5)
        im0 = Image.open(StringIO.StringIO(self.browser.get_screenshot_as_png()))
        box = self.browser.find_element_by_id('patternCaptchaHolder')
        im = im0.crop((int(box.location['x']) + 10, int(box.location['y']) + 100,
                       int(box.location['x']) + box.size['width'] - 10,
                       int(box.location['y']) + box.size['height'] - 10)).convert('L')
        newBox = self.getExactly(im)
        im = im.crop(newBox)
        width = im.size[0]
        height = im.size[1]
        for png in ims.keys():
            isGoingOn = True
            for i in range(width):
                for j in range(height):
                    if ((im.load()[i, j] >= 245 and ims[png][i][j] < 245) or (
                            im.load()[i, j] < 245 and ims[png][i][j] >= 245)) and abs(ims[png][i][j] - im.load()[
                        i, j]) > 10:  # 以245为临界值，大约245为空白，小于245为线条；两个像素之间的差大约10，是为了去除245边界上的误差
                        isGoingOn = False
                        break
                if isGoingOn is False:
                    ttype = ''
                    break
                else:
                    ttype = png
            else:
                break
        px0_x = box.location['x'] + 40 + newBox[0]
        px1_y = box.location['y'] + 130 + newBox[1]
        PIXELS.append((px0_x, px1_y))
        PIXELS.append((px0_x + 100, px1_y))
        PIXELS.append((px0_x, px1_y + 100))
        PIXELS.append((px0_x + 100, px1_y + 100))
        return ttype

    def move(self, browser, coordinate, coordinate0):
        """ 从坐标coordinate0，移动到坐标coordinate """
        time.sleep(0.05)
        length = sqrt((coordinate[0] - coordinate0[0]) ** 2 + (coordinate[1] - coordinate0[1]) ** 2)  # 两点直线距离
        if length < 4:  # 如果两点之间距离小于4px，直接划过去
            ActionChains(browser).move_by_offset(coordinate[0] - coordinate0[0],
                                                 coordinate[1] - coordinate0[1]).perform()
            return
        else:  # 递归，不断向着终点滑动
            step = random.randint(3, 5)
            x = int(step * (coordinate[0] - coordinate0[0]) / length)  # 按比例
            y = int(step * (coordinate[1] - coordinate0[1]) / length)
            ActionChains(browser).move_by_offset(x, y).perform()
            self.move(browser, coordinate, (coordinate0[0] + x, coordinate0[1] + y))

    def draw(self, browser, ttype):
        """ 滑动 """
        if len(ttype) == 4:
            px0 = PIXELS[int(ttype[0]) - 1]
            login = browser.find_element_by_id('loginAction')
            ActionChains(browser).move_to_element(login).move_by_offset(
                px0[0] - login.location['x'] - int(login.size['width'] / 2),
                px0[1] - login.location['y'] - int(login.size['height'] / 2)).perform()
            browser.execute(Command.MOUSE_DOWN, {})

            px1 = PIXELS[int(ttype[1]) - 1]
            self.move(browser, (px1[0], px1[1]), px0)

            px2 = PIXELS[int(ttype[2]) - 1]
            self.move(browser, (px2[0], px2[1]), px1)

            px3 = PIXELS[int(ttype[3]) - 1]
            self.move(browser, (px3[0], px3[1]), px2)
            browser.execute(Command.MOUSE_UP, {})
        else:
            print
            'Sorry! Failed! Maybe you need to update the code.'

    def parseFollows(self, uid):
        time.sleep(1)
        url = "https://weibo.cn/%s/follow" % uid
        self.browser.get(url)
        follows = self.browser.find_elements_by_xpath('//a[text()="关注他" or text()="关注她"]')
        for ele in follows:
            user_url = ele.get_attribute('href')
            uid = re.findall('uid=(\d+)', user_url, re.S)
            self.follow_ids.append(uid[0])
        next_url = self.browser.find_elements_by_xpath('//a[text()="下页"]')
        if len(next_url) != 0:
            print
            next_url[0].get_attribute('href')
            self.parseFollows(next_url[0].get_attribute('href'))
        else:
            return

    def parseFollowsURL(self, url):
        self.browser.get(url)
        fans = self.browser.find_elements_by_xpath('//a[text()="关注他" or text()="关注她"]')
        for ele in fans:
            user_url = ele.get_attribute('href')
            uid = re.findall('uid=(\d+)', user_url, re.S)
            self.fan_ids.append(uid[0])
        next_url = self.browser.find_elements_by_xpath('//a[text()="下页"]')
        if len(next_url) != 0:
            print
            next_url[0].get_attribute('href')
            self.parseFollowsURL(next_url[0].get_attribute('href'))
        else:
            return

    def parseFans(self, uid):
        time.sleep(1)
        url = "https://weibo.cn/%s/fans" % uid
        self.browser.get(url)
        fans = self.browser.find_elements_by_xpath('//a[text()="关注他" or text()="关注她"]')
        for ele in fans:
            user_url = ele.get_attribute('href')
            uid = re.findall('uid=(\d+)', user_url, re.S)
            self.fan_ids.append(uid[0])
        next_url = self.browser.find_elements_by_xpath('//a[text()="下页"]')
        if len(next_url) != 0:
            print
            next_url[0].get_attribute('href')
            self.parseFansURL(next_url[0].get_attribute('href'))
        else:
            return

    def parseFansURL(self, url):
        self.browser.get(url)
        fans = self.browser.find_elements_by_xpath('//a[text()="关注他" or text()="关注她"]')
        for ele in fans:
            user_url = ele.get_attribute('href')
            uid = re.findall('uid=(\d+)', user_url, re.S)
            self.fan_ids.append(uid[0])
        next_url = self.browser.find_elements_by_xpath('//a[text()="下页"]')
        if len(next_url) != 0:
            print
            next_url[0].get_attribute('href')
            self.parseFansURL(next_url[0].get_attribute('href'))
        else:
            return

    def parseInfo(self, uid):
        url = "https://weibo.cn/%s/info" % uid
        self.uid = uid
        self.browser.get(url)
        text1 = self.browser.find_elements_by_xpath('//body/div[@class="c"]')
        if len(text1) <= 3:
            # 不存在该微博号对应的内容
            print('不存在该微博号对应的内容')
            return 1, '{}'
        content = []
        for te in text1:
            tmp = te.text
            content.append(";".join(tmp.split()))
        contents = ";".join(content)
        print
        contents

        nickname = re.findall('昵称[：:]?(.*?);'.decode('utf8'), contents)
        gender = re.findall('性别[：:]?(.*?);'.decode('utf8'), contents)
        place = re.findall('地区[：:]?(.*?);'.decode('utf8'), contents)
        briefIntroduction = re.findall('简介[：:]?(.*?);'.decode('utf8'), contents)
        birthday = re.findall('生日[：:]?(.*?);'.decode('utf8'), contents)
        sexOrientation = re.findall('性取向[：:]?(.*?);'.decode('utf8'), contents)
        sentiment = re.findall('感情状况[：:]?(.*?);'.decode('utf8'), contents)
        vipLevel = re.findall('会员等级[：:]?(.*?);'.decode('utf8'), contents)
        authentication = re.findall('认证[：:]?(.*?);'.decode('utf8'), contents)
        url = re.findall('互联网[：:]?(.*?);'.decode('utf8'), contents)
        data = {
            "weiboid": uid,
            "nickname": nickname,
            "gender": gender,
            "place": place,
            "briefIntroduction": briefIntroduction,
            "birthday": birthday,
            "sexOrientation": sexOrientation,
            "sentiment": sentiment,
            "vipLevel": vipLevel,
            "authentication": authentication,
            "url": url
        }
        return 0, data


if __name__ == '__main__':
    weibo_test = weibo_info()
    # weibo_test.parseFollows(3803606068)
    # weibo_test.parseFans(3803606068)
    weibo_test.parseInfo(1111)
    weibo_test.browser.close()
