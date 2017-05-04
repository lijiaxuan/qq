#coding=utf-8
#!/usr/bin/env python
# Created by Tina on 2017/5/4

class InformationItem():
    """个人信息"""
    def __init__(self):
        self._id = ''  # 用户ID
        self.NickName = ''  # 昵称
        self.Gender = ''  # 性别
        self.Province = ''  # 所在省
        self.City = ''  # 所在城市
        self.Signature = ''  # 个性签名
        self.Birthday = ''  # 生日
        self.Num_Tweets = ''  # 微博数
        self.Num_Follows = ''  # 关注数
        self.Num_Fans = ''  # 粉丝数
        self.Sex_Orientation = ''  # 性取向
        self.Marriage = ''  # 婚姻状况
        self.URL = ''  # 首页链接
