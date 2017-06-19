#coding=utf-8
#!/usr/bin/env python
# Created by Tina on 2017/5/4


class TweetsItem():
    """ 微博信息 """
    def __init__(self):
        self._id = ''  # 用户ID-微博ID
        self.ID = ''  # 用户ID
        self.Content = ''  # 微博内容
        self.PubTime = ''  # 发表时间
        self.Co_oridinates = ''  # 定位坐标
        self.Tools = ''  # 发表工具/平台
        self.Like = ''  # 点赞数
        self.Comment = ''  # 评论数
        self.Transfer = ''  # 转载数