#coding=utf-8
#!/usr/bin/env python
# Created by Tina on 2017/5/4

class FollowsItem(Item):
    """ 关注人列表 """
    def __init__(self):
        self._id = ''  # 用户ID
        self.follows = ''  # 关注