# -*- coding:utf-8 -*-

import json

"""
QQ State Class
"""


class QQState:
    def __init__(self, qq, state, recover_cnt=0):
        self.qq = qq
        self.state = state
        self.recover_cnt = recover_cnt

    def __repr__(self):
        result = {'qq': self.qq,
                  'state': self.state,
                  'recover_cnt': self.recover_cnt}
        return json.dumps(result)

    def __str__(self):
        return self.__repr__()
