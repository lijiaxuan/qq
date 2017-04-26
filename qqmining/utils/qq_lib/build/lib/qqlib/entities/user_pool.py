# -*- coding:utf-8 -*-

import threading

"""
User Pool Class
"""

user_lock = threading.Lock()


class UserPool:
    def __init__(self):
        self.user_cnt = 0
        self.user_dict = dict()

    def add_user(self, user):
        with user_lock:
            if user.username not in self.user_dict:
                self.user_dict[user.username] = user
                self.user_cnt += 1

    def get_one_user(self):
        with user_lock:
            for key, value in self.user_dict.items():
                if (not value.used) and value.healthy:
                    value.used = True
                    return value
            return None

    def set_user(self, username):
        with user_lock:
            self.user_dict[username].used = False
            self.user_dict[username].healthy = False
