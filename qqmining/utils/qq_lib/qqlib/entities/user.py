# -*- coding:utf-8 -*-


class User:
    """
    User Class
    """

    def __init__(self, username, pwd):
        self.username = username
        self.pwd = pwd
        self.used = False
        self.healthy = True
