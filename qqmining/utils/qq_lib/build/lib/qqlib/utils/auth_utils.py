# -*- coding:utf-8 -*-

import base64
import hmac
from hashlib import sha1

"""
Utils for user authorization
"""


def gen_sig(para_dict, access_key):
    """
    Generate signature
    :param para_dict:
    :param access_key:
    :return:
    """
    sort = sorted(para_dict.items(), key=lambda e: e[0].lower())
    AuStr = ''
    for e in sort:
        AuStr += e[0] + e[1]

    sig = hmac.new(bytes(access_key, encoding='utf8'), AuStr.encode('utf-8'), sha1).digest()
    sig = base64.b64encode(sig)
    return bytes.decode(sig)
