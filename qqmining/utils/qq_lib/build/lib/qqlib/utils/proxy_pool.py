# -*- encoding:utf-8 -*-

import logging
import random
import threading
import time

import requests

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='qq_crawler.log',
                    filemode='w')

proxy_lock = threading.RLock()


class ProxyPool:
    """
    Proxy Pool
    """

    def __init__(self):
        self.url = 'http://dev.kuaidaili.com/api/getproxy'
        self.last_query = 0
        self.interval = 60 * 5
        self.proxies = list()
        self.validate_url = 'http://www.baidu.com'
        self.protocol_header = 'http://'

    def get_proxy(self):
        """
        Get one proxy for current use
        :return:
        """
        with proxy_lock:
            if self.last_query == 0:
                self.proxies = self.get_proxies()
                self.last_query = time.time()
            else:
                if len(self.proxies) == 0 or time.time() - self.last_query > self.interval:
                    self.proxies = self.get_proxies()
                    self.last_query = time.time()
            if len(self.proxies) == 0:
                return None
            return {'http': random.choice(self.proxies)}

    def get_proxies(self):
        """
        Query and get proxies
        :return:
        """
        par = {
            'orderid': 997263314579137,
            'num': 80,
            'area': u'大陆',
            'b_pcchrome': 1,
            'b_pcie': 1,
            'b_pcff': 1,
            'protocol': 2,
            'method': 2,
            'an_ha': 1,
            'an_an': 1,
            'sp2': 1,
            'quality': 1,
            'sort': 1,
            'format': 'json',
            'sep': 1
        }
        try:
            r = requests.get(self.url, params=par)
            result = r.json()
            if result['code'] == 0:
                self.proxies.extend(result['data']['proxy_list'])
            proxy_result = list()
            for proxy in self.proxies:
                if proxy not in proxy_result:
                    if self.validate_proxy({'http': proxy}):
                        proxy_result.append(proxy)
            return proxy_result
        except Exception as ex:
            logging.warning('Failed to get proxies...')
            logging.error(ex)
            return list()

    def validate_proxy(self, proxy):
        try:
            requests.get(self.validate_url, proxies=proxy, timeout=1.5)
            print(proxy)
            logging.info(proxy)
            return True
        except Exception as ex:
            return False


if __name__ == '__main__':
    pool = ProxyPool()
    request_url = 'http://httpbin.org/ip'
    r = requests.get(request_url, proxies=pool.get_proxy())
    print(r.json()['origin'])
