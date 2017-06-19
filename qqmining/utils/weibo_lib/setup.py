# -*- coding:utf-8 -*-

from setuptools import setup

packages = [
    'weibolib',
    'weibolib.entities',
    'weibolib.utils'
]

setup(
    name='weibo_test',
    version='0.12',
    description='A Simple weibo Crawler.',
    author='yjchen',
    author_email='tigercyj@163.com',
    packages=packages,
    package_dir={'weibolib': 'weibolib'},
    license='Apache 2.0',
    zip_safe=False
)
