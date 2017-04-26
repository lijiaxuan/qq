# -*- coding:utf-8 -*-

import json
import logging
import datetime
import os
import pickle
from configparser import ConfigParser

from flask import Flask, render_template, request, url_for, redirect, session

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename='qq_monitor.log',
                    filemode='w')

app = Flask(__name__)


@app.route('/search')
def search():
    return render_template('search.html')


@app.route('/about')
def about():
    return render_template('about.html')


@app.route('/research')
def research():
    return render_template('research.html')


if __name__ == '__main__':
    # app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'
    # app.config['SESSION_TYPE'] = 'filesystem'
    app.run(debug=True, host='0.0.0.0', port=8081)
