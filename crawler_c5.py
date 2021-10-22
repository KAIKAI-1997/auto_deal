# -*- coding = utf-8 -*-
# @Time : 10/21/2021 5:00 PM
# @Author : Chongkai YAN
# @File : crawler_c5.py
# @Software: PyCharm

import time
import random

import urllib  # 获得网页源码
import urllib.request
import json
import requests
import mysql.connector
from mysql.connector import errorcode
from crawler_buff import askURL,create_database,SQLinit,SQLstore,SQLdestinct

if __name__ == '__main__':
