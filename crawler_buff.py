# -*- coding = utf-8 -*-
# @Time : 8/11/2021 3:50 PM
# @Author : Chongkai YAN
# @File : crawler_buff.py
# @Software: PyCharm

import time
import random

import urllib  # 获得网页源码
import urllib.request
import json
import requests
import mysql.connector
from mysql.connector import errorcode

ip_pool = (
    '122.234.27.206',
    '114.98.114.20',
    '117.94.222.149',
    '211.65.197.93',
    '47.100.14.22',
    '121.230.210.212',
    '182.84.144.251',
    '117.88.35.71',
    '117.94.222.233',
    '182.84.144.71',
    '223.244.179.31',
    '118.117.189.68',
    '175.7.199.232',
    '182.84.145.162',
    '112.95.26.204',
    '182.84.144.238',
    '182.84.145.180',
    '117.65.1.48',
    '117.35.255.250',
    '211.65.197.93',
    '60.185.205.72',
    '114.98.114.141',
    '121.232.194.229',
    '113.254.178.224'
)

mycookie_buff = 'Device-Id=HT5QXhXmfByW01TzFPud; _ga=GA1.2.957548081.1627891129; _ntes_nnid=4ec3e4c4693333fd6a6746ac8e71ec2d,1628586973786; _ntes_nuid=4ec3e4c4693333fd6a6746ac8e71ec2d; game=csgo; _gid=GA1.2.1633472767.1634770689; NTES_YD_SESS=D5Gu57ES4HOZ1ySBSXEnPnZ0cbEJ7_4sjssrmiU7ILzFMA0BJw8GfeOF4XHAYEQloz.Bh5LssR6fXQU9Jwu9_PQKf7ZMETMKpa5Wu67sUIl6Xpp5ziYYIktXifgjyyFUkgxdhKZe1lePSxm0HRIHowzcypKJ2jxYxmHC6FP7j3wk1vB2g5H1blau7P_KTAZTYTeBuzX9XwQdIMc14ZJv97r_kedWaFCOF58p.hXJeFPFc; S_INFO=1634770718|0|0&60##|1-3104989190; P_INFO=1-3104989190|1634770718|1|netease_buff|00&99|null&null&null#US&null#10#0|&0||1-3104989190; remember_me=U1091746795|7YmIh5L27iiUu6WofbnBodDN97HjcAeC; session=1-pAnUvBbpFmEEXlOo1ha121NTFZYmFv61WBo5K8MXGNSN2044470451; Locale-Supported=zh-Hans; _gat_gtag_UA_109989484_1=1; csrf_token=IjQzNWMyYWFhOWZmMWUzNTA3MWQzOGMwYjRlZmRjMmVkNjI4MDljM2Ui.FFIxaQ.9lrrj49Pq9Cu_M5e2ZuWC6pTHqM'

urlpool = {
    'pistol': 'https://buff.163.com/api/market/goods?game=csgo&page_num=%d&category_group=pistol&quality=normal&use_suggestion=0&trigger=undefined_trigger',
    'knife': 'https://buff.163.com/api/market/goods?game=csgo&page_num=%d&category_group=knife&quality=normal',
    'rifle': 'https://buff.163.com/api/market/goods?game=csgo&page_num=%d&category_group=rifle&quality=normal',
    'smg': 'https://buff.163.com/api/market/goods?game=csgo&page_num=%d&category_group=smg&quality=normal',
    'shotgun': 'https://buff.163.com/api/market/goods?game=csgo&page_num=%d&category_group=shotgun&quality=normal',
    'machiengun': 'https://buff.163.com/api/market/goods?game=csgo&page_num=%d&category_group=machinegun&quality=normal',
    'hands': 'https://buff.163.com/api/market/goods?game=csgo&page_num=%d&category_group=hands',
    'sticker': 'https://buff.163.com/api/market/goods?game=csgo&page_num=%d&category_group=sticker&quality=normal',
    'player': 'https://buff.163.com/api/market/goods?game=csgo&page_num=%d&category_group=type_customplayer&quality=normal',
    'chest': 'https://buff.163.com/api/market/goods?game=csgo&page_num=%d&category_group=other&quality=normal'
}

#market_hash_name, name,sell_min_price_buff, sell_number_buff, steam_price, steam_price_cny
DB_NAME = 'GOODS_data'
table_name = 'buff_goods'
Table = (
    "CREATE TABLE `buff_goods` ("
    " `game` varchar(10) NOT NULL,"
    " `type` varchar(15),"
    " `id_buff` int(15) NOT NULL,"
    " `market_hash_name` varchar(90) NOT NULL,"
    " `name` varchar(50) NOT NULL,"
    " `sell_min_price_buff` varchar(10) NOT NULL,"
    " `sell_number_buff` varchar(10) NOT NULL,"
    " `steam_price` varchar(10) NOT NULL,"
    " `steam_price_cny` varchar(10) NOT NULL"
    ") ENGINE=InnoDB"
)


def askURL(URL):
    time.sleep(4)

    myIp = random.choice(ip_pool)

    headers = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
        'Connection': 'keep-alive',
        'Cookie': mycookie_buff,
        'Host': 'buff.163.com',
        'Referer': 'https://buff.163.com/market/csgo',
        'sec-ch-ua': '"Google Chrome";v="93", " Not;A Brand";v="99", '
                     '"Chromium";v="93"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': 'Windows',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) Chrome'
                      '/93.0.4577.82 Safari/537.36',
        'X-Requested-With': 'XMLHttpRequest'
    }

    try:
        response = requests.get(url=URL, headers=headers,
                                proxies={'http': myIp}).text
    except urllib.error.URLError:
        print("banned")

    return response


def getData(urlbase, page):
    urlbase = urlbase % page
    data = askURL(urlbase)

    return str(data)


def parseBUFFdata(myjson,buff_type):
    """game, type, id_buff, market_hash_name, name,
    sell_min_price_buff, sell_number_buff, steam_price, steam_price_cny"""
    data_json = json.loads(myjson)
    thisdata = []
    i = 0
    for row in data_json['data']['items']:
        thisdata.append([])
        thisdata[i].append(row['game'])
        thisdata[i].append(buff_type)
        thisdata[i].append(row['id'])
        thisdata[i].append(row['market_hash_name'])
        thisdata[i].append(row['name'])
        thisdata[i].append(row['sell_min_price'])
        thisdata[i].append(row['sell_num'])
        thisdata[i].append(row['goods_info']['steam_price'])
        thisdata[i].append(row['goods_info']['steam_price_cny'])

        i += 1

    return thisdata


def create_database(cursor):
    # try to create database
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)


def SQLinit():
    cnx = mysql.connector.connect(user='root', password='321456',
                                  host='localhost')
    cursor = cnx.cursor()

    # if database not exit, create
    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("Database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)
            exit(1)

    # if table not exist, create
    table_description = Table
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

    cursor.close()
    cnx.close()


def SQLstore(data_1):
    cnx = mysql.connector.connect(user='root', password='321456',
                                  host='localhost', database='goods_data')
    cursor = cnx.cursor()

    add_data_1 = ("INSERT INTO buff_goods"
                  "(game, type, id_buff, market_hash_name, name,"
                  "sell_min_price_buff, sell_number_buff, "
                  "steam_price, steam_price_cny) VALUES("
                  "%s, %s, %s, %s, %s, "
                  "%s, %s, %s, %s)")

    data_1 = tuple(data_1)
    cursor.execute(add_data_1, data_1)
    cnx.commit()

    cursor.close()
    cnx.close()


def SQLdestinct():
    cnx = mysql.connector.connect(user='root', password='321456',
                                  host='localhost', database='goods_data')
    cursor = cnx.cursor(buffered=True)
    query = ("CREATE TABLE goods_data.temp_table LIKE buff_goods;"
             "INSERT INTO goods_data.temp_table"
                "SELECT DISTINCT * FROM buff_goods;"
             "RENAME TABLE buff_goods TO old_buff_goods,"
                           "temp_table TO buff_goods;"
             "DROP TABLE old_buff_goods;"
            )

    cursor.execute(query)
    print("duplicate eliminated")
    cursor.close()
    cnx.close()


if __name__ == '__main__':
    SQLinit()
    # ['pistol', 'knife', 'rifle', 'smg', 'shotgun', 'machiengun',
    # 'hands', 'sticker', 'player', 'chest']
    for key in ['player', 'chest']:
        url = urlpool[key]
        print("process ", key)
        last_id = 0
        for i in range(1, 500):
            json_ori = getData(url, i)
            print(json_ori)
            data_20 = parseBUFFdata(json_ori, key)
            for data_1 in data_20:
                SQLstore(data_1)

            if last_id == json.loads(json_ori)['data']['items'][0]['id']:
                break
            last_id = json.loads(json_ori)['data']['items'][0]['id']

    SQLdestinct()
