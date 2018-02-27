# -*- coding: utf-8 -*-
# @Time    : 2018/2/27 8:57
# @Author  : wf
# @简介    : 区域查询:每30分钟查询一次实时表，查询减量车（is_out字段）并统计各个区域减量车数量，写入表tb_area_min,tb_area_mid,tb_area_max的area_out_num字段
# @File    : tb_area_out_num.py
import redis
from DBConn import mysql_conn
import logging
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import numpy
bound = {'ymi': 29.745676, 'yma': 30.56237, 'xmi': 119.436459, 'xma': 120.704416}
lon = (bound['xma'] - bound['xmi']) / 1000
lat = (bound['yma'] - bound['ymi']) / 1000


def insert_area_out_num_mid(area_out_num, cur, conn):
    # conn = MySQLdb.connect(host='localhost', user='bike', passwd='bike', db='bike', port=6052)
    # cur = conn.cursor()
    insert_sql = 'update tb_area_mid set area_out_num = %s where area_id = %s'
    tup_list = []
    for i in area_out_num:
        AN = area_out_num[i]
        AI = i
        tup = (AN, AI)
        tup_list.append(tup)
    cur.executemany(insert_sql, tup_list)
    conn.commit()


def insert_area_num_max(area_num, cur, conn):
    # conn = MySQLdb.connect(host='localhost', user='bike', passwd='bike', db='bike', port=6052)
    # cur = conn.cursor()
    insert_sql = 'update tb_area_max set area_out_num = %s where area_id = %s'
    tup_list = []
    for i in area_num:
        AN = area_num[i]
        AI = i
        tup = (AN, AI)
        tup_list.append(tup)
    cur.executemany(insert_sql, tup_list)
    conn.commit()


def get_data_mid():
    mid_num = {}
    mid_num1 = {}
    max_num = {}
    max_num1 = {}
    for i in range(1, 75):
        mid_num[i] = []
        mid_num1[i] = 0
    for i in range(1, 25):
        max_num[i] = []
        max_num1[i] = 0
    conn = mysql_conn.get_bike_connection()
    # conn = MySQLdb.connect(host='172.18.106.159', user='bike', passwd='tw_85450077', db='bike', port=3306)
    cur = conn.cursor()
    sql = 'select * from tb_area_min'
    cur.execute(sql)
    for i in cur:
        mid_num[int(i[2])].append(int(i[9]))
    for i in mid_num:
        list = mid_num[i]
        a = numpy.sum(list)
        mid_num1[i] = a
    insert_area_out_num_mid(mid_num1, cur, conn)
    sql1 = 'select * from tb_area_mid'
    cur.execute(sql1)
    for i in cur:
        max_num[int(i[2])].append(int(i[9]))
    for i in max_num:
        list = max_num[i]
        a = numpy.sum(list)
        max_num1[i] = a
    insert_area_num_max(max_num1, cur, conn)
    cur.close()
    conn.close()


def get_mapindex():
    mi_dic = {}
    reg = []
    reg1 = []
    pool = redis.ConnectionPool(host='172.18.106.157', port=6068, db=1)
    r = redis.StrictRedis(connection_pool=pool)
    bt = time.time()
    for t in range(1, 1001):
        for n in range(1, 1001):
            reg.append("{0},{1}".format(n, t))
            reg1.append((n, t))
    result = r.mget(reg)
    for i in range(0, 1000000):
        mi_dic[reg1[i]] = int(result[i])
    et = time.time()
    print 'index cost {0}'.format(et-bt)
    return mi_dic


def insert_area_num(out_num):
    conn = mysql_conn.get_bike_connection()
    # conn = MySQLdb.connect(host='172.18.106.159', user='bike', passwd='tw_85450077', db='bike', port=3306)
    cur = conn.cursor()
    insert_sql = 'update tb_area_min set area_out_num = %s where area_id = %s'
    tup_list = []
    for i in out_num:
        AON = out_num[i]
        AI = i
        tup = (AON, AI)
        tup_list.append(tup)
    cur.executemany(insert_sql, tup_list)
    conn.commit()
    cur.close()
    conn.close()


def get_result():
    out_num = {}
    for i in range(1, 3288):
        out_num[i] = 0
    conn = mysql_conn.get_bike_connection()
    # conn = MySQLdb.connect(host='172.18.106.159', user='bike', passwd='tw_85450077', db='bike', port=3306)
    cur = conn.cursor()
    sql = 'select * from tb_bike_status_realtime'
    cur.execute(sql)
    global bpund, lon, lat
    for i in cur:
        if i[3] == None or i[4] == None:
            continue
        logi = i[3]
        lati = i[4]
        ind_x = int((logi - bound['xmi']) / lon) + 1
        ind_y = int((lati - bound['ymi']) / lat) + 1
        is_out = i[9]
        if is_out == None:
            continue
        if 1 <= ind_x <= 1000 and 1 <= ind_y <= 1000:
            global map_index
            reg = map_index[(ind_x, ind_y)]
            if reg != 0:
                out_num[reg] += is_out
    return out_num


def main():
    now = datetime.now()
    print 'time is: %s' % now
    bt = time.time()
    out_num = get_result()
    et = time.time()
    print 'dict cost {0}'.format(et-bt)
    insert_area_num(out_num)
    get_data_mid()


map_index = get_mapindex()
if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    # scheduler.add_job(tick, 'interval', days=1)
    scheduler.add_job(main, 'cron', minute='0,30')
    try:
        scheduler.start()
    except SystemExit:
        pass
