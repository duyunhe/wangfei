# -*- coding: utf-8 -*-
# @Time    : 2018/2/27 8:57
# @Author  : wf
# @简介    : 区域查询:每小时查询一次，更新区域当前自行车数量，写入表tb_bike_area的area_num字段
# @File    : tb_bike_area.py
from DBConn import mysql_conn
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import re
import matplotlib.path as mpltPath
from datetime import datetime


def get_xq_index(cur):
    xq_index = {}
    sql = 'SELECT * from tb_bike_area'
    cur.execute(sql)
    for i in cur:
        t = re.findall(r'\d+', i[3])
        list = []
        for a in range(0, len(t), 4):
            x0 = float('{0}.{1}'.format(t[a], t[a + 1]))
            y0 = float('{0}.{1}'.format(t[a + 2], t[a + 3]))
            list.append((x0, y0))
        xq_index[i[0]] = list
    path_l = []
    for i in xq_index:
        path = mpltPath.Path(xq_index[i])
        path_l.append([path, i])
    return path_l


def judge_region(point, path_l):
    for i in path_l:
        inside = i[0].contains_point(point)
        if inside:
            return i[1]
    return 0


def get_data():
    conn = mysql_conn.get_bike_connection()
    cur = conn.cursor()
    path_l = get_xq_index(cur)
    sql = 'SELECT * from tb_bike_status_realtime where PositionTime > DATE_SUB(Now(),INTERVAL 1 day) '
    cur.execute(sql)
    pl = []   # point list
    for i in cur:
        pl.append((i[3], i[4]))
    sta_num = {}
    for i in path_l:
        sta_num[i[1]] = 0
    for i in pl:
        if i[0] == None or i[1] == None:
            continue
        res = judge_region(i, path_l)
        if res != 0:
            if res not in sta_num:
                sta_num[res] = 1
            else:
                sta_num[res] += 1
    cur.close()
    conn.close()
    return sta_num


def insert_bike_area(sta_num):
    conn = mysql_conn.get_bike_connection()
    cur = conn.cursor()
    insert_sql = 'update tb_bike_area set area_num = %s where area_id = %s '
    tup_list = []
    cnt = 0
    for i in sta_num:
        cnt += 1
        tup = (sta_num[i], i)
        tup_list.append(tup)
    cur.executemany(insert_sql, tup_list)
    conn.commit()
    cur.close()
    conn.close()
    print 'insert !'


def tick():
    now = datetime.now()
    print 'time is: %s' % now
    sta_num = get_data()
    insert_bike_area(sta_num)


if __name__ == '__main__':
    tick()
    logging.basicConfig()
    scheduler = BlockingScheduler()
    # scheduler.add_job(tick, 'interval', days=1)
    scheduler.add_job(tick, 'cron', hour='*')
    try:
        scheduler.start()
    except SystemExit:
        pass
