# -*- coding: utf-8 -*-
# @Time    : 2018/2/27 8:57
# @Author  : wf
# @简介    : 周转量(每日订单数量和发生订单的车辆数量)---每天统计一次
# @File    : search_gather.py
from DBConn import mysql_conn
import matplotlib.path as mpltPath
import time
from datetime import datetime
from datetime import timedelta
from apscheduler.schedulers.blocking import BlockingScheduler
import logging

dic = {}
num = {}
company = ['qibei', 'ofo', 'mb', 'hellobike', 'mt', 'yonganxing', 'xiaoming']
# area = ['binjiang', 'gongshu', 'jianggan', 'shangcheng', 'xiacheng', 'xiaoshan','xihu', 'yuhang', 'jingqu', 'xiasha']
area = ['jingqu', 'xiasha']


def load_area(filename):
    fp = open(filename, 'r')
    pl = []
    for line in fp.readlines():
        poly = line.split(',')
        cnt = 0
        px, py = 0, 0
        for p in poly:
            if cnt % 2 == 0:
                px = float(p)
            else:
                py = float(p)
                pl.append([px, py])
            cnt += 1
    cur_path = mpltPath.Path(pl)
    return cur_path


def write_db():
    conn = mysql_conn.get_bike_connection()
    cursor = conn.cursor()

    sql = "select CompanyID, Longitude, Latitude from tb_bike_status_realtime"
    cursor.execute(sql)

    global dic
    global num
    not_cnt = 0
    for item in cursor.fetchall():
        point = [item[1], item[2]]
        cid = item[0]
        inside = False
        for a in dic.keys():
            inside = dic[a].contains_point(point)
            if inside:
                num[a][cid] += 1
                break
        if not inside:
            # print point
            not_cnt += 1

    print "not in", not_cnt
    for area in num.keys():
        test = num[area]
        print area
        for name in test:
            print name, test[name]


def main():
    global dic
    path_binjiang = load_area('coord/binjiang.txt')
    path_gongshu = load_area('coord/gongshu.txt')
    path_jianggan = load_area('coord/jianggan.txt')
    path_shangcheng = load_area('coord/shangcheng.txt')
    path_xiacheng = load_area('coord/xiacheng.txt')
    path_xiaoshan = load_area('coord/xiaoshan.txt')
    path_xihu = load_area('coord/xihu.txt')
    path_yuhang = load_area('coord/yuhang.txt')
    path_jingqu = load_area('coord/jingqu.txt')
    path_xiasha = load_area('coord/xiasha.txt')

    for a in area:
        num[a] = {}
        for name in company:
            num[a][name] = 0

    #dic['binjiang'] = path_binjiang
    #dic['gongshu'] = path_gongshu
    #dic['jianggan'] = path_jianggan
    #dic['shangcheng'] = path_shangcheng
    #dic['xiaoshan'] = path_xiaoshan
    #dic['xiacheng'] = path_xiacheng
    #dic['xihu'] = path_xihu
    #dic['yuhang'] = path_yuhang
    dic['xiasha'] = path_xiasha
    dic['jingqu'] = path_jingqu

    write_db()


def check_bike(t1, t2, t):
    conn = mysql_conn.get_bike_connection()
    cursor = conn.cursor()
    bike_status = {}
    bike_fact = {}
    for name in company:
        bike_status[name] = set()
    bike = set()

    sql = "select bicycleno from tb_bike"
    cursor.execute(sql)
    for item in cursor.fetchall():
        bike.add(item[0])

    sql = "select companyid, bicycleno, orderid from tb_bike_gps_{2} " \
          "where state = 0 and positiontime >= '{0}' and positiontime < '{1}'".format(t1, t2, t)
    bt = time.clock()
    cursor.execute(sql)

    for item in cursor.fetchall():
        bicycleno = item[1]
        name = item[0]
        bike_status[name].add(bicycleno)
    et = time.clock()
    print et - bt

    for name in company:
        bike_fact[name] = bike_status[name] & bike
        print name, len(bike_fact[name])

    cursor.close()
    conn.close()
    return bike_fact


def check_orderid(t1, t2, t):
    conn = mysql_conn.get_bike_connection()
    cursor = conn.cursor()
    orders = {}
    for name in company:
        orders[name] = set()
    sql = "select companyid, bicycleno, orderid from tb_bike_gps_{2} " \
          "where state = 0 and positiontime >= '{0}' and positiontime < '{1}'".format(t1, t2, t)
    bt = time.clock()
    cursor.execute(sql)

    for item in cursor.fetchall():
        orderid = item[2]
        name = item[0]
        if orderid != '':
            orders[name].add(orderid)
    et = time.clock()
    print et - bt
    for name in company:
        print name, len(orders[name])
    cursor.close()
    conn.close()
    return orders


def insert_data(bike_fact, orders, DT):
    st = DT.strftime("%Y-%m-%d")
    conn = mysql_conn.get_bike_connection()
    cur = conn.cursor()
    sql = 'insert into tb_static(CompanyId,Order_num,Vehicle_num,DBtime) values(%s,%s,%s,%s)'
    tup_list = []
    for i in bike_fact:
        if i == 'xiaoming' or i == 'qibei':
            continue
        CI = i
        On = len(orders[i])
        Vn = len(bike_fact[i])
        # DT = time.strftime("%Y-%m-%d", time.localtime())
        tup = (CI, On, Vn, st)
        tup_list.append(tup)
    cur.executemany(sql, tup_list)
    conn.commit()
    cur.close()
    conn.close()


def tick():
    now = datetime.now()
    yst = now + timedelta(days=-1)
    st1 = yst.strftime("%Y-%m-%d 00:00:00")
    st2 = now.strftime("%Y-%m-%d 00:00:00")
    st = yst.strftime("%Y%m")[2:]
    print st2
    bike_fact = check_bike(st1, st2, st)
    print "-------------------"
    orders = check_orderid(st1, st2, st)
    insert_data(bike_fact, orders, yst)


if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    # scheduler.add_job(tick, 'interval', days=1)
    scheduler.add_job(tick, 'cron', hour='1')
    try:
        scheduler.start()
    except SystemExit:
        pass
