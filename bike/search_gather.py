# -*- coding: utf-8 -*-
# @Time    : 2018/2/27 8:57
# @Author  : wf
# @简介    : 车辆OD图--tb_bike_odgraph和车辆OD统计情况--tb_bike_odgraph_statis
# @File    : search_gather.py
from DBConn import mysql_conn
import numpy as np
from datetime import datetime
from datetime import timedelta
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import redis


def get_xiaoqu(Dep, Dest):  # 出发点和到达点所在小方块（1000*1000）
    bound = {'ymi': 29.745676, 'yma': 30.56237, 'xmi': 119.436459, 'xma': 120.704416}
    lon = (bound['xma'] - bound['xmi']) / 1000
    lat = (bound['yma'] - bound['ymi']) / 1000
    dep_xq = []
    dest_xq = []
    bicycle = []
    for i in range(0, len(Dep)):
        x = int((Dep[i][0] - bound['xmi']) / lon) + 1
        y = int((Dep[i][1] - bound['ymi']) / lat) + 1
        x1 = int((Dest[i][0] - bound['xmi']) / lon) + 1
        y1 = int((Dest[i][1] - bound['ymi']) / lat) + 1
        if 0 < x <= 1000 and 0 < y <= 1000 and 0 < x1 <= 1000 and 0 < y1 <= 1000:
            dep_xq.append((x, y))
            dest_xq.append((x1, y1))
            bicycle.append(Dep[i][2])
    print 'get ten region'
    res = ten_start_region(dep_xq, dest_xq, bicycle)
    return res


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
        if int(result[i]) != 0:
            mi_dic[reg1[i]] = int(result[i])
    et = time.time()
    print 'index cost {0}'.format(et-bt)
    return mi_dic


def insert_OD(d_ten):
    conn = mysql_conn.get_bike_connection()
    # conn = MySQLdb.connect(host='172.18.106.159', user='bike', passwd='tw_85450077', db='bike', port=3306)
    cur = conn.cursor()
    insert_sql = 'insert into tb_bike_odgraph (OrientID, DestID, Count, DBtime) values(%s,%s,%s,%s) '
    tup_list = []
    for i in d_ten:
        OD = i
        t = d_ten[i]
        cnt = 0
        for k in range(0, len(t)):
            DI = t[k][0]
            if DI == OD:
                continue
            cnt += 1
            C = t[k][1]
            DT = time.strftime("%Y-%m-%d", time.localtime())
            tup = (OD, DI, C, DT)
            tup_list.append(tup)
            if cnt == 10:
                break
    cur.executemany(insert_sql, tup_list)
    conn.commit()
    cur.close()
    conn.close()


def insert_bicycle_num(by_dict):
    conn = mysql_conn.get_bike_connection()
    # conn = MySQLdb.connect(host='172.18.106.159', user='bike', passwd='tw_85450077', db='bike', port=3306)
    cur = conn.cursor()
    insert_sql = 'insert into tb_bike_odgraph_statis (OrientID, Number, DBtime) values(%s,%s,%s)'
    tup_list = []
    for i in by_dict:
        OD = i
        nm = by_dict[i]
        DT = time.strftime("%Y-%m-%d", time.localtime())
        tup = (OD, nm, DT)
        tup_list.append(tup)
    cur.executemany(insert_sql, tup_list)
    conn.commit()
    cur.close()
    conn.close()


def static_bc_num(bno, ten_xq):
    n_bno = {}
    for i in bno:
        if i in ten_xq:
            n_bno[i] = len(set(bno[i]))
    insert_bicycle_num(n_bno)


def ten_start_region(dep_xq, dest_xq, bc_no):  # 订单发起的10个区域
    global map_i
    i_d = {}
    dest = {}
    bno = {}
    cnt = 0
    num = []
    new_num = []
    xq = []
    for i in dep_xq:
        if i in map_i:
            t = map_i[i]
            if t not in i_d:
                i_d[t] = 1
                dest[t] = []
                bno[t] = []
                dest[t].append(dest_xq[cnt])
                bno[t].append(bc_no[cnt])
            else:
                i_d[t] += 1
                dest[t].append(dest_xq[cnt])
                bno[t].append(bc_no[cnt])
        cnt += 1
    for i in i_d:
        num.append(i_d[i])
        new_num.append(i_d[i])
        xq.append(i)
    ten_xq = []
    for i in range(0, 10):
        mnum = np.max(num)
        con = new_num.count(mnum)
        # print con
        if i == 0:
            sum = con
        else:
            sum = con + sum
        if con > 1:
            for q, v in enumerate(new_num):
                if v == mnum:
                    ten_xq.append(xq[q])
                    num.remove(mnum)
        else:
            ind = new_num.index(mnum)
            ten_xq.append(xq[ind])
            num.remove(mnum)
        if sum >= 10:
            break
    static_bc_num(bno, ten_xq)
    result = dest_region_n(dest, ten_xq, map_i)
    return result


def dest_region_n(dest, ten_xq, map_i):  # 10个发起区域中，聚集的到达区域
    result = {}
    for i in dest:
        if i in ten_xq:
            result[i] = []
            m_d = {}
            for t in dest[i]:
                if t in map_i:
                    map = map_i[t]
                    if map not in m_d:
                        m_d[map] = 1
                    else:
                        m_d[map] += 1
            num1 = []
            new_num1 = []
            xq1 = []
            for k in m_d:
                num1.append(m_d[k])
                new_num1.append(m_d[k])
                xq1.append(k)
            ten_xq1 = []
            if len(m_d) > 11:
                dest_num = 11
            else:
                dest_num = len(m_d)
            for n in range(0, dest_num):
                mnum = np.max(num1)
                con = new_num1.count(mnum)
                # print con
                if n == 0:
                    sum = con
                else:
                    sum = con + sum
                if con > 1:
                    for q, v in enumerate(new_num1):
                        if v == mnum:
                            ten_xq1.append(xq1[q])
                            num1.remove(mnum)
                            result[i].append([xq1[q], mnum])
                else:
                    ind = new_num1.index(mnum)
                    ten_xq1.append(xq1[ind])
                    num1.remove(mnum)
                    result[i].append([xq1[ind], mnum])
                if sum >= 11 or len(num1) == 0:
                    break
    return result


def unix2date(now):    # 当前时间datetime格式（2017-08-03 14:49:41.804000）转换为数字格式（20170808000000）
    dt = now.year
    dt *= 100
    dt += now.month
    dt *= 100
    dt += now.day
    dt *= 100
    dt += now.hour
    dt *= 100
    dt += now.minute
    dt *= 100
    dt += now.second
    return dt


def process(or_dict):  # 规范订单，将其转换为[0,1]模式
    record = {}
    for i in or_dict:
        t = or_dict[i]
        if len(t) >= 2:
            record[i] = []
            array = []
            time = []
            dt = []
            dt1 = []
            rec1 = []
            rec = []
            for r in t:
                array.append(r[4])
                time.append(r[1])
            tt = array.count(0)
            tt1 = array.count(1)
            if tt == 1:
                cnt = array.index(0)
                record[i].append([t[cnt][0], t[cnt][2], t[cnt][3]])
            elif tt > 1:
                for n in range(len(array)):
                    if array[n] == 0:
                        d_t = unix2date(time[n])
                        dt.append(d_t)
                        rec.append(t[n])
                try:
                    a = dt.index(min(dt))
                    record[i].append([rec[a][0], rec[a][2], rec[a][3]])
                except ValueError:
                    print i
            if tt1 == 1:
                cnt = array.index(1)
                record[i].append([t[cnt][0], t[cnt][2], t[cnt][3]])
            elif tt1 > 1:
                for n in range(len(array)):
                    if array[n] == 1:
                        d_t = unix2date(time[n])
                        dt1.append(d_t)
                        rec1.append(t[n])
                try:
                    a = dt1.index(min(dt1))
                    record[i].append([rec1[a][0], rec1[a][2], rec1[a][3]])
                except ValueError:
                    print i
    return record


def get_data_all(t, t1, t2):
    or_dict = {}
    record = []
    conn = mysql_conn.get_bike_connection()
    # conn = MySQLdb.connect(host='172.18.106.159', user='bike', passwd='tw_85450077', db='bike', port=3306)
    cur = conn.cursor()
    bt = time.time()
    sql = 'SELECT * from tb_bike_gps_{2} WHERE CompanyId = "mb" and PositionTime>="{0}" and PositionTime< "{1}"'.format(t1, t2, t[2:])
    cur.execute(sql)
    for i in cur:
        record.append([i[2], i[3], i[4], i[5], i[7], i[8]])
    # bicycle_no ,position_time,longitude,latitude,state,order_id
    et = time.time()
    print 'get orderid cost {0}'.format(et - bt)
    for i in record:
        or_dict[i[5]] = []
    for i in record:
        or_dict[i[5]].append(i[:5])
    record1 = process(or_dict)
    return record1


def get_2hour_data(record):
    Dep = []
    Dest = []
    for i in record:
        if len(record[i]) == 2:
            r1 = record[i][0]
            r2 = record[i][1]
            depLon = r1[1]
            depLat = r1[2]
            desLon = r2[1]
            desLat = r2[2]
            Dep.append([depLon, depLat, r1[0]])
            Dest.append([desLon, desLat, r2[0]])
    return Dep, Dest


def processing(Dep, Dest):
    res = get_xiaoqu(Dep, Dest)
    insert_OD(res)


def tick():
    now = datetime.now()
    print 'time is: %s' % now
    yst = now + timedelta(days=-1)
    # tor = now + timedelta(days=-6)
    st1 = yst.strftime("%Y-%m-%d 00:00:00")
    st = yst.strftime("%Y%m")
    st2 = now.strftime("%Y-%m-%d 00:00:00")
    record = get_data_all(st, st1, st2)
    Dep, Dest = get_2hour_data(record)
    if len(Dep) == 0:
        print 'mb---{0}条'.format(len(Dep)).decode('utf8')
    else:
        print 'mb---{0}条'.format(len(Dep)).decode('utf8')
        processing(Dep, Dest)


map_i = get_mapindex()
if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    # scheduler.add_job(tick, 'interval', days=1)
    scheduler.add_job(tick, 'cron', hour='1')
    try:
        scheduler.start()
    except SystemExit:
        pass


