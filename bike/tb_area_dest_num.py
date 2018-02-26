# coding=utf-8
__author__ = 'wf'
import MySQLdb
from datetime import datetime
from datetime import timedelta
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
import redis
import re


def get_admin_divi():
    adm_dict = {}
    f = open('district.txt', 'r')
    lines = f.readlines()
    for i in lines:
        t = i.split(',')
        ad = t[1].strip('\n').decode('gbk').encode('utf8')
        adm_dict[int(t[0])] = ad
    return adm_dict


def get_xiaoqu(Dest, map_i):  # 出发点和到达点所在小方块（1000*1000）
    bound = {'ymi': 29.745676, 'yma': 30.56237, 'xmi': 119.436459, 'xma': 120.704416}
    lon = (bound['xma'] - bound['xmi']) / 1000
    lat = (bound['yma'] - bound['ymi']) / 1000
    dep_reg = {}
    dest_reg = {}
    for i in range(1, 3288):
        dep_reg[i] = 0
        dest_reg[i] = 0
    for i in range(0, len(Dest)):
        # x = int((Dep[i][0] - bound['xmi']) / lon) + 1
        # y = int((Dep[i][1] - bound['ymi']) / lat) + 1
        x1 = int((Dest[i][0] - bound['xmi']) / lon) + 1
        y1 = int((Dest[i][1] - bound['ymi']) / lat) + 1
        if 0 < x1 <= 1000 and 0 < y1 <= 1000:
            ind1 = map_i[(x1, y1)]
            if ind1 == 0:
                continue
            dest_reg[ind1] += 1
    dest_num = []
    for i in dest_reg:
        dest_num.append((i, dest_reg[i]))
    dest_num.sort(key=lambda x: x[1], reverse=True)
    return dest_num


def get_mapindex():
    mi_dic = {}
    reg = []
    reg1 = []
    pool = redis.ConnectionPool(host='127.0.0.1', port=6068, db=1)
    r = redis.StrictRedis(connection_pool=pool)
    bt = time.time()
    for t in range(1, 1001):
        for n in range(1, 1001):
            reg.append("{0}, {1}".format(n, t))
            reg1.append((n, t))
    result = r.mget(reg)
    for i in range(0, 1000000):
        mi_dic[reg1[i]] = int(result[i])
    et = time.time()
    print 'index cost {0}'.format(et-bt)
    return mi_dic


def insert_OD(d_num):
    global xq_district
    conn = MySQLdb.connect(host='60.191.16.73', user='bike', passwd='bike', db='bike', port=6052, charset='utf8')
    cur = conn.cursor()
    insert_sql = 'insert into tb_area_dest_num (Area_ID, area_district, DestCount, DBtime) values(%s,%s,%s,%s) '
    tup_list = []
    for i in d_num:
        if i[1] == 0:
            break
        AI = i[0]
        AD = xq_district[i[0]]
        DC = i[1]
        DT = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        tup = (AI, AD, DC, DT)
        tup_list.append(tup)
    cur.executemany(insert_sql, tup_list)
    conn.commit()
    cur.close()
    conn.close()


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


def get_data_all(t1, t2):
    or_dict = {}
    record = []
    conn = MySQLdb.connect(host='60.191.16.73', user='bike', passwd='bike', db='bike', port=6052)
    cur = conn.cursor()
    bt = time.time()
    sql = 'SELECT * from tb_bike_gps_1801 WHERE CompanyId = "mb" and PositionTime>="{0}" and PositionTime< "{1}"'.format(t1, t2)
    cur.execute(sql)
    for i in cur:
        record.append([i[1], i[2], i[3], i[4], i[6], i[7]])
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
            Dep.append([depLon, depLat])
            Dest.append([desLon, desLat])
    return Dest


def process_data(st1, st2):
    record = get_data_all(st1, st2)
    Dest = get_2hour_data(record)
    global map_i
    Dest_num = get_xiaoqu(Dest, map_i)
    return Dest_num


def tick():
    now = datetime.now()
    print 'time is: %s' % now
    yst = now + timedelta(hours=-1)
    # tor = now + timedelta(days=-6)
    st1 = yst.strftime("%Y-%m-%d %H:%M:%S")
    st2 = now.strftime("%Y-%m-%d %H:%M:%S")
    res = process_data(st1, st2)
    insert_OD(res)

xq_district = get_admin_divi()
map_i = get_mapindex()
tick()
if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    # scheduler.add_job(tick, 'interval', days=1)
    scheduler.add_job(tick, 'interval', minutes=10)
    try:
        scheduler.start()
    except SystemExit:
        pass


