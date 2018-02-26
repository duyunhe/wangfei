# coding=utf-8
'''
交通小区每小时统计车辆数量，分公司显示(保留历史记录一天)
'''
__author__ = 'cd'
import MySQLdb
import time
from apscheduler.schedulers.blocking import BlockingScheduler
import logging
from datetime import datetime
import redis
from datetime import timedelta


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


def get_data_all():
    record = []   # 公司名+坐标
    bt = time.time()
    conn = MySQLdb.connect(host='172.18.106.159', user='bike', passwd='tw_85450077', db='bike', port=3306)
    cur = conn.cursor()
    sql = 'SELECT * from tb_bike_status_realtime where PositionTime > DATE_SUB(Now(),INTERVAL 1 month)'
    cur.execute(sql)
    for i in cur:
        if i[3] == None or i[4] == None:
            continue
        record.append([i[0], (i[3], i[4])])
    et = time.time()
    cur.close()
    conn.close()
    print 'select cost', et-bt
    return record


def insert_OD(company_num):
    conn = MySQLdb.connect(host='172.18.106.159', user='bike', passwd='tw_85450077', db='bike', port=3306)
    cur = conn.cursor()
    insert_sql = 'insert into tb_fq_company_history (AreaID, CompanyID, AreaCount, DBTime) values(%s,%s,%s,%s)'
    tup_list = []
    for i in company_num:
        CI = i
        d = company_num[i][0]
        for t in range(1, 3288):
            AI = t
            if t in d:
                AN = d[t]
            else:
                AN = 0
            DT = time.strftime("%Y-%m-%d %H:00:00", time.localtime())
            tup = (AI, CI, AN, DT)
            tup_list.append(tup)
    cur.executemany(insert_sql, tup_list)
    conn.commit()
    cur.close()
    conn.close()
    print 'insert done'


def process_data():
    global map_ind
    com_id = ['hellobike', 'mb', 'mt', 'ofo', 'qibei', 'xiaoming', 'yonganxing']
    # bound = {'ymi': 30.0782718096, 'xma': 120.413638931, 'yma': 30.3962689534, 'xmi': 119.996230966}
    bound = {'ymi': 29.745676, 'yma': 30.56237, 'xmi': 119.436459, 'xma': 120.704416}
    comp_num = {}
    comp_l = {}
    record = get_data_all()
    for i in com_id:
        comp_num[i] = []
        comp_l[i] = []
    lon = (bound['xma'] - bound['xmi']) / 1000
    lat = (bound['yma'] - bound['ymi']) / 1000
    bt = time.time()
    for i in record:
        x = int((i[1][0] - bound['xmi']) / lon) + 1
        y = int((i[1][1] - bound['ymi']) / lat) + 1
        if 1 <= x <= 1000 and 1 <= y <= 1000:
            ar_id = map_ind[(x, y)]
            if ar_id != 0:
                comp_l[i[0]].append(ar_id)
    for i in comp_l:
        list = comp_l[i]
        com_sum = {}
        ind_l = []
        for ind in list:
            if ind not in ind_l:
                ind_l.append(ind)
                com_sum[ind] = 1
            else:
                com_sum[ind] += 1
        comp_num[i].append(com_sum)
    et = time.time()
    print 'process cost', et-bt
    return comp_num


def tick():
    now = datetime.now()
    print 'time is: %s' % now
    comp_num = process_data()
    insert_OD(comp_num)


def tick1():
    now = datetime.now()
    print 'tick1 is: %s' % now
    yst = now + timedelta(days=-1)
    st = yst.strftime("%Y-%m-%d 00:00:00")
    conn = MySQLdb.connect(host='172.18.106.159', user='bike', passwd='tw_85450077', db='bike', port=3306)
    cur = conn.cursor()
    sql = 'delete from tb_fq_company_history where DBTime <"{0}"'.format(st)
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()


map_ind = get_mapindex()
if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BlockingScheduler()
    # scheduler.add_job(tick, 'interval', days=1)
    scheduler.add_job(tick, 'cron', hour='*')
    scheduler.add_job(tick1, 'cron', hour='0')
    try:
        scheduler.start()
    except SystemExit:
        pass
