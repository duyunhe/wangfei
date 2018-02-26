# coding=utf-8
'''
共享单车监控：统计每小时每家公司的数据量
'''
__author__ = 'wf'
import time
import stomp
import json
import MySQLdb
from datetime import datetime
from MySQLdb.cursors import DictCursor
from DBUtils.PooledDB import PooledDB
from apscheduler.schedulers.background import BackgroundScheduler
import logging


sql = "insert into tb_monitor values(%s, %s, %s, 'position')"
# conn = MySQLdb.connect(host='172.18.106.159', user='bike', passwd='tw_85450077', db='bike', port=3306)
sql_settings = {'mysql': {'host': '172.18.106.159', 'port': 3306, 'user': 'bike',
                              'passwd': 'tw_85450077', 'db': 'bike'}}
pool = PooledDB(creator=MySQLdb,
                mincached=1, maxcached=20,
                use_unicode=True, charset='utf8',
                cursorclass=DictCursor,
                **sql_settings['mysql'])
dbConn = pool.connection()
cursor = dbConn.cursor()
comp_num = {'ofo': 0, 'mb': 0, 'hellobike': 0, 'mt': 0, 'xiaoming': 0, 'qibei': 0, 'yonganxing': 0}
tup_list = []


class MyListener(object):
    def on_error(self, headers, message):
        print('received an error %x' % message)

    def on_message(self, headers, message):
        dic = json.loads(message)
        CompanyId = dic['CompanyId']
        global comp_num
        try:
            comp_num[CompanyId] += 1
        except KeyError:
            pass


def tick():
    DT = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print DT
    global comp_num
    for i in comp_num:
        print i, comp_num[i]
        tup = (i, comp_num[i], DT)
        global tup_list
        tup_list.append(tup)
    global cursor
    cursor.executemany(sql, tup_list)
    global dbConn
    dbConn.commit()
    tup_list = []
    comp_num = {'ofo': 0, 'mb': 0, 'hellobike': 0, 'mt': 0, 'xiaoming': 0, 'qibei': 0, 'yonganxing': 0}


if __name__ == '__main__':
    logging.basicConfig()
    scheduler = BackgroundScheduler()
    scheduler.add_job(tick, 'cron', hour='*')
    scheduler.start()
    conn = stomp.Connection10([('172.18.106.157', 61613)])
    conn.set_listener('', MyListener())
    conn.start()
    conn.connect()
    conn.subscribe(destination='/topic/positionData', id='1', ack='auto')
    while True:
        time.sleep(2)
    # conn.disconnect()
