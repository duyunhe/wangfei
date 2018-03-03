# coding=utf-8
import MySQLdb
import ConfigParser
from MySQLdb.cursors import DictCursor
from DBUtils.PooledDB import PooledDB


def get_bike_connection_dict():
    abs_file = __file__
    filename = abs_file[:abs_file.rfind("\\")] + '\config.ini'
    cf = ConfigParser.ConfigParser()
    fp = open(filename)
    cf.readfp(fp)

    host = cf.get('mysql', 'host')
    port = int(cf.get('mysql', 'port'))
    pswd = cf.get('mysql', 'pswd')
    db = cf.get('mysql', 'mysql')
    user = cf.get('mysql', 'user')
    sql_settings = {'mysql': {'host': host, 'port': port, 'user': user,
                              'passwd': pswd, 'mysql': db}}
    pool = PooledDB(creator=MySQLdb,
                    mincached=1, maxcached=20,
                    use_unicode=True, charset='utf8',
                    cursorclass=DictCursor,
                    **sql_settings['mysql'])
    dbConn = pool.connection()
    return dbConn


def get_bike_connection():
    abs_file = __file__
    filename = abs_file[:abs_file.rfind("\\")] + '\config.ini'
    cf = ConfigParser.ConfigParser()
    fp = open(filename)
    cf.readfp(fp)

    host = cf.get('mysql', 'host')
    port = int(cf.get('mysql', 'port'))
    pswd = cf.get('mysql', 'pswd')
    db = cf.get('mysql', 'mysql')
    user = cf.get('mysql', 'user')
    sql_settings = {'mysql': {'host': host, 'port': port, 'user': user,
                              'passwd': pswd, 'db': db}}
    pool = PooledDB(creator=MySQLdb,
                    mincached=1, maxcached=20,
                    use_unicode=True, charset='utf8',
                    **sql_settings['mysql'])
    dbConn = pool.connection()
    return dbConn


