# coding=utf-8
import MySQLdb
import ConfigParser


def get_bike_connection():
    abs_file = __file__
    filename = abs_file[:abs_file.rfind("\\")] + '\config.ini'
    cf = ConfigParser.ConfigParser()
    fp = open(filename)
    cf.readfp(fp)

    host = cf.get('db', 'host')
    port = int(cf.get('db', 'port'))
    pswd = cf.get('db', 'pswd')
    db = cf.get('db', 'db')
    user = cf.get('db', 'user')

    conn = MySQLdb.connect(host=host, user=user, passwd=pswd, db=db, port=port)
    return conn
