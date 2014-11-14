#!/usr/bin/python
#coding:utf-8
'''
example:
   python mysql_full_backup_mysqldump.py -H 192.168.1.2 -u root -p test123 -B dataname -s /tmp/mysql.sock  -d /backup
'''

import time, argparse, os, sys
import MySQLdb, logging, subprocess, shlex

os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
backup_date_time = time.strftime("%Y_%m_%d_%H_%M_%S")
backup_command = '/usr/local/mysql/bin/mysqldump'
backup_log = '/var/log/mysql_backup.log'
datadir = '/usr/local/mysql/data'


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", help="Specified mysql user", default="root")
    parser.add_argument("-d", help="Specified backup dir")
    parser.add_argument("-H", help="Specified mysql host", default="localhost")
    parser.add_argument("-P", help="Specified mysql port", default=3306)
    parser.add_argument("-B", help="Specified mysql database", default='test')
    parser.add_argument("-p", help="Specified mysql password", default="")
    parser.add_argument("-s", help="Specified mysql sock", default='/var/run/mysqld/mysqld.sock')
    #parser.add_help
    return parser.parse_args()


def logger_handle(backup_log):
    logger = logging.getLogger('video_rsync')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler(backup_log)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s: %(funcName)s-%(lineno)d %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger


def get_binlog_pos(args):
    try:
        init_conn = MySQLdb.Connect(host=args.H, user=args.u, passwd=args.p, db=args.B, unix_socket=args.s)
        cursor = init_conn.cursor()
    except Exception, e:
        #print e
        log = "mysql connect failure  becuase {0}".format(e)
        logger.error(log)
        sys.exit(0)
    else:

        sql = "show master status;"
        try:
            cursor.execute(sql)
            row = cursor.fetchall()
            binlog_file = row[0][0]
            binlog_pos = int(row[0][1])
            return binlog_file, binlog_pos
        except Exception, e:
            log = "mysql get binlog pos failure  becuase {0}".format(e)
            logger.error(log)
            sys.exit(0)


def exec_backup(args):
    backup_sql = "{0}/{1}.sql".format(args.d, args.B)
    backup_file = "{0}_{1}.tar.gz".format(args.B, backup_date_time)
    cmd = "{0} -h {1} -u{2} -p{3} --flush-logs --opt --databases {4}".format(
        backup_command, args.H, args.u, args.p, args.B)
    #print cmd
    backup_cmd = shlex.split(cmd)
    #    print backup_cmd
    #try:
    #print backup_cmd
    with open(backup_sql, 'w') as output_sql:
        output = subprocess.Popen(backup_cmd, stdout=output_sql, stderr=subprocess.PIPE)
        output.wait()
        output_sql.close()
        #print output.returncode

        if not output.returncode:
            binlog_file, binlog_pos = get_binlog_pos(args)
            log = "mysql backup {0} sucess binlog={1} pos={2}".format(args.B, binlog_file, binlog_pos)
            os.chdir(args.d)
            zip_cmd = "tar zcf {0} {1}.sql".format(backup_file, args.B)
            subprocess.call(zip_cmd.split(' '))
            rm_dbsql = "rm -rf {0}/{1}.sql".format(args.d, args.B)
            subprocess.call(rm_dbsql.split(' '))
            logger.info(log)
        else:
            #print output.stderr.readline().decode()
            log = "mysql backup {0} failure  becuase {1}".format(args.B, output.stderr.readline().decode())
            #print log
            logger.error(log)
            sys.exit(0)


if __name__ == "__main__":
    logger = logger_handle(backup_log)
    args = arg_parse()
    if args.B and args.p and args.d:
        exec_backup(args)
        #get_binlog_pos(args)
    else:
        print "please enter -h get help"
