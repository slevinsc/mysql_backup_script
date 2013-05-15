#coding:utf-8
import time,argparse,os,sys
import MySQLdb,logging,subprocess
os.environ['PYTHON_EGG_CACHE'] = '/tmp/.python-eggs'
backup_date_time=time.strftime("%Y_%m_%d_%H_%M_%S")

def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("-u", help="Specified mysql user",default="root")
    parser.add_argument("-d", help="Specified backup dir")
    parser.add_argument("-H", help="Specified mysql host",default="localhost")
    parser.add_argument("-P", help="Specified mysql port",default=3306)
    parser.add_argument("-B", help="Specified mysql database",default='test')    
    parser.add_argument("-p", help="Specified mysql password",default="")
    parser.add_argument("-s", help="Specified mysql sock")
    #parser.add_help
    return parser.parse_args()
def backup_info_log(log):
    logger = logging.getLogger('mysql_backup')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('/var/log/test.log')  
    fh.setLevel(logging.DEBUG)    
    #ch = logging.StreamHandler()  
    #ch.setLevel(logging.DEBUG)   
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')  
    fh.setFormatter(formatter)  
    #ch.setFormatter(formatter)   
    logger.addHandler(fh)  
    #logger.addHandler(ch)    
    logger.info(log) 
    #logger.error(log)
def backup_error_log(log):
    logger = logging.getLogger('mysql_backup')
    logger.setLevel(logging.DEBUG)
    fh = logging.FileHandler('/var/log/test.log')  
    fh.setLevel(logging.DEBUG)    
    #ch = logging.StreamHandler()  
    #ch.setLevel(logging.DEBUG)   
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')  
    fh.setFormatter(formatter)  
   # ch.setFormatter(formatter)   
    logger.addHandler(fh)  
    #logger.addHandler(ch)    
    logger.error(log)

def get_binlog_pos(args):
    try:
        init_conn=MySQLdb.Connect(host=args.H,user=args.u,passwd=args.p,db=args.B,unix_socket=args.s)
        cursor=init_conn.cursor()
    except Exception, e:
        #print e
        log="mysql connect failure  becuase {0}".format(e)
        backup_error_log(log)
        sys.exit(0)
    else:
        
        sql="show master status;"
        cursor.execute(sql)
        row=cursor.fetchall()
        binlog_file=row[0][0]
        binlog_pos=int(row[0][1])
        return binlog_file,binlog_pos

    
    
    
def exec_backup(args):
    backup_file="{0}_{1}.tar.gz".format(args.B,backup_date_time)
    cmd="/usr/local/mysql/bin/mysqlhotcopy,--flushlog,-u,{0},-p,{1},{2},{3}".format(args.u,args.p,args.B,args.d)
    
    backup_cmd=cmd.split(',')
    #try:
    output=subprocess.Popen(backup_cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    output.wait()
    if not output.returncode:
        os.chdir(args.d)
        zip_cmd="tar zcf {0} {1}".format(backup_file,args.B)
        subprocess.call(zip_cmd.split(' '))
        rm_dbname="rm -rf {0}/{1}".format(args.d,args.B)
        subprocess.call(rm_dbname.split(' '))
        binlog_file,binlog_pos=get_binlog_pos(args)
        log="mysql backup sucess  binlog={0}  pos={1}".format(binlog_file,binlog_pos)
        backup_info_log(log)
    else:
        log="mysql backup failure  becuase {0}".format(output.stderr.readline().decode())
        backup_error_log(log)
        sys.exit(0)

    
if __name__ == "__main__": 
    args=arg_parse()
    if args.B and args.d and args.p:
        exec_backup(args)
         #get_binlog_pos(args)
    else:
        print "please enter -h get help"