#coding:utf-8
import MySQLdb,sys
import argparse
class Mysql_monitor(object):

    def __init__(self,host,port,user,password,dbname,sock):
        self.host=host
        self.port=port
        self.user=user
        self.password=password
        self.dbname=dbname
        self.sock=sock
        
    
    def connect(self):
        try:
            self.conn=MySQLdb.connect(host = self.host,port=self.port,user = self.user,passwd = self.password,db=self.dbname,unix_socket=self.sock,connect_timeout = 2)
            self.cursor=self.conn.cursor(cursorclass = MySQLdb.cursors.DictCursor)
        except Exception,e:
            #print e
            print 1
            sys.exit(1)
#
    def db_Active(self):
        #conn = MySQLdb.connect(host = self.host,port=self.port,user = self.user,passwd = self.password,dbname=self.dbname,connect_timeout = 2)  
        #cursor = conn.cursor()
        #self.connect()
        sql="create table test(id int(5) primary key,name char(10));drop table test;"
        try:
            self.cursor.execute(sql)
            print 0
        except Exception, e:
            #print e
            print 1
            sys.exit(1)
    
    def Monitor_replication(self):
        sql="show slave status;"
        try:
            self.cursor.execute(sql)
            row=self.cursor.fetchall()
            row_dict=row[0]
            if row_dict['Slave_IO_Running'] == 'Yes' or row_dict['Slave_SQL_Running'] == 'Yes':
                print row_dict['Seconds_Behind_Master']
            else:
                print 1
        except Exception, e:
            print 1
    
    def close(self):
        self.cursor_close=self.cursor.close()
        self.conn_close=self.conn.close()
#monitor=Mysql_monitor("192.168.1.1",3306,"repli","test","test")
#monitor.connect()
#print monitor.db_Active()
#monitor.close()
if __name__ == "__main__": 
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", help="Specified monitoring object active and replication")
    parser.add_argument("-H", help="Specified mysql host",default="localhost")
    parser.add_argument("-P", help="Specified mysql port",default=3306)
    parser.add_argument("-B", help="Specified mysql database",default="test")
    parser.add_argument("-u", help="Specified mysql user",default="root")
    parser.add_argument("-p", help="Specified mysql password",default="utvgo.com")
    parser.add_argument("-s", help="Specified mysql sock",default="/tmp/mysql.sock")
    #parser.add_help
    args=parser.parse_args()
    #print args
#print args.active
    monitor=Mysql_monitor(args.H,args.P,args.u,args.p,args.B,args.s)
    #monitor=Mysql_monitor('192.168.1.136',3306,'root','utvgo.com','test','/tmp/mysql.sock')
    monitor.connect() 
    if args.a == "active":
        monitor.db_Active()
        monitor.close()
    elif args.a == "replication":
        monitor.Monitor_replication()
        monitor.close()
    #elif args.active=="replication":
        #pass
