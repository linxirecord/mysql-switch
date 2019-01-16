#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pymysql
class Mysql_switch(object):
    def __init__(self,host,port,user,passwd):
        self.host=host
        self.port=int(port)
        self.user=user
        self.passwd=passwd

    def conn_database(self,*args):
        '''连接数据库'''
        if len(args)==1:
            sql=args[0]
            conn = pymysql.connect(host=self.host, port=self.port, user=self.user, password=self.passwd,
                                   charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
            cursor = conn.cursor()
            cursor.execute(sql)
            data = cursor.fetchall()
            conn.commit()
            cursor.close()
            conn.close()
            return data
        elif len(args)==3:
            host=args[0]
            port=args[1]
            sql=args[2]
            conn = pymysql.connect(host=host, port=port, user=self.user, password=self.passwd,
                                   charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
            cursor = conn.cursor()
            cursor.execute(sql)
            data=cursor.fetchall()
            conn.commit()
            cursor.close()
            conn.close()
            return data

    def check_slave(self):
        '''返回主从的io、sql线程的状态及是以那种模式复制的参数'''
        sql="show slave status"
        data=self.conn_database(sql)
        io_status = data[0]['Slave_IO_Running']
        sql_status=data[0]['Slave_SQL_Running']
        a_position=data[0]['Auto_Position']
        return io_status,sql_status,a_position

    def reuse_code(self,sql1):
        '''重复代码'''
        sql = "stop  slave"
        sql1 = sql1
        sql2 = "start slave"
        self.conn_database(sql)
        self.conn_database(sql1)
        self.conn_database(sql2)
        new_replication = self.check_slave()
        if new_replication[0] == 'Yes' and new_replication[1] == 'Yes':
            return True
        else:
            return "switch exception"

    def switch_data(self,new_host,new_port):
        '''主从两种模式切换'''
        slave_data=self.check_slave()
        if slave_data[0]=='Yes' and slave_data[1]=='Yes':
            return True
        else:
            if slave_data[2]==0:      #复制为传统模式
                sql="show master status"
                master_pos=self.conn_database(new_host,new_port,sql)
                position=master_pos[0]['Position']
                binlog_file=master_pos[0]['File']
                sql1="change master to master_host=%s,master_port=%s,master_user=%s,master_password=%s,\
                   master_log_file=%s,master_log_pos=%s"%(new_host,new_port,self.user,self.passwd,binlog_file,position)
                self.reuse_code(sql1)
            else:                     #复制为gtid模式
                sql="show variables like 'server_id%'"
                master_server_id=int(self.conn_database(new_host,new_port,sql)[0]['Value'])
                sql1="change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s', \
                   master_auto_position=%s" % (new_host, new_port, self.user, self.passwd, master_server_id)
                self.reuse_code(sql1)


print('####需要切换的slave ip,port,user,passwd####')
s_host=input("输入slave的ip：")
s_port=int(input("输入slave的port："))
s_user=input("输入slave的user：")
s_passwd=input("输入slave的passwd：")
print('####新master的ip，port#####')
nm_host=input("输入new master的ip：")
nmport=int(input("输入new master的port："))
print(Mysql_switch(s_host,s_port,s_user,s_passwd).switch_data(nm_host,nmport))