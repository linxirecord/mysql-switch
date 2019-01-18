#!/usr/bin/env python
# -*- coding: utf-8 -*-
import pymysql
import paramiko,time

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

    def ssh_server(self,new_host,user,passwd,cmd):
        '''获取逻辑备份文件'''
        transport = paramiko.Transport(new_host, 22)
        transport.connect(username=user, password=passwd)
        ssh = paramiko.SSHClient()
        ssh._transport = transport
        stdin, stdout, stderr = ssh.exec_command(cmd)
        transport.close()

    def ftp_file(self,new_host,user,passwd):
        '''从新master上下载备份文件用来恢复数据'''
        transport = paramiko.Transport(new_host,22)
        transport.connect(username=user, password=passwd)
        sftp = paramiko.SFTPClient.from_transport(transport)
        sftp.get('/tmp/mysql.bak.sql', '/tmp')
        transport.close()

    def check_slave(self):
        '''返回主从的io、sql线程的状态及是以那种模式复制的参数'''
        sql="show slave status"
        data=self.conn_database(sql)
        print(data)
        io_status = data[0]['Slave_IO_Running']
        sql_status=data[0]['Slave_SQL_Running']
        master_ip=data[0]['Master_Host']
        master_port=data[0]['Master_Port']
        a_position=data[0]['Auto_Position']
        exec_master_log_pos=data[0]['Exec_Master_Log_Pos']
        return io_status,sql_status,a_position,master_ip,master_port, exec_master_log_pos

    def reuse_code(self,sql1):
        '''重复代码'''
        sql = "stop  slave"
        sql1 = sql1
        sql2 = "start slave"
        self.conn_database(sql)
        self.conn_database(sql1)
        self.conn_database(sql2)
        new_replication = self.check_slave()
        if new_replication=='Yes' and new_replication=='Yes':
            return True
        else:
            return False

    def switch_data(self,new_host,new_port,user,passwd):
        '''主从两种模式切换'''
        old_replication = self.check_slave()
        if old_replication[0] == 'Yes' and old_replication[1] == 'Yes':  # 旧master无故障时切换
            master_ip = old_replication[3]
            master_port = old_replication[4]
            print(master_ip,master_port)
            sql = "show master status"
            sql1="stop slave"
            self.conn_database(sql1)
            self.conn_database(new_host, new_port, sql1)
            time.sleep(5)
            master_data = self.conn_database(master_ip, master_port, sql)
            binlog_file = master_data[0]['File']
            binlog_pos = master_data[0]['Position']
            if old_replication[2]==0:                                     #复制为传统模式
                sql2="start slave until master_log_file ='%s' , master_log_pos =%s"%(binlog_file,binlog_pos)         #slave上的数据同步到指定的pos点时停止复制
                self.conn_database(sql2)
                self.conn_database(new_host,new_port,sql2)
                #此时两slave的数据一致，直接change master to 即可
                lock_sql='flush tables with read lock;'                   #锁表，防止此时new master有数据写入
                self.conn_database(new_host,new_port,lock_sql)
                sql3="change master to master_host=%s,master_port=%s,master_user=%s,master_password=%s,\
                    master_log_file=%s,master_log_pos=%s"%(new_host,new_port,self.user,self.passwd,binlog_file,binlog_pos)
                self.reuse_code(sql3)
                self.conn_database(new_host,new_port,'unlock tables')
            else:                                                         #复制为gtid模式
                sql2 = "start slave until master_log_file ='%s' , master_log_pos =%s"%(binlog_file,binlog_pos)      # 所有slave上的数据同步到指定的pos点时停止复制
                self.conn_database(sql2)
                self.conn_database(new_host, new_port, sql2)
                #此时两slave日志里面的的gtid相同，可直接change master to
                sql3="show variables like 'server_id%'"
                master_server_id=int(self.conn_database(new_host,new_port,sql3)[0]['Value'])
                sql4="change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s', \
                   master_auto_position=%s" % (new_host, new_port, self.user, self.passwd, master_server_id)
                self.reuse_code(sql4)
        else:                                     #master故障切换
            if old_replication[2]==0:                  #传统复制模式
                cmd="mysqldump -h%s  -P%s -u%s -p%s --all-databases > /tmp/mysql.bak.sql"%(new_host,new_port,self.user,self.passwd)
                lock_sql = 'flush tables with read lock;'  # 锁表，防止此时new master有数据写入
                self.conn_database(new_host, new_port, lock_sql)
                self.ssh_server(new_host,user,passwd,cmd)
                self.ftp_file(new_host,user,passwd)
                sql="stop slave"
                sql1="source /tmp/mysql.bak.sql"
                sql2="show master status"
                self.conn_database(sql)
                self.conn_database(sql1)
                new_master_status=self.conn_database(new_host,new_port,sql2)
                binlog_file=new_master_status[0]['File']
                binlog_pos=new_master_status[0]['Position']
                sql3="change master to master_host=%s,master_port=%s,master_user=%s,master_password=%s,\
                    master_log_file=%s,master_log_pos=%s"%(new_host,new_port,self.user,self.passwd,binlog_file,binlog_pos)                                  #gtid复制
                self.reuse_code(sql2)
                self.conn_database(new_host, new_port, 'unlock tables')

            else:
                sql="show slave status"
                new_master_data=self.conn_database(new_host,new_port,sql)
                exec_master_log_pos1=new_master_data[0]['Exec_Master_Log_Pos']
                exec_master_log_pos=old_replication[5]
                if  exec_master_log_pos <=exec_master_log_pos1:         #新master数据比slave新时
                     sql1="show variables like 'server_id%'"
                     master_server_id=int(self.conn_database(new_host,new_port,sql1)[0]['Value'])
                     sql2="change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s', \
                         master_auto_position=%s" % (new_host, new_port, self.user, self.passwd, master_server_id)
                     self.reuse_code(sql2)
                else:                                                   #新master数据比slave旧时
                    sql1="show variables like 'server_id%'"
                    sql2="show master status"
                    slave_file_pos=self.conn_database(sql2)
                    slave_file=slave_file_pos[0]['File']
                    slave_pos=  slave_file_pos[0]['Position']
                    sql3="stop slave"
                    sql4="start slave until master_log_file ='%s' , master_log_pos =%s" %(slave_file,slave_pos)
                    slave_server_id= int(self.conn_database(sql1)[0]['Value'])
                    sql3="change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s',\
                        master_auto_position=%s" %(self.host, self.port, self.user, self.passwd,slave_server_id)
                    self.reuse_code(sql3)                                 #此时两库的数据一致


                    sql1="show variables like 'server_id%'"
                    master_server_id=int(self.conn_database(new_host,new_port,sql1)[0]['Value'])
                    sql2="change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s',\
                         master_auto_position=%s" % (new_host, new_port, self.user, self.passwd, master_server_id)
                    self.reuse_code(sql2)


print('####需要切换的slave ip,port,user,passwd####')
s_host=input("输入slave的ip：")
s_port=int(input("输入slave的port："))
s_user=input("输入slave的user：")
s_passwd=input("输入slave的passwd：")
print('####新master的ip，port#####')
nm_host=input("输入new master的ip：")
nmport=int(input("输入new master的port："))
user=input("输入登录new master所在的机器的用户名")
passwd=input("输入登录new master所在的机器的用户密码")
print(Mysql_switch(s_host,s_port,s_user,s_passwd).switch_data(nm_host,nmport,user,passwd))
