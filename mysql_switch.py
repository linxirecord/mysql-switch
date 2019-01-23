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
        try:
            transport = paramiko.Transport(new_host, 22)
            transport.connect(username=user, password=passwd)
            ssh = paramiko.SSHClient()
            ssh._transport = transport
            stdin, stdout, stderr = ssh.exec_command(cmd)
            transport.close()
        except Exception as e:
            print(e)

    def ftp_file(self,new_host,user,passwd):
        '''从新master上下载备份文件用来恢复数据'''
        try:
            transport = paramiko.Transport(new_host,22)
            transport.connect(username=user, password=passwd)
            sftp = paramiko.SFTPClient.from_transport(transport)
            sftp.get('/tmp/bak.sql', '/tmp')
            transport.close()
        except Exception as e:
            print(e)

    def check_slave(self):
        '''返回主从的io、sql线程的状态及是以那种模式复制的参数'''
        sql="show slave status"
        data=self.conn_database(sql)
        io_status = data[0]['Slave_IO_Running']
        sql_status=data[0]['Slave_SQL_Running']
        master_ip=data[0]['Master_Host']
        master_port=data[0]['Master_Port']
        a_position=data[0]['Auto_Position']
        exec_master_log_pos=data[0]['Exec_Master_Log_Pos']
        new_master_data=data[0]['Master_Log_File']
        return io_status,sql_status,a_position,master_ip,master_port, exec_master_log_pos,new_master_data

    def reuse_code(self,sql1):
        '''重复代码'''
        sql = "stop  slave"
        sql1 = sql1
        sql2 = "start slave"
        self.conn_database(sql)
        self.conn_database(sql1)
        self.conn_database(sql2)
        new_replication = self.check_slave()
        if new_replication[0]=='Yes' and new_replication[1]=='Yes':
            return True
        else:
            return False


    def switch_data(self,new_host,new_port,user,passwd):
        '''主从两种模式切换'''
        old_replication = self.check_slave()
        if old_replication[0] == 'Yes' and old_replication[1] == 'Yes':  # 旧master无故障时切换
            master_ip = old_replication[3]
            master_port = old_replication[4]
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
                #当两slave的数据一致，直接change master to 即可
                while True:
                    if self.conn_database("show slave status")[0]['Read_Master_Log_Pos']==self.conn_database(new_host,new_port,
                        "show slave status")[0]['Read_Master_Log_Pos']==binlog_pos and self.conn_database("show slave status")\
                        [0]['Master_Log_File']==self.conn_database(new_host,new_port,"show slave status")[0]['Master_Log_File']==binlog_file:
                        #判断slave是否同步到start slave until中指定的file和pos点
                        master_data1 = self.conn_database(new_host, new_port, sql)
                        binlog_file1 = master_data1[0]['File']
                        binlog_pos1 = master_data1[0]['Position']
                        self.conn_database(sql1)
                        sql3="change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s',master" \
                             "_log_file='%s',master_log_pos=%s;"%(new_host,new_port,self.user,self.passwd,binlog_file1,binlog_pos1)
                        return self.reuse_code(sql3)

                    else:
                        print("数据正在同步,请等待")
            else:                                                         #复制为gtid模式
                sql2 = "start slave until master_log_file ='%s' , master_log_pos =%s"%(binlog_file,binlog_pos)      # 所有slave上的数据同步到指定的pos点时停止复制
                self.conn_database(sql2)
                self.conn_database(new_host, new_port, sql2)
                while True:
                    if self.conn_database("show slave status")[0]['Read_Master_Log_Pos'] ==\
                            self.conn_database(new_host, new_port, "show slave status")[0]['Read_Master_Log_Pos'] == \
                            binlog_pos and self.conn_database("show slave status")[0]['Master_Log_File'] ==\
                            self.conn_database(new_host, new_port, "show slave status")[0]['Master_Log_File'] == binlog_file:
                        #此时两slave日志里面的的gtid相同，可直接change master to
                        sql4="change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s', \
                           master_auto_position=1" % (new_host, new_port, self.user, self.passwd)
                        return self.reuse_code(sql4)

                    else:
                        print("数据正在同步，请等待")
        else:                                          #master故障切换
            if old_replication[2]==0:                  #传统复制模式
                cmd="innobackupex --defaults-file=/usr/local/mysql%s/my.cnf --host %s --port %s --user=%s --password=%s " \
                    "--parallel=2  /tmp/bak.sql"%(new_port,new_host,new_port,self.user,self.passwd)
                cmd1="innobackupex --datadir=/usr/local/mysql%s/data/  --apply-log /tmp/bak.sql/"%self.port
                cmd2="innobackupex --datadir=/usr/local/mysql%s/data/  --copy-back /tmp/bak.sql/"%self.port
                cmd3="/etc/init.d/mysqld%s start"%self.port
                cmd4 ="/etc/init.d/mysqld%s stop" % self.port
                cmd5="rm -fr /usr/local/mysql%s/data"%self.port
                cmd6="chown mysql.mysql /usr/local/mysql%s/data"%self.port
                cmd7="rm -fr /tmp/bak.sql"
                sql = "show master status"
                sql2="stop slave"
                self.ssh_server(new_host,user,passwd,cmd)             #备份
                master_file_pos=self.conn_database(new_host,new_port,sql)
                binlog_file = master_file_pos[0]['File']
                binlog_pos = master_file_pos[0]['Position']           #获取pos和file
                self.ftp_file(new_host,user,passwd)                   #传输备份文件
                self.ssh_server(new_host,user,passwd,cmd7)
                self.ssh_server(self.host,user,passwd,cmd4)
                self.ssh_server(self.host,user,passwd,cmd5)
                self.ssh_server(self.host,user,passwd,cmd1)           #回滚为日志
                self.ssh_server(self.host,user,passwd,cmd2)           #拷贝
                self.ssh_server(self.host,user,passwd,cmd6)
                self.ssh_server(self.host,user,passwd,cmd3)
                self.conn_database(sql2)
                sql1="change master to master_host=%s,master_port=%s,master_user=%s,master_password=%s,\
                    master_log_file=%s,master_log_pos=%s"%(new_host,new_port,self.user,self.passwd,binlog_file,binlog_pos)
                return_data=self.reuse_code(sql1)
                if return_data==True:
                    database_list = []
                    all_database = self.conn_database(new_host, new_port, "show databases")
                    for j in range(len(all_database)):
                        if all_database[j]['Database'] in ['information_schema', 'mysql', 'performance_schema', 'sys']:
                            pass
                        else:
                            database_list.append(all_database[j]['Database'])
                    for i in range(len(database_list)):  # 检查除过系统表以外的所有表
                        cmd = "pt-table-checksum --nocheck-replication-filters --no-check-binlog-format --replicate= " \
                              "%s.checksums --databases=%s  h=%s,u=%s,p=%s,P=%s --recursion-method=hosts" % \
                              (database_list[i], database_list[i], new_host, self.user, self.passwd, new_port)  # 检查数据是否偏差
                        cmd1 = "pt-table-sync --sync-to-master h=%s,u=%s,p=%s,P=%s --databases=%s --execute" % \
                               (self.host, self.user, self.port, self.port, database_list[i])  # 同步数据
                        self.ssh_server(new_host, user, passwd, cmd)
                        self.ssh_server(new_host, user, passwd, cmd1)
                    return True
                else:
                    return False

            else:                                                       #gtid模式
                stop_slave_sql="stop slave"
                sql="show slave status"
                new_master_data=self.conn_database(new_host,new_port,sql)
                exec_master_log_pos1=new_master_data[0]['Exec_Master_Log_Pos']      #-->
                master_log_file1=new_master_data[0]['Master_Log_File']              #-->
                exec_master_log_pos=old_replication[5]                              #-->
                master_log_file=old_replication[6]                                  #-->获取slave在宕机时获取的binlog文件和中继日志中处理的行在原master
                mlist = master_log_file1.split('.')[1].split('0')                   #-->中对应的pos点进行比较得出新master和slave的数据量比较
                mlist[-1]=int(mlist[-1])                                            #-->
                slist = master_log_file.split('.')[1].split('0')                    #-->
                slist[-1]=int(slist[-1])                                            #-->
                if  slist[-1] < mlist[-1]:                              #新master数据比slave新时
                     self.conn_database(stop_slave_sql)
                     sql2="change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s'," \
                          "master_auto_position=1" % (new_host, new_port, self.user, self.passwd,)
                     return self.reuse_code(sql2)
                elif slist[-1]==mlist[-1]:
                    if exec_master_log_pos <= exec_master_log_pos1:     #新master数据比slave新或相同时
                        self.conn_database(stop_slave_sql)
                        sql2 = "change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s'," \
                               "master_auto_position=1" %(new_host, new_port, self.user, self.passwd)
                        return self.reuse_code(sql2)
                    else:                                               #新master数据旧于slave的数据
                        self.conn_database(stop_slave_sql)
                        sql2= "change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s'," \
                              "master_auto_position=1" %(new_host, new_port, self.user, self.passwd)
                        return_data=self.reuse_code(sql2)
                        #此时虽然主从成功，但新master和slave上的数据不一致，借助precona-toolkit修复
                        if return_data==True:
                            database_list = []
                            all_database=self.conn_database(new_host,new_port,"show databases")
                            for j in range(len(all_database)):
                                if all_database[j]['Database'] in ['information_schema', 'mysql', 'performance_schema','sys']:
                                    pass
                                else:
                                    database_list.append(all_database[j]['Database'])
                            for i in range(len(database_list)):         #检查除过系统表以外的所有表
                                cmd="pt-table-checksum --nocheck-replication-filters --no-check-binlog-format --replicate= " \
                                    "%s.checksums --databases=%s  h=%s,u=%s,p=%s,P=%s --recursion-method=hosts"%\
                                    (database_list[i],database_list[i],new_host,self.user,self.passwd,new_port)   #检查数据是否偏差
                                cmd1="pt-table-sync --sync-to-master h=%s,u=%s,p=%s,P=%s --databases=%s --execute"%\
                                     (self.host,self.user,self.port,self.port,database_list[i])   #同步数据
                                print(self.ssh_server(new_host,user,passwd,cmd))
                                self.ssh_server(new_host,user,passwd,cmd1)
                            return True
                        else:
                            return False
                else:                                                   #另外一种新master数据旧于slave的数据的情况
                    self.conn_database(stop_slave_sql)
                    sql2 = "change master to master_host='%s',master_port=%s,master_user='%s',master_password='%s'," \
                           "master_auto_position=1" % (new_host, new_port, self.user, self.passwd)
                    flag=self.reuse_code(sql2)
                    if flag==True:
                        #此时新master和slave上的数据不一致，借助precona-toolkit修复
                        database_list = []
                        all_database=self.conn_database(new_host,new_port,"show databases")
                        for j in range(len(all_database)):
                            if all_database[j]['Database'] in ['information_schema','mysql','performance_schema','sys']:
                                pass
                            else:
                                database_list.append(all_database[j]['Database'])
                        for i in range(len(database_list)):         #检查除过系统表以外的所有表
                            cmd="pt-table-checksum --nocheck-replication-filters --no-check-binlog-format --replicate= " \
                            "%s.checksums --databases=%s  h=%s,u=%s,p=%s,P=%s --recursion-method=hosts"%\
                            (database_list[i],database_list[i],new_host,self.user,self.passwd,new_port)   #检查数据是否偏差
                            cmd1="pt-table-sync --sync-to-master h=%s,u=%s,p=%s,P=%s --databases=%s --execute"%\
                            (self.host,self.user,self.port,self.port,database_list[i])   #同步数据
                            self.ssh_server(new_host,user,passwd,cmd)
                            self.ssh_server(new_host,user,passwd,cmd1)
                        return True
                    else:
                        return False

print('####需要切换的slave ip,port,user,passwd####')
s_host=input("输入slave的ip：")
s_port=int(input("输入slave的port："))
s_user=input("输入slave的user：")
s_passwd=input("输入slave的passwd：")
print('####新master的ip，port#####')
nm_host=input("输入new master的ip：")
nm_port=int(input("输入new master的port:"))
user=input("输入登录new master所在的机器的用户名")
passwd=input("输入登录new master所在的机器的用户密码")
print(Mysql_switch(s_host,s_port,s_user,s_passwd).switch_data(nm_host,nm_port,user,passwd))
