# 主从复制切换脚本
## 脚本实现的功能：
1. 涵盖mysql的传统模式和gtid模式主从切换
2. 两种模式在宕机和正常情况下切换
## 代码描述：
1. conn_database 连接数据库方法
2. check_slave 获取slave的复制状态方法
3. ssh_server备份数据库文件 (线上机器可能不是22，用时需要修改)
4. ftp_file下载备份文件
5. reuser_code 重用代码
6. switch_data 两种切换模式
## 脚本使用的环境：
1. 一个集群中，master宕机，此时没有数据写入和有数据写入<br>
2. 集群正常，因需求需要切换，支持数据写入时和无数据写入时切换<br>
## 脚本思路:
### 核心分为两部分：<br>
#### master无故障切换：<br>
根据auto_position值可将复制模式分为两类：1. 传统复制   2. gtid复制<br>
两种模式在切换之前先执行stop slave，间隔5秒（保证同步后的数据一定时一致的）再执行start slave until bin_log_file="指定二进制文件"，pos="指定一个pos点"，此时两slave的数据一致，然后可以直接执行 change master to 切换.<br>
#### master故障切换：<br>
根据auto_position值可将复制模式分为两类：1. 传统复制   2. gtid复制<br>
1. 传统模式：新master物理备份，导入slave后然后直接切换，使用percona-toolkit修复数据达到数据一致。<br>
2. gtid模式：获取slave在宕机时获取的binlog文件和中继日志中处理的行在原master中对应的pos点进行比较得出新master和slave的数据量比较，如果slave同步使用的二进制日志文件不是同一个，先比较文件的顺序，总共分为以下几类：<br>
      <1> 新master使用的二进制文件编号比slave大，直接切换 <br>
      <2> 新master使用的二进制文件编号比slave小，直接切换，然后使用percona-toolkit修复 <br>
      <3> 新master和slave使用的二进制文件相同，新master最后的处理日志在master中对应的pos点比slave大或等于，直接切换，否则先切换在修复 <br>
## 脚本执行结果说明
1. 返回结果为True：切换成功
2. 返回结果为False：切换失败
## 脚本需要传入的参数
s_host:需要切换从库的ip <br>
s_port:需要切换从库的port <br>
s_user:需要切换从库的user <br>
s_passwd:需要切换从库的用户密码 <br>
nm_host:新master的ip <br>
nm_port:新master的port <br>
user：登录新master所在的服务器时用的用户 <br>
passwd:登录新master所在的服务器时用的密码（线上机器之间如果是免密，不用输入） <br>
## 使用时需要修改的地方
1. 在使用逻辑备份的过程中，数据库的安装目录可能不相同，数据库的启动方式可能不同
2. 在使用ssh和ftp时，可能有的环境修改了ssh和ftp的端口，使用时要检查
3. 使用时需在主机上安装paramiko模块、percona-toolkit、xtrabackup工具
4. 在使用percona-toolkit时，代码中用--recursion-method=hosts参数发现从库，所以使用时需要在从库的my.cnf文件添加report_host=‘本机ip’
5. 使用percona-toolkit校验数据时，如果连接不上数据库，需要在master上授权，因为在这个过程，需要创建checksums表
