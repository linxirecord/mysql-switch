# 主从复制切换脚本
## 脚本实现的功能：
1. 涵盖mysql的两种复制模式切换
2. 检查slave的io、sql线程状态
## 代码描述：
1. conn_database 连接数据库方法
2. check_slave 获取slave的复制状态方法
3. ssh_server备份数据库文件 (线上机器可能不是22，用时需要修改)
4. ftp_file下载备份文件
5. reuser_code 重用代码
6. switch_data 两种切换模式
## 脚本使用的环境：
1. 一个集群中，master宕机，此时没有数据写入<br>
2. 集群正常，因需求需要切换，支持数据写入时切换<br>
## 脚本思路:
### 核心分为两部分：<br>
#### master无故障切换：<br>
根据auto_position值可将复制模式分为两类：1. 传统复制   2. gtid复制<br>
两种模式在切换之前先执行stop slave，再执行start slave until bin_log_file="指定二进制文件"，pos="指定一个pos点"，此时两slave的数据一致，然后可以直接执行 change master to 切换.<br>
#### master故障切换：<br>
根据auto_position值可将复制模式分为两类：1. 传统复制   2. gtid复制<br>
1. 传统模式：锁表，新master逻辑备份，导入slave达到数据一致，然后直接切换，解锁.<br>
2. gtid模式：所有slave的二进制日志中同一个事务的gtid相同，可以取值比较，如果新master的值较大，说明其上的数据量多于slave，然后slave直接切换change master to ,因为gtid会进行比对，相同的gtid会被kill掉，直到找到自己没有的gtid值时开始复制；新master的值小，说明数据量少于slave，先将新master临时设为slave的从库，让数据达到一致，关闭新master的slave，然后再进行切换
## 脚本需要传入的参数
s_host:需要切换从库的ip <br>
s_port:需要切换从库的port <br>
s_user:需要切换从库的user <br>
s_passwd:需要切换从库的用户密码 <br>
nm_host:新master的ip <br>
nm_port:新master的port <br>
user：登录新master所在的服务器时用的用户 <br>
passwd:登录新master所在的服务器时用的密码（线上机器之间如果时免密，不用输入） <br>
