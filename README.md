# 主从复制切换脚本
## 脚本实现的功能：
1. 涵盖mysql的两种复制模式切换
2. 检查slave的io、sql线程状态
## 代码描述：
1. conn_database 连接数据库方法
2. check_slave 获取slave的复制状态方法
3. reuser_code 重用代码
4. switch_data 两种切换模式
## 脚本思路:
### 核心分为两部分：<br>
#### master无故障切换：<br>
根据auto_position值可将复制模式分为两类：1. 传统复制   2. gtid复制<br>
两种模式在切换之前所有slave执行stop slave until bin_log_file="指定二进制文件"，pos="指定一个pos点"，此时两slave的数据一致，然后可以直接执行 change master to 切换.<br>
#### master故障切换：<br>
根据auto_position值可将复制模式分为两类：1. 传统复制   2. gtid复制<br>
1. 传统模式：锁表，新master逻辑备份，导入slave达到数据一致，然后直接切换，解锁.<br>
2. gtid模式：所有slave的二进制日志中同一个事务的gtid相同，可以取值比较，如果新master的值较大，说明其上的数据量多于slave，然后slave直接切换change master to ,因为gtid会进行比对，相同的gtid会被kill掉，直到找到自己没有的gtid值时开始复制；新master的值小，说明数据量少于slave，这个还没有啥好方法，最坏用备份.
