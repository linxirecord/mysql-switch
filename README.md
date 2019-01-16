# 主从复制切换脚本
## 脚本实现的功能：
1. 涵盖mysql的两种复制模式切换
2. 检查slave的io、sql线程状态
## 代码描述：
1. conn_database 连接数据库方法
2. check_slave 获取slave的复制状态方法
3. reuser_code 重用代码
4. switch_data 两种切换模式
