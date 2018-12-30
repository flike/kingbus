## 1.安装和运行

kingbus是一个raft集群，最好启动三个进程并且分布在三台机器上。例如在下面网络拓扑上部署kingbus:
![](../img/kingbus_topology.png)

1. 安装Go语言环境，具体步骤请Google。
2. 从releases page下载kinbus最新源码包并存储到目录： $GOPATH/src/github.com/flike/kingbus
3. cd $GOPATH/src/github.com/flike/kingbus
4. make
5. 设置配置文件（如何设置配置，详见[配置文件]()说明）。
6. 运行kingbus: nohup ./bin/kingbus -config=etc/kingbus.yaml &

## 2.启动binlog syncer

用户可以通过http API在kingbus会在Raft Cluster的Lead节点启动binlog syncer服务，这个服务会伪装成一个slave，从Real Master上同步binlog，启动命令如下：

```
curl -H "Content-Type:application/json" -X PUT --data '{"syncer_id":1001,"syncer_uuid":"f7e30637-f15a-11e8-accc-244427b6b60e","mysql_addr":"192.168.1.150:3415","mysql_user":"kb","mysql_password":"kb","semi_sync":false}' http://192.168.1.150:9595/binlog/syncer/start
```

PUT请求中有以下几个参数，意义如下：

- syncer_id：一个整数，对应该syncer的id，相对于mysql中的server_id
- syncer_uuid：string类型，对应该syncer的uuid，相对于mysql中的server_uuid
- mysql_addr：string类型，格式为：IP:PORT，表示Real Master的IP和端口。
- mysql_user：syncer连接Real Master使用的用户名，如果不存在，需要提前在Real Master上创建好，并且有对应的复制权限。
- mysql_password：syncer连接Real Master使用的密码。
- semi_sync：同步binlog是否开启半同步。

## 3.启动binlog server

在任意一台kingbus实例上，启动binlog server。binlog server相当于一个MySQL Master，会响应slave复制binlog的请求，并源源不断地推送binlog到slave。启动命令如下：

```
curl -H "Content-Type:application/json" -X PUT --data '{"addr":"192.168.1.149:3390","user":"kb","password":"kb"}' http://192.168.1.149:9595/binlog/server/start
```

PUT请求中有以下几个参数，意义如下：

- addr: binlog server监听的地址
- user: slave连接binlog server使用的用户名
- password: slave连接binlog server使用的密码

## 4.全量备份的生成和导入

我们已经在kingbus的一台实例上启动了一个binlog server服务，现在我们可以启动一个或多个slave来连接binlog
server，在启动slave之前，我们需要先导入一个real master的全量备份到slave，然后再开始同步。你可以使用mysqldump,xtrabackup,[mydumper](https://github.com/maxbube/mydumper)
等多种工具操作，以下我以mysqldump为例，展示在real master上生成全量备份，并导入slave的过程：

1. 在master上生成全量备份

```
./mysqldump -h127.0.0.1 -uroot -P3415 --all-databases --single-transaction >/home/kb/full_backup.sql
```

2. 在slave上导入全量备份

将full_backup.sql拷贝到slave机器上，导入全量备份：

```
#清除掉slave的Executed_Gtid_Set，否则会导入报错
MySQL [(none)]> reset master
MySQL [(none)]> quit
```

```
mysql -h127.0.0.1 -uroot -P3415 <./full_backup.sql
```

## 5. 启动slave
导入了全量备份后，现在slave可以开始同步日志，同步命令和原生搭建主从复制命令是一致的：

```
CHANGE MASTER TO MASTER_HOST = '192.168.1.149', MASTER_USER = 'kingbus', MASTER_PASSWORD = 'kingbus', MASTER_PORT = 3390, MASTER_AUTO_POSITION = 1, MASTER_RETRY_COUNT = 0, MASTER_HEARTBEAT_PERIOD = 100;
```

启动slave的io线程和sql线程

```
start slave
```

## 6.备注

1.通常情况下，real master中有数据，slave也有全量备份。这时候需要先将kingbus中的syncer启动起来，
等待kingbus中的binlog同步位点(gtid_set)超过slave后，才能将slave接到binlog server上。否则会导致同步失败，因为不满足binlog server中没有
最新的日志。




