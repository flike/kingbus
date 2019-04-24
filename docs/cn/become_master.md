# kingbus架构设计之如何伪装成MySQL Master角色

## 1.背景

kingbus是一个基于raft强一致协议实现的分布式MySQL binlog 存储系统。它能够充当一个MySQL Slave从真正的Master上同步binglog，并存储在分布式集群中；同时又充当一个MySQL Master将集群中的binlog 同步给其他Slave。这就需要kingbus具备伪装成MySQL Master角色，给其他Slave发送binlog的能力。 

网络上有很多文章，介绍如何伪装成MySQL Slave角色，从Master上同步binlog，并解析同步到其他存储系统中，例如ES。以此来支持异构复制。本文着重介绍一下，kingbus如何实现一个伪Master的功能。

## 2. 实现原理

### 2.1 启动MySQL主从复制的命令

在基于Gtid模式的主从复制下，设置好了MySQL的相关配置，我们会在Slave上执行如下命令来启动主从复制：

```sql
STOP SLAVE;
RESET SLAVE;
RESET MASTER;
SET @@GLOBAL.GTID_PURGED ='214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-11756,
6098d558-96ec-11e8-a2d7-fa163ef40021:1-41,
b93fc470-a1fc-11e8-9b46-fa163e8d0f56:1-29987'

CHANGE MASTER TO MASTER_HOST = '192.168.1.10', MASTER_USER = 'kb', MASTER_PASSWORD = 'kb', MASTER_PORT = 3415, MASTER_AUTO_POSITION = 1, MASTER_RETRY_COUNT = 0, MASTER_HEARTBEAT_PERIOD = 10;
START SLAVE;
```

### 2. MySQL内部实现

上述命令，对应到Slave内部，Slave会发生如下命令到Master节点：

```
SELECT UNIX_TIMESTAMP() (rpl_slave.cc:get_master_version_and_clock)
SHOW VARIABLES LIKE ‘SERVER_ID’ (rpl_slave.cc:get_master_version_and_clock)
SET @master_heartbeat_period=? (rpl_slave.cc:get_master_version_and_clock)
SET @master_binlog_checksum= @@global.binlog_checksum (rpl_slave.cc:get_master_version_and_clock)
SELECT @master_binlog_checksum (rpl_slave.cc:get_master_version_and_clock)
SELECT @@GLOBAL.GTID_MODE (rpl_slave.cc:get_master_version_and_clock)
SHOW VARIABLES LIKE ‘SERVER_UUID’ （rpl_slave.cc:get_master_uuid）
SET @slave_uuid= ‘%s’（rpl_slave.cc:io_thread_init_commands)
COM_REGISTER_SLAVE(rpl_slave.cc:register_slave_on_master)
COM_BINLOG_DUMP(rpl_slave.cc:request_dump)
```

## 3. kingbus的伪Master实现

kingbus需要实现上述所有命令，这些命令的请求包格式和响应格式，在MySQL协议文档中都有具体的定义。感兴趣的同学可以参考kingbus相关代码文件：

[command.go]: https://github.com/flike/kingbus/blob/master/mysql/command.go

这个文件实现了上面全部命令。这样kingbus就可以成功地伪装成一个master了。然后源源不断地发送binlog event给slave。