# 通过docker-compose启动一个kingbus集群

kingbus支持通过docker-compose方式启动集群，命令如下：

1. 下载源码包并解压，cd kingbus
2. docker-compose up -d
3. curl http://127.0.0.1:5000/cluster 获取kingbus集群信息

然后可以通过curl命令操作kingbus集群，例如连接master，启动binlog server等

停止docker-compose:

docker-compose down

重新构建docker-compose:

docker-compose up --build