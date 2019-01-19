# Start a kingbus cluster with docker-compose

Kingbus supports launching the cluster through docker-compose mode. The command is as follows:

1. Download the source package and extract it, cd kingbus
2. docker-compose up -d
3. curl http://127.0.0.1:5000/cluster Get the kingbus cluster information

Then you can use the curl command to operate the kingbus cluster, such as connecting the master, starting the binlog server, etc.

stop docker compose:

docker-compose down

rebuild docker-compose:

docker-compose up --build
