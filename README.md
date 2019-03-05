![](docs/img/kingbus_logo.png)

[![Go Report Card](https://goreportcard.com/badge/github.com/flike/kingbus)](https://goreportcard.com/report/github.com/flike/kingbus)
[![Build Status](https://travis-ci.org/flike/kingbus.svg?branch=master)](https://travis-ci.org/flike/kingbus)
![](https://img.shields.io/github/license/flike/kingbus.svg)

## What is kingbus? [中文](README_ZH.md)
Kingbus is a distributed MySQL binlog store based on raft. Kingbus can act as a slave to the real master and as a master to the slaves in the same way as an intermediate MySQL master does. Kingbus has the following key features:

* MySQL replication protocol compatibility, pull the binlog files from the master through **gtid mode**, and push the binlog file to slave through **gtid mode** in the same way.

* Geo-Replication, kingbus uses Raft to support Geo-Replication. The binlog data written to the cluster is guaranteed to be consistent between multiple nodes, and the order of binlog event is exactly the same as that on the master.

* High availability, your mysql binlog replication is always on and continuously available with kingbus.

![](docs/img/kingbus_arch.png)

## Why need kingbus?

In a traditional MySQL replication setup a single master server is created and a set of slaves of MySQL servers are configured to pull the binlog files from the master, putting a lot of load on the master. 

* Introducing a layer between the master server and the slave servers can reduce the load on the master by only serving kingbus instead of all the slaves. 

* The slaves will only need to be aware of kingbus and not the real master server. Removing the requirement for the slaves to have knowledge of the master also simplifies the process of replacing a failed master within a replication environment.

* Kingbus allows us to horizontally scale our slaves without fear of overloading the network interface of the master

* Kingbus can also be used to avoide deep nested replication on remote sites, with kingbus you don't need e deeply nested replication.

* The size of the binlog storage space on the Master can be reduced, and store the binlog in kingbus.

* Support MYSQL database heterogeneous log based replication. Other heterogeneous replication components can be connected to the kingbus, such as [canal](https://github.com/alibaba/canal).

For more usage scenarios of binlog server, please refer:

* Booking: [blog](https://medium.com/booking-com-infrastructure/mysql-slave-scaling-and-more-a09d88713a20) 

* Facebook: [binlog server at Facebook](docs/binlog_server_at_fackbook.pdf)

* Google: [mysql-ripple](https://github.com/google/mysql-ripple)

## Quick start

Read the [Quick Start](docs/en/quick_start.md)

## Documentation

1.[Kingbus management API introduction](docs/en/admin_api.md)

2.[Start a kingbus cluster with docker-compose](docs/en/docker_compose.md)

## License

Kingbus is under the Apache 2.0 license. See the [LICENSES](LICENSES) file for details.

## Acknowledgments

* Thanks [etcd](https://github.com/etcd-io/etcd) for providing raft library.

* Thanks [go-mysql ](https://github.com/siddontang/go-mysql)for providing mysql replication protocol parser.
