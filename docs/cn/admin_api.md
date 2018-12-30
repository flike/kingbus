## kb管理端命令
kb管理端命令都是通过http API触发的。主要分为以下几类：
* raft cluster membership操作，查看集群状态，添加一个节点、移除一个节点，更新节点信息等
* binlog syncer相关操作，启动一个binlog syncer，停止binlog syncer，查看binlog syncer状态。
* binlog server相关操作，启动一个binlog server，停止binlog server，查看binlog server状态。

## 1. raft cluster membership操作

### 1.1获取集群状态信息

API

```
curl http://192.168.1.150:9595/cluster

```

Response：

```
{  
   "message":"success",
   "data":[  
      {  
         "Id":"757200e01244565",
         "peerURLs":[  
            "http://192.168.1.150:9698"
         ],
         "name":"kingbus_150",
         "adminURLs":[  
            "http://192.168.1.150:9595"
         ],
         "isLeader":false
      },
      {  
         "Id":"13fbbb5c6c773e83",
         "peerURLs":[  
            "http://192.168.1.148:9696"
         ],
         "name":"kingbus_148",
         "adminURLs":[  
            "http://192.168.1.148:9595"
         ],
         "isLeader":false
      },
      {  
         "Id":"e2a0e8de3b98eec6",
         "peerURLs":[  
            "http://192.168.1.149:9697"
         ],
         "name":"kingbus_149",
         "adminURLs":[  
            "http://192.168.1.149:9595"
         ],
         "isLeader":true
      }
   ]
}
```

### 1.2添加节点到集群

API

```
curl -H "Content-Type:application/json" \
-X POST \
--data '{"name":"kingbus_148","peer_url":"http://192.168.1.148:9696"}' \
http://192.168.1.149:9595/members
```

response:

```
{  
   "message":"success",
   "data":[  
      {  
         "Id":528926725760107877,
         "peerURLs":[  
            "http://192.168.1.150:9698"
         ],
         "name":"kingbus_150",
         "adminURLs":[  
            "http://192.168.1.150:9595"
         ]
      },
      {  
         "Id":11102851159674377966,
         "peerURLs":[  
            "http://192.168.1.148:9696"
         ],
         "name":"kingbus_148"
      },
      {  
         "Id":16330308290025680582,
         "peerURLs":[  
            "http://192.168.1.149:9697"
         ],
         "name":"kingbus_149",
         "adminURLs":[  
            "http://192.168.1.149:9595"
         ]
      }
   ]
}
```

### 1.3从集群中删除节点

API

```
curl -H "Content-Type:application/json" \
-X DELETE \
--data '{"name":"kingbus_148","peer_url":"http://192.168.1.148:9696"}' \
http://192.168.1.149:9595/members
```
通过调用获取集群状态信息的API获得name和peer_url信息

response：

```
{  
   "message":"success",
   "data":[  
      {  
         "Id":528926725760107877,
         "peerURLs":[  
            "http://192.168.1.150:9698"
         ],
         "name":"kingbus_150",
         "adminURLs":[  
            "http://192.168.1.150:9595"
         ]
      },
      {  
         "Id":16330308290025680582,
         "peerURLs":[  
            "http://192.168.1.149:9697"
         ],
         "name":"kingbus_149",
         "adminURLs":[  
            "http://192.168.1.149:9595"
         ]
      }
   ]
}
```

### 1.4更新节点信息

只能更新节点的raft协议通讯接口

API

```
curl -H "Content-Type:application/json" \
-X PUT \
--data '{"name":"kingbus_149","new_peer_url":"http://192.168.1.149:9699"}' \
http://192.168.1.150:9595/members
```

response

```
{  
   "message":"success",
   "data":[  
      {  
         "Id":528926725760107877,
         "peerURLs":[  
            "http://192.168.1.150:9698"
         ],
         "name":"kingbus_150",
         "adminURLs":[  
            "http://192.168.1.150:9595"
         ]
      },
      {  
         "Id":3934485057008157414,
         "peerURLs":[  
            "http://192.168.1.148:9696"
         ],
         "name":"kingbus_148",
         "adminURLs":[  
            "http://192.168.1.148:9595"
         ]
      },
      {  
         "Id":16330308290025680582,
         "peerURLs":[  
            "http://192.168.1.149:9699"
         ],
         "name":"kingbus_149",
         "adminURLs":[  
            "http://192.168.1.149:9595"
         ]
      }
   ]
}
```

## 2. binlog syncer操作

一个kingbus集群只能有一个binlog syncer，并且运行在Lead节点上。但启动、停止和查看binlog syncer的http API调用，可以在任意节点执行，
所有的请求都会转发请求到Lead节点执行，并返回结果。

### 2.1启动binlog syncer

API

```
curl -H "Content-Type:application/json" -X PUT --data '{"syncer_id":1001,"syncer_uuid":"f7e30637-f15a-11e8-accc-244427b6b60e","mysql_addr":
"192.168.1.150:3415","mysql_user":"kb","mysql_password":"kb","semi_sync":false}' http://192.168.1.149:9595/binlog/syncer/start
```

response

```
{"message":"success","data":""}
```

### 2.2停止binlog syncer

API

```
curl -H "Content-Type:application/json" -X PUT http://192.168.1.149:9595/binlog/syncer/stop
```

response

```
{"message":"success","data":""}
```

### 2.3查看binlog syncer信息

API

```
curl -H "Content-Type:application/json" -X GET http://192.168.1.149:9595/binlog/syncer/status
```

response

```
{  
   "message":"success",
   "data":{  
      "current_gtid":"",
      "executed_gtid_set":"214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-9257530",
      "last_binlog_file":"mysql_bin.000006",
      "last_file_position":194,
      "mysql_addr":"192.168.1.150:3415",
      "mysql_password":"kb",
      "mysql_user":"kb",
      "purged_gtid_set":"",
      "semi_sync":false,
      "status":"running",
      "syncer_id":0,
      "syncer_uuid":""
   }
}
```


## 3. binlog server 操作

### 3.1启动binlog server

API

```
curl -H "Content-Type:application/json" -X PUT --data '{"addr":"192.168.1.149:3390","user":"kb","password":"kb"}' http://192.168.1.1
49:9595/binlog/server/start
```
response

```
{"message":"success","data":""}
```

### 3.2停止binlog server

API

```
curl -H "Content-Type:application/json" -X PUT http://192.168.1.149:9595/binlog/server/stop
```

response

```
{"message":"success","data":""}
```

### 3.3查看binlog server

API

```
curl http://192.168.1.149:9595/binlog/server/status
```
response

```
{  
   "message":"success",
   "data":{  
      "addr":"192.168.1.149:3390",
      "user":"kb",
      "password":"kb",
      "slaves":[  

      ],
      "current_gtid":"",
      "last_binlog_file":"mysql_bin.000006",
      "last_file_position":194,
      "executed_gtid_set":"214eb71f-96ec-11e8-ab4f-fa163e72d4ae:1-9257530",
      "purged_gtid_set":"",
      "status":"running"
   }
}
```

