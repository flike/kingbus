## Kingbus Admin Command
The kingbus console commands are triggered by the http API. Mainly divided into the following categories:
* raft cluster membership operation, view cluster status, add a node, remove a node, update node information, etc.
* binlog syncer related operations, start a binlog syncer, stop binlog syncer, view binlog syncer status.
* binlog server related operations, start a binlog server, stop the binlog server, check the binlog server status.

## 1. Raft cluster membership operation

### 1.1 Get the status information of cluster

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

### 1.2 Add a node to the cluster

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

### 1.3 Remove a node from the cluster

API

```
curl -H "Content-Type:application/json" \
-X DELETE \
--data '{"name":"kingbus_148","peer_url":"http://192.168.1.148:9696"}' \
http://192.168.1.149:9595/members
```
Get the name and peer_url information by calling the API that gets the cluster status information.

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

### 1.4 Update node information

Can only update the node's raft protocol communication url

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

## 2. Binlog syncer operation

A kingbus cluster can only have one binlog syncer and runs on the Lead node. But the http API call of start, stop, and view operation for binlog syncer, which can be executed at any node.
All requests will forward the Lead node.

### 2.1 Start binlog syncer

API

```
curl -H "Content-Type:application/json" -X PUT --data '{"syncer_id":1001,"syncer_uuid":"f7e30637-f15a-11e8-accc-244427b6b60e","mysql_addr":
"192.168.1.150:3415","mysql_user":"kb","mysql_password":"kb","semi_sync":false}' http://192.168.1.149:9595/binlog/syncer/start
```

response

```
{"message":"success","data":""}
```

### 2.2 Stop binlog syncer

API

```
curl -H "Content-Type:application/json" -X PUT http://192.168.1.149:9595/binlog/syncer/stop
```

response

```
{"message":"success","data":""}
```

### 2.3 Get the status information of binlog syncer

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


## 3. Binlog server operation

### 3.1 Start binlog server

API

```
curl -H "Content-Type:application/json" -X PUT --data '{"addr":"192.168.1.149:3390","user":"kb","password":"kb"}' http://192.168.1.1
49:9595/binlog/server/start
```
response

```
{"message":"success","data":""}
```

### 3.2 Stop binlog server

API

```
curl -H "Content-Type:application/json" -X PUT http://192.168.1.149:9595/binlog/server/stop
```

response

```
{"message":"success","data":""}
```

### 3.3 Get the status information of binlog server 

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

