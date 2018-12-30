#!/bin/bash
EC_OK=0
EC_UNKOWN_ERR=100
EC_PARAM_ERR=101
EC_DB_ERR=102


function print_usage()
{
  echo "Usage: $0 -h<host> -P<port> -u<user> -p<passwd> -D<database> --time=<time>  [--reset]"
}

function exit_wrapper()
{
  local final_errno=$1
  local error=$2

  for f in ${files_to_rm[@]}; do
    rm -rf $f
  done

  if [ -z "$3" ]; then
    echo "{\"errno\":$final_errno,\"error\":\"$error\"}"
  else
    echo "{\"errno\":$final_errno,\"error\":\"$error\",$3}"
  fi
  exit $final_errno
}

function test_errno()
{
  local errno=$?
  if [ $errno -ne 0 ]; then
    local final_errno=$1
    local error=$2
    if [ $errno -ne 0 ]; then
      exit_wrapper $final_errno $error
    fi
  fi
}

function rand()
{
  min=$1
  max=$(($2-$min+1))
  num=$(date +%s%N)
  echo $(($num%$max+$min))
}

# 插入奇数id的记录
function insert() {
  local string_value=$(cat /dev/urandom | head -n 10 | md5sum | head -c 5)
  local int_value=$(rand 1 500000)
  local float_value=$(awk "BEGIN{print $int_value / $RANDOM }")
  local date_value=$(date '+%Y-%m-%d %H:%M:%S' )


  mysql ${mysql_connect_str}    << EOF
insert into kb_test set varchar_var='$string_value', tinyint_var=1, text_var='$string_value',
date_var='2018-01-01 00:00:00', smallint_var=$RANDOM, mediumint_var=$int_value, bigint_var=$int_value,
float_var=$float_value, double_var=$float_value, decimal_var=$float_value,
datetime_var='$date_value',  time_var='00:00:00',year_var=2018,
char_var='$string_value',tinyblob_var='$string_value', tinytext_var='$string_value',
blob_var='$string_value', mediumblob_var='$string_value', mediumtext_var='$string_value',longblob_var='$string_value',
longtext_var='$string_value',enum_var='1', set_var='3', bool_var=true,varbinary_var='$string_value',
bit_var=$int_value;
EOF

}

# 删除奇数id中最小的一行
function delete_odd() {
  mysql ${mysql_connect_str}  -Nse "delete from kb_test where id%2=1  order by id limit 1;"
}

function delete_even() {
  mysql ${mysql_connect_str}  -Nse "delete from kb_test where id%2=0 order by id limit 1;"
}


# 更新奇数id中最大的3行
function update_odd() {
  mysql ${mysql_connect_str}  -Nse "update kb_test set tinyint_var=$1 where id%2=1 order by id desc limit 3;"
}

function update_even() {
  mysql ${mysql_connect_str}  -Nse "update kb_test set tinyint_var=$1 where id%2=0 order by id desc limit 3;"
}

# 对数据库进行reset操作，会清空数据
function reset() {
  mysql ${mysql_connect_str}    << EOF
DROP TABLE IF EXISTS kb_test;
CREATE TABLE kb_test  (
varchar_var varchar(20) NOT NULL,
tinyint_var tinyint(4) NOT NULL,
text_var text NOT NULL,
date_var date NOT NULL,
smallint_var smallint(6) NOT NULL,
mediumint_var mediumint(9) NOT NULL,
id int(11) NOT NULL auto_increment,
bigint_var bigint(20) NOT NULL,
float_var FLOAT(10, 2) NOT NULL ,
double_var DOUBLE NOT NULL ,
decimal_var DECIMAL(10, 2) NOT NULL ,
datetime_var datetime NOT NULL,
update_time timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
time_var time NOT NULL,
year_var year(4) NOT NULL,
char_var char(10) NOT NULL,
tinyblob_var tinyblob NOT NULL,
tinytext_var tinytext NOT NULL,
blob_var blob NOT NULL,
mediumblob_var mediumblob NOT NULL,
mediumtext_var mediumtext NOT NULL,
longblob_var longblob NOT NULL,
longtext_var longtext NOT NULL,
enum_var enum('3','2','1') NOT NULL,
set_var SET('3','2','1') NOT NULL,
bool_var tinyint(1) NOT NULL,
varbinary_var varbinary(20) NOT NULL,
bit_var bit(64) DEFAULT NULL,
PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
EOF

}


if [ $# -eq 0 ]; then
  print_usage
  exit 0
fi

ARGS=`getopt -o h:,P:,u:,p:,D: -l time:,reset -- $0 $@`
if [ $? -ne 0 ]; then
  exit_wrapper $EC_PARAM_ERR "parse param failed"
fi

eval set -- "${ARGS}"
while true; do
  case "$1" in
    -h)
      host=$2
      shift 2
      ;;
    -P)
      port=$2
      shift 2
      ;;
    -u)
      user=$2
      shift 2
      ;;
    -p)
      passwd=$2
      shift 2
      ;;
    -D)
      db=$2
      shift 2
      ;;
    --time)
      time=$2
      shift 2
      ;;
    --reset)
      need_reset=true
      shift 1
      ;;
    --)
      shift
      break
      ;;
    *)
      exit_wrapper $EC_PARAM_ERR "parse param failed"
      ;;
  esac
done

if [ -z $host ] || [ -z $port ] || [ -z $user ]; then
  exit_wrapper $EC_PARAM_ERR "exists some empty params"
fi

mysql_connect_str="-h$host -P$port -u$user"
if [ -n "$passwd" ]; then
  mysql_connect_str="${mysql_connect_str} -p$passwd"
fi
mysql_connect_str="${mysql_connect_str} -D$db"


if [ "$need_reset" == "true" ]; then
  reset
  test_errno $EC_DB_ERR "reset database failed"
fi

sleep_time=0.001
now=$(date '+%s')
target_time=$((now + time))

  count=0
  while true;do
    ((count += 1))
      insert
      insert
      insert
      insert

      delete_even
      delete_odd

      update_even 3
      update_odd 3

    now=$(date '+%s')
    if [ $now -gt $target_time ]; then
      break
    fi
  sleep $sleep_time
  done

exit_wrapper $EC_OK "success"



