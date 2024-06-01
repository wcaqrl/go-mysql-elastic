#!/bin/bash

:<<EOF
# 命令原型
mysqlbinlog -v --base64-output=decode-rows mysql-bin.000002 --start-datetime='2021-09-02 19:40:00' --stop-datetime='2021-09-02 20:00:00' | grep -i -A 2 '# at '

# 格式提醒
如果在windows环境下编辑过,在linux请先执行 sed -i 's/\r$//' position.sh && chmod +x position.sh

# 参数说明
-e mysqlbinlog的执行路径
-d 日志文件所在目录,建议为绝对路径
-i 日志索引文件名称
-f 查找日志的起始时间,required,形如 2006-01-02 15:04:05
-t 查找日志的结束时间,默认当前时间,形如 2006-01-02 15:04:05
-s 给定的查找时间,required
-l 脚本运行时log文件路径,形如 /tmp/search.log
EOF
#exec_path='/usr/local/mysql/bin/mysqlbinlog'
#file_dir='/home/mysql/data'
exec_path='/usr/bin/mysqlbinlog'
file_dir='/var/log/mysql'
index_file="mysql-bin.index"
from=`date -d @0 "+%Y-%m-%d %H:%M:%S"`
to=`date "+%Y-%m-%d %H:%M:%S"`
search_time=0
tmp_log_file='./tmp.log'

# 组织参数并校验
while getopts ":e:d:i:f:t:s:l:h" opt
do
  case $opt in
  e)
    exec_path=$OPTARG
    ;;
  d)
    file_dir=$OPTARG
    ;;
  i)
    index_file=$OPTARG
    ;;
  f)
    from=$OPTARG
    ;;
  t)
    to=$OPTARG
    ;;
  s)
    search_time=`date -d "${OPTARG}" +%s`
    ;;
  l)
    tmp_log_file=$OPTARG
    ;;
  h)
    echo '请求参数列表:
    -e mysqlbinlog的执行路径
    -d 日志文件所在目录,建议为绝对路径
    -i 日志索引文件名称
    -f 查找日志的起始时间,required,形如 2006-01-02 15:04:05
    -t 查找日志的结束时间,默认当前时间,形如 2006-01-02 15:04:05
    -s 给定的查找时间,required
    -l 脚本运行时log文件路径,形如 /tmp/search.log'
    exit 0
    ;;
  ?)
    echo "unknown param"
    exit 1
    ;;
  esac
done

from_ts=`date -d "$from" +%s`
to_ts=`date -d "$to" +%s`

if [[ $from_ts -le 0 ]]; then
  echo "start time invalid"
  exit 0
fi

if [[ $search_time -le 0 ]]; then
    echo "search time invalid"
    exit 0
  elif [[ $search_time -lt $from_ts ]] || [[ $search_time -gt $to_ts ]]; then
    echo "search time must between start time and end time"
    exit 0
fi

index_file="${file_dir}/${index_file}"
# 判断日志索引文件是否存在
if [ ! -f "${index_file}" ];then
  echo -e "There is no index file of mysql binlog\n"
  exit 0
fi

:<<EOF
根据给定的内容查找位置
$1 10位时间戳
$2 按行分割的日志记录
EOF

function search_position() {
  pos=0
  time_int=0
  at_reg="^# at ([0-9]+)"
  time_reg="^#(([0-9]{6}|[0-9]{8}) +[0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}) +server .*"
  flag_line=-1
  lines=()
  i=0
  while read -r line || [[ -n $line ]]; do
    if [[ "$line" =~ $time_reg ]]; then
        ((i++))
        time_str=`echo $line | sed -r "s/${time_reg}/\1/g"`
        time_digital=`date -d "${time_str}" +%s`
        if [[ $time_int = 0 ]] || [[ $time_int -gt $time_digital ]]; then
          time_int=$time_digital
        fi
        # 遇到小于参考时间的则终止
        if [[ $time_digital -le $1 ]]; then
          flag_line=$i
        fi
      elif [[ "$line" =~ $at_reg ]]; then
        ((i++))
        if [[ $flag_line -ge 0 ]] && [[ $flag_line -eq `expr $i - 1` ]]; then
          pos=`echo $line | sed -r "s/${at_reg}/\1/g"`
          break
        fi
      else
        continue
    fi
  done <<< $(tac $2)
  echo $pos $time_int
}

:<<EOF
遍历文件列表
$1 10位时间戳
$2 按行分割的日志记录
EOF

cat_files=`tac $index_file`
echo "total:     `wc -l $index_file | awk '{print $1}'` files"
for f in ${cat_files}
do
  filename=""
  if [ ${f:0:1} = '/' ];then
      filename=${f}
    elif [ ${f:0:2} = './' ];then
      filename="$file_dir/${f:2}"
    else
      filename="$file_dir/${f}"
  fi
  # 如果找得到文件,就进行查找
  if [[ "$filename" != "" ]]; then
    # echo ""${exec_path}" -v --base64-output=decode-rows "$filename" --start-datetime='"${from}"' --stop-datetime='"${to}"' | grep -i -A 1 '# at '"
    echo "reading:   $filename"
    res=`"${exec_path}" -v --base64-output=decode-rows "$filename" --start-datetime="${from}" --stop-datetime="${to}" | grep -i -A 1 '# at ' > "$tmp_log_file"`
    sleep 5s
    echo "searching: $filename"
    pos_and_time=`search_position "$search_time" "$tmp_log_file"`
    pos_arr=(${pos_and_time})
    if [[ ${pos_arr[0]} -gt 0 ]]; then
        printf '=%.0s' {1..64}
        echo ''
        rm -f $tmp_log_file
        echo "result:    $filename ${pos_arr[0]}"
        exit 0
      elif [[ ${pos_arr[1]} -gt 0 ]]; then
          to=`date -d @"${pos_arr[1]}" '+%Y-%m-%d %H:%M:%S'`
    fi
  fi
done
printf '=%.0s' {1..64}
echo ''
rm -f $tmp_log_file
echo "result:    cannot find any position"
exit 0


