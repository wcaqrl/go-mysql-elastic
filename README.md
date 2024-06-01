* 编译
```shell script
go build -o ./bin/go-mysql-elasticsearch
```
* 索引有变更的情况下需重新生成索引静态文件
```shell script
go-bindata -o=./asset/asset.go -pkg=asset indices/...
```

* 创建配置文件软链接,默认读取脚本执行路径下的 app.ini 文件,需建立软链接指向源配置文件
```shell script
ln -s "app-dev.ini" "app.ini"
```
* 启动
```shell script
./go-mysql-elasticsearch -op=lemon_media
```
* 日志搜集
```
# 日志索引名称约定 按月生成日志索引, 如 lemon_log202106
# 当前日志生产消费所用队列名称示例  lemon_log
# 参数(时间、域名、请求方法、请求Url、请求状态码、响应时间、用户代理、真实ip)决定唯一id
# 日志生产
./go-mysql-elasticsearch -op=lemon_log_parse
# 日志消费
./go-mysql-elasticsearch -op=lemon_log_collect
```

* 启动参数:
```
-config={值}  配置文件路径,比如 -config=app.ini 或 -config=/tmp/app.ini
-op={值} 必选参数 指定本次要写入的索引,可取值: 
        lemon_media,lemon_media_update,lemon_media_delete,lemon_media_ccr
        position  // 查询当前增量更新同到的binlog位置信息, 当同时存在参数 -filename  -pos 表示设置位置信息
        clear_version  // 去除全局version_id缓存,重置版本号为0
-ids={值} 可选参数 用逗号分隔的整型 当此参数不为空时,表示按主键新增或删除指定索引的文档
-resume_id={值} 可选参数, 正整数, 用于全量创建索引,突然中断时可从指定断点续写,详见文末使用说明
-start_time={值} 更新起始时间, 如 -start_time='2021-07-06 15:30:51'
-end_time={值}   更新结束时间, 如 -end_time='2021-07-07 15:51:06'
-yield  启动解析binlog进程,发送生产数据
-job    启动消费队列监听进程
        -driver=redis 默认以redis作为队列的驱动,保留rabbitmq作驱动
-h      帮助手册
``` 
* app.ini参数:
```
[app]
level={值} 指定日志打印级别,默认info, 可选 debug,info,warn,error,fatal,panic
logPath={值} 指定错误日志目录,例如 /tmp/go-mysql-elasticsearch/logs
save={值} 指定错误日志保存天数,默认7天
page_limit={值} 指定本次运行要写入多少次数据,比如 50表示向mysql数据库查询50次并写入对应索引;  默认0表示不限制写入次数
refresh={值} 指定增量更新的时间间隔,单位是秒
transfer_interval={值} 指定日志解析失败重试时间间隔,单位秒
sniffer_interval={值} 进程探活时间间隔,默认30,单位秒
handler_queue_length={值} 指定日志解析队列长度,默认4096
bulk_size=={值} 指定日志达到多少行即冲刷结果,默认1000条
flush_interval={值} 指定冲刷结果最长时间间隔,默认1000,单位毫秒
numbers={值} 消费模式下的并发数量,默认16
job_retries={值} 索引文档或删除文档的失败重试次数,默认3
period={值} 指定增量相对的时间点,单位是秒,默认3600秒,即查询过去1小时内更新过的数据
dict_period={值} 指定词库增量更新时间间隔,单位是分钟
is_full={bool} 是否保留全量索引,默认true(此时索引预排序字段为version_id)
[mysql]
sql_numbers={值} 指定内存中等待运行的sql语句条数,默认100
numbers={值} 全量写入时向mysql并发查询的数量
update_numbers={值} 增量写入时向mysql并发查询的数量
perpage={值} 指定每页查询条数
max_life_time={值} 指定连接最大活跃时长
skip_no_pk_table={bool} 是否容忍无主键的表, 默认false不容忍并终止; true 忽略此表
mysqldump={值} 默认留空(常用于全量备份)  是否优先启用mysqldump 是: 填写mysqldump执行路径(win示例 C:\xampp\mysql\bin\mysqldump.exe  linux示例 /usr/local/mysql/bin/mysqldump);否: 留空
skip_master_data={bool} 请保留默认值false  执行mysqldump时是否将binlog文件和位置记录在sql文件中 --master-data=1
[elastic]
shards={值} 索引分片数量,默认值3
replicas={值} 索引副本数量
replication.replicas={值} 复制集群的副本数
refresh_interval={值} 带有时间单位的字符串,表示文档更新后可查询到的时间间隔, 默认 1s
cpu_percent={值} elasticsearch集群cpu警界值
cpu_interval={值} cpu达到警界值时指定暂停写入的时间间隔, 单位秒
numbers={值} 并发向elasticsearch写入的时的并发数,建议与集群节点的逻辑cpu核数相等
bulk_number={值} 指定单次向elasticsearch写入的条数
[redis]
job_key={值} 指定redis作为驱动的情况下,读取的有序集合键名
[log]
path={值} 需要解析的nginx日志文件路径
response_time={值} 接口响应时长过滤,单位毫秒
[storage]
path={值} 存储postion信息的文件(相对|绝对)路径,默认storage/position.db,读写权限
[rule-N]
[rule-1]
;数据库名
database=test
;表名,填*表示匹配所有表
table=student
;包含的列,多值逗号分隔,如: id,name,age,area_id  为空时表示包含全部列
include_columns=
;排除掉的列,多值逗号分隔,如：id,name,age,area_id  默认为空
exclude_columns=
;date类型格式化,默认yyyy-MM-dd
date_formatter=
;datetime,timestamp类型格式化, 不填写默认yyyy-MM-dd HH:mm:ss
datetime_formatter=
;数据类型,支持string,hash,list,set,sortedset类型(与redis的数据类型一致)
redis_structure=string
;key的前缀,写入redis缓存的键名前缀
redis_key_prefix=
;使用哪一列的值作为key,默认使用主键
redis_key_column=
;key的值(固定值), 当redis_structure为hash,list,set,sortedset此值不能为空
redis_master_key=
;sortedset的计算score时对应的列名称,如果没有则留空
redis_score_column=
;当选择存储到list结构时,是否将数据转储到有序集合sortedset去重;默认false
redis_list_to_set=
;当选择存储到list结构且转储到有序集合时,指定该有序集合的键名
redis_list_to_set_key=
;当选择存储到list结构且转储到有序集合时,指定队列超长暂停时间,单位秒; 默认 0 表示不限制
redis_list_to_set_interval=
;当选择存储到list结构且转储到有序集合且限制队列长度时,指定队列最大长度; 默认 0 表示不限制
redis_list_to_set_length=
``` 

* 自定义词库说明:
```
(1)需要配置elasticsearch 7.8.0服务器远程[sftp]文件传输
(2)在elasticsearch 7.8.0服务器上启用nginx提供内部请求服务
    http://127.0.0.1:8081/lemon_add_custom.dic
(3)elasticsearch 7.8.0服务器ik插件配置项修改如下：
    vim /home/es7/app/elasticsearch-7.8.0/config/analysis-ik/IKAnalyzer.cfg.xml

    <?xml version="1.0" encoding="UTF-8"?>
    <!DOCTYPE properties SYSTEM "http://java.sun.com/dtd/properties.dtd">
    <properties>
            <comment>IK Analyzer 扩展配置</comment>
            <!--用户可以在这里配置自己的扩展字典 -->
            <entry key="ext_dict">lemon_full_custom.dic;apple_full_custom.dic</entry>
             <!--用户可以在这里配置自己的扩展停止词字典-->
            <entry key="ext_stopwords">ext_stopword.dic</entry>
            <!--用户可以在这里配置远程扩展字典 -->
            <entry key="remote_ext_dict">http://127.0.0.1:8081/lemon_add_custom.dic;http://127.0.0.1:8081/apple_add_custom.dic</entry>
            <!--用户可以在这里配置远程扩展停止词字典-->
            <!-- <entry key="remote_ext_stopwords">http://lemon.com/service/public/dict/stopword.dic</entry> -->
    </properties>
(4) 自定义词库的sql建表语句
    create table if not exists fs_custom_dict (
    	id int(11) unsigned not null auto_increment comment '关键词id',
    	word varchar(50) not null default '' comment '关键词',
    	type tinyint(1) not null default 1 comment '关键词类型: 1,扩展词库; 2停用词库',
    	status tinyint(1) not null default 1 comment '关键词状态: 1,正常; 0,暂停',
    	update_time  int(10) not null default 0 comment '更新时间',
    	primary key (id),
    	unique key word (word),
        key update_time(update_time)
    )engine=InnoDB auto_increment=1 charset=utf8mb4 comment = 'elasticsearch远程词库表';
``` 
* 脚本执行顺序:
```
Step 1
执行dict全量命令上传全量词库
Step 2
执行dict增量命令热更新增量词库
Step 3
各索引执行全量写入命令
Step 4
各索引启动增量更新命令
``` 

* 查找增量日志位置shell脚本
```
# 运行示例 /your/path/to/search.sh -e '/usr/bin/mysqlbinlog' -i 'mysql-bin.index' -d '/var/log/mysql' -f '2021-09-03 15:10:00' -s '2021-09-03 16:11:25'
-h 查看帮助选项
-e mysqlbinlog的执行路径, option
-d 日志文件所在目录,建议为绝对路径, option
-i 日志索引文件名称, option
-f 查找日志的起始时间,required,形如 2006-01-02 15:04:05
-t 查找日志的结束时间,option,默认当前时间,形如 2006-01-02 15:04:05
-s 给定的查找时间,required
-l 脚本运行时log文件路径,形如 /tmp/search.log
```

* 断点续写参数使用说明
```
# 使用场景
  当全量写入较大的索引时,若意外中断,可在写入操作指定断点,并在断点处续写数据
# 前置条件
  1) 找出中断索引中已有的最大id值 max_id
  2) 从mysql数据库中按指定筛选条件的sql语句查出断点id值,尽量保证不丢失数据,通常要补足配置文件中的sql_numbers
     如 resume_id = select video_id as id from fv_video where video_id<={max_id} {some filters} order by video_id desc limit {sql_numbers},1
  3) 给中断索引指定别名, 命令示例
     curl -u elastic:DOAbkn7pnqtcwTPinUY9  -X POST '172.17.12.120:9200/_aliases' -H "Content-Type: application/json" -d '{{ "remove":  { "index": "旧的索引带日期的全名", "alias": "lemon_media" }},{ "add":  { "index": "被中断索引带日期的全名", "alias": "lemon_media" }}'
# 命令示例
./go-mysql-elasticsearch -config=/tmp/app.ini -op=lemon_media -resume_id=194226947
```