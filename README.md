### check data文件说明
```
check_data_mysql.py  核对mysql\tidb\doris之间的数据
check_data_mongo.py  核对mongo数据
check_data_pgsql.py  核对postgresql数据
```

### check_data_mysql脚本使用说明（其他类似）


#### check_data_mysql.py
非侵入式，支持在程序端和数据库服务器做crc32sum,效率高，支持跨平台对比：比如mysql\tidb\doris对比。

#### pt-table-checksum 
侵入式，会生成语句级别的binlog，需要在所有的备库重复计算crc32sum，容易导致备库延时，在繁忙的数据库上使用可能导致备库延时，效率低。


### check_data_mysql使用场景

1、核对2个非主备关系的myql数据库使用check_data_mysql.py

2、在mysql、tidb、doris之间核对数据使用check_data_mysql.py --mode=slow

3、需要修复某种类型（不是全部修复，比如只修复目标库缺失的数据、只修复目标库多出的数据或修复值不同的数据），请使用check_data_mysql.py

4、主备数据库量大，可以先使用check_data_mysql.py定位到不一致的表和主键，再使用pt-table-checksum复核，pt-table-sync 进行修复。

5、其他情况使用pt-table-checksum


先使用check_data_mysql.py缩小核对数据的范围
pt-table-checksum h=10.0.0.43,P=3306,u=dba_rw,p='abc' \
--no-check-replication-filters --no-check-binlog-format --replicate=bakdb.checksums \
--max-load=threads_running=100 --check-interval=1s --max-lag=30s --recurse=1 \
--recursion-method dsn=D=bakdb,t=dsns \
--databases=banggood_work --tables=orders --where "orders_id>=100000000"



同步不一致的数据

pt-table-sync h=10.177.69.43,P=3306,u=dba_rw,p='abc' --replicate=bakdb.checksums --no-check-slave --databases=banggood_work --tables=customers_basket --print #不加--execute参数，只显示不执行



核对172.30.32.6:33064和172.30.32.8:33064的数据：

./check_data_mysql.py -S 172.30.32.6:33064 -T 172.30.32.8:33064 -d ewsus  &> check_data.log


如果核对实例没有默认账号，可使用-u -p指定用户名和密码：

./check_data_mysql.py -S 172.30.32.6:33064 -T 172.30.32.8:33064 -d ewsus  -u root -p abc



如果表没有主键，可以使用-k指定一个唯一键进行核对

./check_data_mysql.py -S 172.30.32.6:33064 -T 172.30.32.8:33064 -d ewsus  -k id



-r 10 开启复核,不一致的数据复核10次

./check_data_mysql.py -S 172.30.32.6:33064 -T 172.30.32.8:33064 -d ewsus  -r 10



-R 100  首次核对不一致数据超过100行，跳过复核，该参数在-r开启后方可生效

./check_data_mysql.py -S 172.30.32.6:33064 -T 172.30.32.8:33064 -d ewsus  -r 10 -R 100



check_data_mysql.py核对过程说明：

1、把source端的表和target端的表数据做crc32计算，再下载下来，保存到 表名.source 、表名.target

2、对 表名.source 、表名.target 的数据 进行第一次核对：对比 主键和crc32校验码。检验不一致的数据会保存到 表名.chg 、表名.del、表名.add 文件

3、不一致的数据可能是同步延时导致的，所以需要复核。复核开关由-r和-R控制。进行复核时，每条不一致的数据会间隔1秒，重复核对，直至复核通过或达到最大复核次数。



check_data_mysql.py日志说明：

RESULT：YES:核对一致   NO：核对不一致

INFO: 行数说明

source: source表行数

target: target表行数  

changed: target表改变的行数

deleted: target表缺少的行数  

added: target表多出的行数  

diff: changed+deleted+added，不一致的行数

recheck_passed：复核通过的行数，recheck_passed=-1不开启复核，复核预期：diff行数等于recheck_passed行数

TABLE:[affiliate_regular_ranking]  RESULT:[YES]  INFO:[ source:37  target:37  changed:0  deleted:0  added:0  diff:0  recheck_passed:0 ]



案例：

ewsuk 海外和国内双写，数据不一致，把国内存在海外缺失的数据补全到海外库，值不一致以海外为准，国内库计划下线，国内库值不一致或缺失的数据不需要修复。

国内：172.30.32.6:33065

海外：10.165.89.144:33065

1、核对数据
./check_data_mysql.py -S 172.30.32.6:33065 -T 10.165.89.144:33065 -P 8 -d ewsuk -t allocation_command,allocation_task_container_command,allocation_way_pre_sale,allocation_way_pre_sale_detail,goods,goods_bill_stock,goods_bill_stock_change_log,goods_category_relate,goods_label_relate,goods_rack_stock,goods_rack_stock_change_log,goods_rack_stock_lock,goods_rack_stock_lock_log,goods_status_stock_change_log,goods_stock_code,goods_stock_code_his,goods_volume,goods_volume_change_log,goods_weight,goods_weight_change_log,in_storage_request,in_storage_request_box,in_storage_request_box_item,ods_post_type_country_limit_meter,ods_product,outbound_order_item_command,profit_loss_item_change_log,separate_box_item_log,stock_tacking_instruction,stock_tacking_instruction_log,stock_tacking_log,storage_rack_usage,storage_rack_usage_change_log,sync_location_record,tripartite_return_registration 

2、核对不一致的数据生成修复SQL
./check_data_mysql.py -S 172.30.32.6:33065 -T 10.165.89.144:33065 -d ewsuk -o


3、手动执行修复SQL
ls -l 172.30.32.6_33065/ewsuk/*.sql

xxx.add.sql 代表目标端多出的数据

xxx.chg.sql 代表目标端不一致的数据

xxx.del.sql 代表目标端缺失的数据

-T参数指定的是目标端：10.165.89.144:33065（海外）

只需要修复xxx.del.sql ，国内库值不一致或缺失的数据不需要修复。

mysql -udba_rw -p'abc123' -A -h 10.165.89.144 -P33065 ewsuk <172.30.32.6_33065/ewsuk/goods_stock_code.del.sql



附：check_data_mysql.py参数参考

usage: check_data_mysql.py [-h] [-v] [-S SOURCE] [-T TARGET] [-u USER]
                           [-p PASSWORD] [-d DATABASE] [-t TABLES] [-w WHERE]
                           [-k KEYS] [-E EXECLUDE_TABLES]
                           [-e EXECLUDE_COLUMNS] [-c CHARSET]
                           [-r RECHECK_TIMES] [-R MAX_RECHECK_ROWS]
                           [-P PARALLEL] [-l {debug,info,warning}]

optional arguments:
  -h, --help            show this help message and exit
  -v, --version         查看版本
  -S SOURCE, --source SOURCE
                        要核对的source实例IP和端口, 如: 10.0.0.201:3306
  -T TARGET, --target TARGET
                        要核对的target实例IP和端口, 如: 10.0.0.202:3306
  -u USER, --user USER  用户名
  -p PASSWORD, --password PASSWORD
                        密码
  -d DATABASE, --database DATABASE
                        要核对的数据库
  -t TABLES, --tables TABLES
                        要核对的表(不指定为全库核对，多个表使用英文逗号分隔)，如：users,orders
  -w WHERE, --where WHERE
                        核对的where条件，默认全表核对
  -k KEYS, --keys KEYS  核对使用的主键(多个列使用英文逗号分隔)，如：id,user_id
  -E EXECLUDE_TABLES, --execlude-tables EXECLUDE_TABLES
                        排除不需要核对的表(多个表使用英文逗号分隔)，如：tmp_users,tmp_orders
  -e EXECLUDE_COLUMNS, --execlude-columns EXECLUDE_COLUMNS
                        排除不需要核对的列(多个列使用英文逗号分隔)，如：sys_ctime,sys_version
  -c CHARSET, --charset CHARSET
                        指定字符集，默认:utf8
  -r RECHECK_TIMES, --recheck-times RECHECK_TIMES
                        复核次数，默认:0（不开启复核）
  -R MAX_RECHECK_ROWS, --max-recheck-rows MAX_RECHECK_ROWS
                        最大复核行数，首核不一致行数超过该数值，不进行复核，默认:100
  -P PARALLEL, --parallel PARALLEL
                        并行度（进行多表核对时生效），默认:2
  -l {debug,info,warning}, --log {debug,info,warning}
                        输出的日志等级：debug,info,warning
