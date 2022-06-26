#!/bin/python3
# -*- encoding: utf-8 -*-
"""
####################################################################################################
#  Name        :  check_data_pgsql.py
#  Author      :  Elison
#  Email       :  Ly99@qq.com
#  Description :  检查postgres主备数据是否一致
#  Updates     :
#      Version     When            What
#      --------    -----------     -----------------------------------------------------------------
#      v1.0        2022-06-12
####################################################################################################
"""

import os, sys
import time
import logging
import argparse
import threading
from multiprocessing import Process, Pool
import binascii
import psycopg2


def set_log_level(level='info'):
    "设置日志等级"
    if level == 'debug':
        lv = logging.DEBUG
    else:
        lv = logging.INFO
    logging.basicConfig(level=lv, format='[%(asctime)s.%(msecs)d] [%(levelname)s] %(funcName)s: %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


def get_args():
    '获取参数'
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--version", action='store_true', help="查看版本")
    parser.add_argument("-S", "--source", type=str, help="要核对的source实例IP和端口, 如: 10.0.0.201:3306")
    parser.add_argument("-T", "--target", type=str, help="要核对的target实例IP和端口, 如: 10.0.0.202:3306")
    parser.add_argument("-u", "--user", type=str, default='dba_ro', help="用户名")
    parser.add_argument("-p", "--password", dest="password", default='abc123', type=str, help="密码")
    parser.add_argument("-d", "--database", type=str, help="要核对的数据库，如：user或users:ods_users(可以使用冒号分别指定源端和目标端db名)")
    parser.add_argument("-t", "--tables", type=str, help="要核对的表(不指定为全库核对，多个表使用英文逗号分隔)，如：users,orders")
    parser.add_argument("-w", "--where", type=str, help="核对的where条件，默认全表核对")
    parser.add_argument("-k", "--keys", type=str, help="核对使用的主键(多个列使用英文逗号分隔)，如：id,user_id")
    parser.add_argument("-E", "--execlude-tables", type=str, help="排除不需要核对的表(多个表使用英文逗号分隔)，如：tmp_users,tmp_orders")
    parser.add_argument("-e", "--execlude-columns", type=str, help="排除不需要核对的列(多个列使用英文逗号分隔)，如：sys_ctime,sys_version")
    parser.add_argument("-c", "--charset", type=str, default='utf8', help="指定字符集，默认：utf8")
    parser.add_argument("-r", "--recheck-times", type=int, default=0, help="复核次数，默认：0（不开启复核）")
    parser.add_argument("-R", "--max-recheck-rows", type=int, default=100, help="最大复核行数，首核不一致行数超过该数值，不进行复核，默认：100")
    parser.add_argument("-P", "--parallel", dest="parallel", type=int, default=2, help="并行度（进行多表核对时生效），默认：2")
    parser.add_argument("-o", "--output", action='store_true', help="生成修复SQL文件")
    parser.add_argument("-m", "--mode", type=str, default='slow', choices=['fast', 'slow'],
                        help="核对模式：fast-速度快，只能核对mysql的数据；slow-速度慢，支持mysql,doris,tidb等")
    parser.add_argument("-l", "--log", type=str, default='info', choices=['debug', 'info', 'warning'],
                        help="输出的日志等级：debug,info,warning")

    args = parser.parse_args()

    # 处理参数
    if args.version:
        print(__doc__)
        sys.exit()

    try:
        source_host, source_port = args.source.split(':')
        source_host.split('.')[3]
        source_port = int(source_port)
        args.source_host = source_host
        args.source_port = source_port
    except Exception as e:
        print("无效参数：-S")
        print("Usage: ./check_data_mysql.py -S 10.0.0.201:3306 -T 10.0.0.202:3306 -d test")
        sys.exit()

    try:
        target_host, target_port = args.target.split(':')
        target_host.split('.')[3]
        target_port = int(target_port)
        args.target_host = target_host
        args.target_port = target_port
    except Exception as e:
        print("无效参数：-T")
        print("Usage: ./check_data_mysql.py -S 10.0.0.201:3306 -T 10.0.0.202:3306 -d test")
        sys.exit()

    if args.database:
        db_list = args.database.split(':')
        if len(db_list) >= 2:
            args.source_db = db_list[0]
            args.target_db = db_list[1]
        else:
            args.source_db = db_list[0]
            args.target_db = db_list[0]
    else:
        print("无效参数：-d")
        print("Usage: ./check_data_pgsql.py -S 10.0.0.201:5432 -T 10.0.0.202:5432 -d test")
        sys.exit()

    if args.tables:
        args.tables = args.tables.split(',')
    else:
        args.tables = []

    if args.keys:
        args.keys = args.keys.split(',')
    else:
        args.keys = []

    if not args.where:
        args.where = None

    if args.execlude_tables:
        args.execlude_tables = args.execlude_tables.split(',')
    else:
        args.execlude_tables = []

    if args.execlude_columns:
        args.execlude_columns = args.execlude_columns.split(',')
    else:
        args.execlude_columns = []

    return args


def crcsum_rows(rows):
    '计算校验和'
    res = []
    for row in rows:
        crcsum = binascii.crc32(row[1].encode('utf-8'))
        row_text = '{}###{}'.format(row[0], crcsum)
        res.append(row_text)
    return res


class Pgsql:
    def __init__(self, conf):
        self.conn = psycopg2.connect(**conf)

    def query(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        res = cur.fetchall()
        return res

    def query_bigtb(self, sql, size=1000):
        cur = self.conn.cursor()
        cur.execute(sql)
        while True:
            rows = cur.fetchmany(size)
            if rows:
                yield rows
            else:
                break
        cur.close()

    def dml(self, sql):
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
        return 1

    def close(self):
        self.conn.close()


def pgsql_query(conf, sql):
    "获取数据"
    conn = Pgsql(conf)
    rows = conn.query(sql)
    conn.close()
    return rows


def get_tables():
    "获取表名"
    global CONFIG
    sql = """select * from pg_tables where schemaname not in ('pg_catalog','information_schema')"""
    source_conf = {'host': CONFIG.source_host, 'port': CONFIG.source_port, 'database': CONFIG.source_db,
                   'user': CONFIG.user, 'password': CONFIG.password}
    target_conf = {'host': CONFIG.target_host, 'port': CONFIG.target_port, 'database': CONFIG.target_db,
                   'user': CONFIG.user, 'password': CONFIG.password}
    res1 = pgsql_query(source_conf, sql)
    res2 = pgsql_query(target_conf, sql)
    source_tables = [i[0] + '.' + i[1] for i in res1]
    target_tables = [i[0] + '.' + i[1] for i in res2]

    if CONFIG.execlude_tables:
        logging.info("排除不需要核对的表：{}".format(CONFIG.execlude_tables))
        source_tables = [tb for tb in source_tables if tb not in CONFIG.execlude_tables]
        target_tables = [tb for tb in target_tables if tb not in CONFIG.execlude_tables]

    source_del_tb = [i for i in target_tables if i not in source_tables]
    target_del_tb = [i for i in source_tables if i not in target_tables]
    check_tb_list = [i for i in target_tables if i in source_tables]
    if len(source_tables) == len(target_tables) and len(source_del_tb) == 0 and len(target_del_tb) == 0:
        logging.info(
            "DB:[{}]    RESULT:[YES]    TBCNT:[ source:{}  target:{} common:{}]".format(CONFIG.source_db,
                                                                                        len(source_tables),
                                                                                        len(target_tables),
                                                                                        len(check_tb_list)))
    else:
        logging.info(
            "DB:[{}]    RESULT:[NO]    TBCNT:[ source:{}  target:{} common:{}]".format(CONFIG.target_db,
                                                                                       len(source_tables),
                                                                                       len(target_tables),
                                                                                       len(check_tb_list)))
        if source_del_tb:
            logging.info("source端不存在的表：{}".format(source_del_tb))
        if target_del_tb:
            logging.info("target端不存在的表：{}".format(target_del_tb))
    return check_tb_list


class Table:
    def __init__(self, name):
        global CONFIG
        self.mode = CONFIG.mode
        self.name = name
        self.schema_name = self.name.split('.')[0]
        self.table_name = self.name.split('.')[1]
        self.source_conf = {'host': CONFIG.source_host, 'port': CONFIG.source_port, 'database': CONFIG.source_db,
                            'user': CONFIG.user, 'password': CONFIG.password}
        self.target_conf = {'host': CONFIG.target_host, 'port': CONFIG.target_port, 'database': CONFIG.target_db,
                            'user': CONFIG.user, 'password': CONFIG.password}
        self.source_datafile = self.name + '.source'
        self.target_datafile = self.name + '.target'
        self.keys = CONFIG.keys
        self.where = CONFIG.where
        self.execlude_columns = CONFIG.execlude_columns
        self.primary_key_text = None
        self.recheck_times = CONFIG.recheck_times
        self.max_recheck_rows = CONFIG.max_recheck_rows
        self.source_rowcnt = 0
        self.target_rowcnt = 0
        self.chg_rowcnt = 0
        self.del_rowcnt = 0
        self.add_rowcnt = 0
        self.diff_rowcnt = 0
        self.recheck_pass_rowcnt = -1

    def __str__(self):
        return str(self.__dict__)

    def get_columns(self):
        "获取列信息"
        sql = "SELECT column_name from information_schema.columns WHERE table_schema='{}' and table_name = '{}'".format(
            self.schema_name, self.table_name)
        self.all_columns = pgsql_query(self.source_conf, sql)
        self.all_column_names = [i[0] for i in self.all_columns]

    def get_primary_key(self, keys=None):
        "获取主键"
        sql = """SELECT a.schemaname,
       a.relname,
       b.conname AS pk_name,
       c.attname AS colname
FROM pg_stat_user_tables a,
     pg_constraint b,
     pg_attribute c
WHERE a.relid=b.conrelid
  AND b.conrelid=c.attrelid
  AND c.attnum=any(b.conkey) and a.schemaname='{}' and a.relname = '{}'
        """.format(self.schema_name, self.table_name)
        if keys:
            self.primary_key = keys
        else:
            res = pgsql_query(self.source_conf, sql)
            self.primary_key = [i[3] for i in res]
        if self.primary_key:
            self.primary_key_quote = self.primary_key
            self.primary_key_text = ','.join(self.primary_key_quote)

    def get_data_sql(self, execlude_columns=None, where=None):
        "获取下载数据SQL"
        if execlude_columns:
            self.check_column_names = [i for i in self.all_column_names if i not in execlude_columns]  # 排除不需要核对的列
            logging.info('不需要核对的列：{0}'.format(execlude_columns))
        else:
            self.check_column_names = [i for i in self.all_column_names]
        self.check_column_names_quote = ['{0}'.format(i) for i in self.check_column_names]  # 列名添加`号
        columns_text = ','.join(self.check_column_names_quote)

        if self.mode == 'slow':
            if where:
                self.get_data_sqltext = """select concat_ws(',',{0}) pk,concat_ws('|',{1}) as data from {2} where {3} order by {0}""".format(
                    self.primary_key_text, columns_text, self.name, where)
            else:
                self.get_data_sqltext = """select concat_ws(',',{0}) pk,concat_ws('|',{1}) as data from {2} order by {0}""".format(
                    self.primary_key_text, columns_text, self.name)
        else:
            if where:
                self.get_data_sqltext = """select concat(concat_ws(',',{0}),'###',crc32(concat_ws('|',{1}))) as crcsum from {2} where {3} order by {0}""".format(
                    self.primary_key_text, columns_text, self.name, where)
            else:
                self.get_data_sqltext = """select concat(concat_ws(',',{0}),'###',crc32(concat_ws('|',{1}))) as crcsum from {2} order by {0}""".format(
                    self.primary_key_text, columns_text, self.name)

        logging.info('primary_key_text：{0}'.format(self.primary_key_text))
        logging.info('get_data_sqltext：{0}'.format(self.get_data_sqltext))

    def init(self):
        "初始化"
        self.get_columns()
        self.get_primary_key(self.keys)
        if self.primary_key_text:
            self.get_data_sql(self.execlude_columns, self.where)


def get_source_data(obj):
    "获取source数据"

    rowcnt = 0
    begin_ts = time.time()
    logging.info('开始下载 [{}]'.format(obj.name))
    f = open(obj.source_datafile, 'w', encoding='utf8')
    source_conn = Pgsql(obj.source_conf)
    i_rows = source_conn.query_bigtb(obj.get_data_sqltext)
    for rows in i_rows:
        for row in rows:
            rowcnt += 1
            f.write(row[0] + '\n')
    source_conn.close()
    end_ts = time.time()
    seconds = int(end_ts - begin_ts)
    logging.info('下载完成 [{0}]  耗时{1}s,写入 {2} rows'.format(obj.name, seconds, rowcnt))


def get_source_data_slow(obj):
    "获取source数据"

    rowcnt = 0
    begin_ts = time.time()
    logging.info('开始下载 [{}]'.format(obj.name))
    f = open(obj.source_datafile, 'w', encoding='utf8')
    source_conn = Pgsql(obj.source_conf)
    i_rows = source_conn.query_bigtb(obj.get_data_sqltext)
    for rows in i_rows:
        crc32_results = crcsum_rows(rows)
        for row_text in crc32_results:
            rowcnt += 1
            f.write(row_text + '\n')
    source_conn.close()
    end_ts = time.time()
    seconds = int(end_ts - begin_ts)
    logging.info('下载完成 [{0}]  耗时{1}s,写入 {2} rows'.format(obj.name, seconds, rowcnt))


def get_target_data(obj):
    "获取target数据"

    rowcnt = 0
    begin_ts = time.time()
    logging.info('开始下载 [{}]'.format(obj.name))
    f = open(obj.target_datafile, 'w', encoding='utf8')
    target_conn = Pgsql(obj.target_conf)
    i_rows = target_conn.query_bigtb(obj.get_data_sqltext)
    for rows in i_rows:
        for row in rows:
            rowcnt += 1
            f.write(row[0] + '\n')
    target_conn.close()
    end_ts = time.time()
    seconds = int(end_ts - begin_ts)
    logging.info('下载完成 [{0}] 耗时{1}s,写入 {2} rows'.format(obj.name, seconds, rowcnt))


def get_target_data_slow(obj):
    "获取target数据"
    rowcnt = 0
    begin_ts = time.time()
    logging.info('开始下载 [{}]'.format(obj.name))
    f = open(obj.target_datafile, 'w', encoding='utf8')
    target_conn = Pgsql(obj.target_conf)
    i_rows = target_conn.query_bigtb(obj.get_data_sqltext)
    for rows in i_rows:
        crc32_results = crcsum_rows(rows)
        for row_text in crc32_results:
            rowcnt += 1
            f.write(row_text + '\n')
    target_conn.close()
    end_ts = time.time()
    seconds = int(end_ts - begin_ts)
    logging.info('下载完成 [{0}]  耗时{1}s,写入 {2} rows'.format(obj.name, seconds, rowcnt))


def download_data(obj):
    "下载数据"
    if obj.mode == 'slow':
        logging.debug('开启slow模式')
        source_download = get_source_data_slow
        target_download = get_target_data_slow
    else:
        source_download = get_source_data
        target_download = get_target_data

    # 创建子进程
    source_downloader = Process(name='source_downloader.' + obj.name, target=source_download, args=(obj,))  # 创建子进程
    source_downloader.daemon = True  # 设置为守护
    target_downloader = Process(name='target_downloader.' + obj.name, target=target_download, args=(obj,))  # 创建子进程
    target_downloader.daemon = True  # 设置为守护

    # 启动子进程
    source_downloader.start()  # 启动进程
    logging.info('[{0}] 进程已启动'.format(source_downloader.name))
    target_downloader.start()  # 启动进程
    logging.info('[{0}] 进程已启动'.format(target_downloader.name))

    # 等待完成
    source_downloader.join()
    target_downloader.join()


def download_data_multithread(obj):
    "下载数据(多线程版)"
    # 创建线程
    source_downloader = threading.Thread(name='source_downloader.' + obj.name, target=get_source_data,
                                         args=(obj,))  # 创建线程
    source_downloader.setDaemon(True)  # 设置为守护线程
    target_downloader = threading.Thread(name='target_downloader.' + obj.name, target=get_target_data,
                                         args=(obj,))  # 创建线程
    target_downloader.setDaemon(True)  # 设置为守护线程

    # 启动线程
    source_downloader.start()
    logging.info('[{0}] 线程已启动'.format(source_downloader.name))
    target_downloader.start()  # 启动线程
    logging.info('[{0}] 线程已启动'.format(target_downloader.name))

    # 等待完成
    source_downloader.join()
    target_downloader.join()


def compare_data(obj):
    "比较数据"
    cmd0 = """diff -y --suppress-common-lines  {0}  {1} >{2}.diff""".format(obj.source_datafile, obj.target_datafile,
                                                                            obj.name)
    cmd1 = """grep "|" {0}.diff|awk '{{print $1}}' >{0}.chg""".format(obj.name)
    cmd2 = """grep "<" {0}.diff|awk '{{print $1}}' >{0}.del""".format(obj.name)
    cmd3 = """grep ">" {0}.diff|awk '{{print $2}}' >{0}.add""".format(obj.name)
    os.system(cmd0)
    os.system(cmd1)
    os.system(cmd2)
    os.system(cmd3)

    obj.source_rowcnt = int(os.popen('cat {0}| wc -l'.format(obj.source_datafile)).readlines()[0])
    obj.target_rowcnt = int(os.popen('cat {0}| wc -l'.format(obj.target_datafile)).readlines()[0])
    obj.chg_rowcnt = int(os.popen('cat {0}.chg| wc -l'.format(obj.name)).readlines()[0])
    obj.del_rowcnt = int(os.popen('cat {0}.del| wc -l'.format(obj.name)).readlines()[0])
    obj.add_rowcnt = int(os.popen('cat {0}.add| wc -l'.format(obj.name)).readlines()[0])
    obj.diff_rowcnt = obj.chg_rowcnt + obj.del_rowcnt + obj.add_rowcnt

    if obj.source_rowcnt == obj.target_rowcnt and obj.diff_rowcnt == 0:
        os.remove(obj.source_datafile)
        os.remove(obj.target_datafile)
        os.remove(obj.name + '.diff')
        os.remove(obj.name + '.chg')
        os.remove(obj.name + '.del')
        os.remove(obj.name + '.add')


def export_repaire_sql(obj):
    "导出修复SQL"
    select_clause = """select * from {} where """.format(obj.name)

    source_conn = Mysql(obj.source_conf)

    # 生成target不一致的数据的update语句
    chg_id_list = []
    with open(obj.name + '.chg', 'r') as f:
        chg_id_list = [i.split('###')[0] for i in f.readlines()]
    if chg_id_list:
        filename = obj.name + '.chg.sql'
        for pk_id_text in chg_id_list:
            pk_ids = pk_id_text.split(',')
            where_text_list = ["""{}='{}'""".format(obj.primary_key[i], pk_ids[i]) for i in
                               range(len(obj.primary_key))]
            where_clause = ' and '.join(where_text_list)
            export_cmd = """mysqldump -h{} -P{} -u{} -p'{}' {} {} --single-transaction --no-create-info --compact --replace --default-character-set={} --where="{}" | grep "REPLACE INTO" >>{}""".format(
                obj.source_conf['host'], obj.source_conf['port'], obj.source_conf['user'], obj.source_conf['password'],
                obj.source_conf['database'], obj.name, obj.source_conf['charset'], where_clause, filename)
            os.system(export_cmd)

    # 生成target缺少的数据的insert语句
    del_id_list = []
    with open(obj.name + '.del', 'r') as f:
        del_id_list = [i.split('###')[0] for i in f.readlines()]
    if del_id_list:
        filename = obj.name + '.del.sql'
        for pk_id_text in del_id_list:
            pk_ids = pk_id_text.split(',')
            where_text_list = ["""{}='{}'""".format(obj.primary_key[i], pk_ids[i]) for i in
                               range(len(obj.primary_key))]
            where_clause = ' and '.join(where_text_list)
            export_cmd = """mysqldump -h{} -P{} -u{} -p'{}' {} {} --single-transaction --no-create-info --compact --replace --default-character-set={} --where="{}" | grep "REPLACE INTO" >>{}""".format(
                obj.source_conf['host'], obj.source_conf['port'], obj.source_conf['user'], obj.source_conf['password'],
                obj.source_conf['database'], obj.name, obj.source_conf['charset'], where_clause, filename)
            # print(export_cmd)
            os.system(export_cmd)

    # 生成target多出的数据的delete语句
    add_id_list = []
    with open(obj.name + '.add', 'r') as f:
        add_id_list = [i.split('###')[0] for i in f.readlines()]
    if add_id_list:
        delete_f = open(obj.name + '.add.sql', 'w')
        for pk_id_text in add_id_list:
            pk_ids = pk_id_text.split(',')
            where_text_list = ["""{}='{}'""".format(obj.primary_key_quote[i], pk_ids[i]) for i in
                               range(len(obj.primary_key_quote))]
            where_clause = ' and '.join(where_text_list)
            delete_text = 'delete from {} where {};\n'.format(obj.name, where_clause)
            delete_f.write(delete_text)
        delete_f.close()

    # 关闭连接
    source_conn.close()


def re_compare_data(obj):
    "复核"
    obj.recheck_pass_rowcnt = 0
    diff_text = []
    with open(obj.name + '.chg', 'r') as f1:
        diff_text += f1.readlines()
    with open(obj.name + '.del', 'r') as f2:
        diff_text += f2.readlines()
    with open(obj.name + '.add', 'r') as f3:
        diff_text += f3.readlines()
    recheck_pk_list = [i.split('###')[0] for i in diff_text]

    select_clause = """select concat_ws('|',{}) as row_text from {} where """.format(
        ','.join(obj.check_column_names_quote), obj.name)
    source_conn = Pgsql(obj.source_conf)
    target_conn = Pgsql(obj.target_conf)
    for pk_id_text in recheck_pk_list:
        pk_ids = pk_id_text.split(',')
        where_text_list = ["""{}='{}'""".format(obj.primary_key_quote[i], pk_ids[i]) for i in
                           range(len(obj.primary_key_quote))]
        where_clause = ' and '.join(where_text_list)
        sql = select_clause + where_clause
        logging.info("TABLE:[{}] where:{}".format(obj.name, where_clause))
        row_is_same = False
        for i in range(obj.recheck_times):
            source_rows = source_conn.query(sql)
            target_rows = target_conn.query(sql)
            # 对比
            if len(source_rows) == 0 and len(target_rows) == 0:
                obj.recheck_pass_rowcnt += 1
                row_is_same = True
                logging.info("TABLE:[{}] where:{} 复核通过".format(obj.name, where_clause))
                break
            elif len(source_rows) == 1 and len(target_rows) == 1 and source_rows[0][0] == target_rows[0][0]:
                obj.recheck_pass_rowcnt += 1
                row_is_same = True
                logging.info("TABLE:[{}] where:{} 复核通过".format(obj.name, where_clause))
                break
            else:
                logging.info("TABLE:[{}] where:{} 第{}次复核不通过".format(obj.name, where_clause,i + 1))
                time.sleep(1)
        if row_is_same is False:
            logging.info("TABLE:[{}] source_rows: {}".format(obj.name, source_rows))
            logging.info("TABLE:[{}] target_rows: {}".format(obj.name, target_rows))
            break

    # 关闭连接
    source_conn.close()
    target_conn.close()


def check_table(table_name, is_parallel=False):
    "主程序"
    global CONFIG
    ts = time.time()
    logging.info("开始核对 [{}]".format(table_name))
    try:
        obj = Table(table_name)
        obj.init()
        if obj.primary_key:
            if is_parallel:
                download_data_multithread(obj)
            else:
                download_data(obj)
            compare_data(obj)
            if obj.recheck_times > 0 and obj.diff_rowcnt > 0:
                if obj.diff_rowcnt <= obj.max_recheck_rows:
                    re_compare_data(obj)  # 复核
                else:
                    logging.info("TABLE:[{}]  [diff_rowcnt:{}]大于最大复核行数，跳过复核".format(obj.name, obj.diff_rowcnt))

            # 打印核对结果
            if obj.diff_rowcnt == 0:
                logging.info(
                    "TABLE:[{}]  RESULT:[YES]  INFO:[ source:{}  target:{}  changed:{}  deleted:{}  added:{}  diff:{}  recheck_passed:{} ]".format(
                        obj.name, obj.source_rowcnt, obj.target_rowcnt, obj.chg_rowcnt, obj.del_rowcnt, obj.add_rowcnt,
                        obj.diff_rowcnt, obj.recheck_pass_rowcnt))
            elif obj.diff_rowcnt > 0 and obj.diff_rowcnt == obj.recheck_pass_rowcnt:
                logging.info(
                    "TABLE:[{}]  RESULT:[YES]  INFO:[ source:{}  target:{}  changed:{}  deleted:{}  added:{}  diff:{}  recheck_passed:{} ]".format(
                        obj.name, obj.source_rowcnt, obj.target_rowcnt, obj.chg_rowcnt, obj.del_rowcnt, obj.add_rowcnt,
                        obj.diff_rowcnt, obj.recheck_pass_rowcnt))
            else:
                logging.info(
                    "TABLE:[{}]  RESULT:[NO]  INFO:[ source:{}  target:{}  changed:{}  deleted:{}  added:{}  diff:{}  recheck_passed:{} ]".format(
                        obj.name, obj.source_rowcnt, obj.target_rowcnt, obj.chg_rowcnt, obj.del_rowcnt, obj.add_rowcnt,
                        obj.diff_rowcnt, obj.recheck_pass_rowcnt))

        else:
            logging.info("TABLE:[{}]    RESULT:[NO]    INFO:[ 没有主键 ]".format(table_name))
    except Exception as e:
        logging.error("TABLE:[{}]    RESULT:[NO]    INFO:[ {} ]".format(table_name, str(e)), exc_info=True)
        print(obj)
    seconds = int(time.time() - ts)
    logging.info("核对结束 [{}] ,耗时{}s".format(table_name, seconds))


# main
if __name__ == "__main__":

    # 初始化参数
    CONFIG = get_args()
    set_log_level(CONFIG.log)

    logging.info("{}".format(' '.join(sys.argv)))

    # 设置工作路径
    dirname, filename = (os.path.split(os.path.realpath(__file__)))
    data_dir = '{}/{}_{}/{}'.format(dirname, CONFIG.source_host, CONFIG.source_port, CONFIG.source_db)
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    os.chdir(data_dir)

    # 获取要核对的表名
    if CONFIG.tables:
        tables = CONFIG.tables
    else:
        tables = get_tables()

    # 导出修复数据的sql文件
    if CONFIG.output:
        logging.info("导出修复数据的sql文件")
        for table_name in tables:
            if os.path.exists(table_name + '.diff'):
                obj = Table(table_name)
                obj.init()
                export_repaire_sql(obj)
        logging.info("导出修复数据的sql文件完成")
    # 核对数据
    elif len(tables) > 1:
        # 创建进程池
        logging.info("创建进程池，并行度：{}".format(CONFIG.parallel))
        pool = Pool(processes=CONFIG.parallel)
        for tb in tables:
            pool.apply_async(check_table, (tb, True))
        pool.close()
        pool.join()
    else:
        for tb in tables:
            check_table(tb)
