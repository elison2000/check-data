#!/bin/python3
# -*- encoding: utf-8 -*-
"""
####################################################################################################
#  Name        :  check_data_mongo.py
#  Author      :  Elison
#  Email       :  Ly99@qq.com
#  Description :  检查mongo主备数据是否一致
#  Updates     :
#      Version     When            What
#      --------    -----------     -----------------------------------------------------------------
#      v2.0        2022-03-26      使用diff替换sqlite3
#      v2.1        2022-06-15      支持不同库名对比
#      v2.2        2022-07-11      增加count模式，核对总行数
####################################################################################################
"""

import os, sys
import time
import argparse
import threading
from multiprocessing import Process, Pool
import logging
import struct, binascii
import bson
import urllib
import pymongo


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
    parser.add_argument("-S", "--source", type=str, help="要核对的source实例IP和端口, 如: 10.0.0.201:27017")
    parser.add_argument("-T", "--target", type=str, help="要核对的target实例IP和端口, 如: 10.0.0.202:27017")
    parser.add_argument("-u", "--user", type=str, default='dba_ro', help="用户名")
    parser.add_argument("-p", "--password", dest="password", default='abc123*', type=str, help="密码")
    parser.add_argument("-d", "--database", type=str, help="要核对的数据库，如：orderdb或orderdb:new_orderdb(可以使用冒号分别指定源端和目标端db名)")
    parser.add_argument("-t", "--tables", type=str, help="要核对的表(不指定为全库核对，多个表使用英文逗号分隔)，如：users,orders")
    parser.add_argument("-w", "--where", type=str, help="核对的where条件，默认全表核对")
    parser.add_argument("-E", "--execlude-tables", type=str, help="排除不需要核对的表(多个表使用英文逗号分隔)，如：tmp_users,tmp_orders")
    parser.add_argument("-r", "--recheck-times", type=int, default=0, help="复核次数，默认：0（不开启复核）")
    parser.add_argument("-R", "--max-recheck-rows", type=int, default=100, help="最大复核行数，首核不一致行数超过该数值，不进行复核，默认：100")
    parser.add_argument("-P", "--parallel", dest="parallel", type=int, default=2, help="并行度（进行多表核对时生效），默认：2")
    parser.add_argument("-m", "--mode", type=str, default='slow', choices=['slow', 'count'],
                        help="核对模式：slow-核对明细，速度慢；count-核对总行数，速度快")
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
        print("Usage: ./check_data_mongo.py -S 10.0.0.201:27017 -T 10.0.0.202:27017 -d test")
        sys.exit()

    try:
        target_host, target_port = args.target.split(':')
        target_host.split('.')[3]
        target_port = int(target_port)
        args.target_host = target_host
        args.target_port = target_port
    except Exception as e:
        print("无效参数：-T")
        print("Usage: ./check_data_mongo.py -S 10.0.0.201:27017 -T 10.0.0.202:27017 -d test")
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
        print("Usage: ./check_data_mongo.py -S 10.0.0.201:27017 -T 10.0.0.202:27017 -d test")
        sys.exit()

    if args.tables:
        args.tables = args.tables.split(',')
    else:
        args.tables = []

    if not args.where:
        args.where = {}

    if args.execlude_tables:
        args.execlude_tables = args.execlude_tables.split(',')
    else:
        args.execlude_tables = []

    return args


def crcsum_bson_v1(raw_data):
    '解码bson并求校验和'
    rows = []
    dataset = bson.decode_all(raw_data)
    for i in dataset:
        object_id = str(i['_id'])
        crcsum = binascii.crc32(bson.encode(i))
        # row = (object_id, crcsum)
        row_text = '{}###{}'.format(object_id, crcsum)
        rows.append(row_text)
    return rows


def crcsum_bson_v2(raw_data):
    """
        解码bson并求校验和
        bosn格式：前4个字节代码数据字节长度，后面代表数据，只适用于_id为ObjectId的数据
    """
    offset = 0
    rows = []
    data_len = len(raw_data)
    while True:
        row_head = raw_data[offset:offset + 4]  # 前4个字节代码数据字节长度
        row_len = struct.unpack('i', row_head)[0]
        row_body = raw_data[offset:offset + row_len]
        object_id = binascii.b2a_hex(row_body[9:21]).decode('utf8')
        crcsum = binascii.crc32(row_body)
        row_text = '{}###{}'.format(object_id, crcsum)
        rows.append(row_text)
        offset = offset + row_len
        if offset >= data_len:
            break
    return rows


def crcsum_bson_v3(raw_data):
    """
        解码bson并求校验和（批量校验，效率更高）
        bosn格式：前4个字节代码数据字节长度，后面代表数据，只适用于_id为ObjectId的数据
    """
    object_id = binascii.b2a_hex(raw_data[9:21]).decode('utf8')
    crcsum = binascii.crc32(raw_data)
    row_text = '{}###{}'.format(object_id, crcsum)
    return row_text


class ObjectId:
    "ObjectId函数"

    @classmethod
    def get_objectid(cls, string=None):
        "生成object id"
        return bson.ObjectId(string)

    @classmethod
    def objectid_to_timestamp(cls, string):
        "object id转换为时间"
        ts = int(string[:8], 16)
        return cls.timestamp_to_str(ts)

    @classmethod
    def time_to_objectid(cls, string):
        "时间转换为object id"
        ts = cls.str_to_timestamp(string)
        id = 2 ** 64 * ts
        return '{:x}'.format(id)

    @classmethod
    def timestamp_to_str(cls, ts):
        "时间戳转换为可读时间"
        t = time.localtime(ts)
        return time.strftime('%Y-%m-%d %H:%M:%S', t)

    @classmethod
    def str_to_timestamp(cls, timestr):
        "可读时间转换为时间戳"
        t = time.strptime(timestr, '%Y-%m-%d %H:%M:%S')
        return int(time.mktime(t))


def get_begin_oid(interval):
    """
        获取增量核对起点的objectid
        interval: 3d | 2h | 10m
    """
    unit = interval[-1]  # 提取时间单位
    val = int(interval[0:-1])
    ts = int(time.time())

    if unit == 'd':
        ts = ts // (86400) * 86400 - val * 86400  # 取整
    elif unit == 'h':
        ts = ts // (3600) * 3600 - val * 3600  # 取整
    elif unit == 'm':
        ts = ts // (60) * 60 - val * 60  # 取整
    else:
        print('无效的时间单位')
        sys.exit(-1)

    id_str = '{:x}'.format(2 ** 64 * ts)
    return bson.ObjectId(id_str)


class Mongo:
    "mongo接口"

    def __init__(self, host, port, user, password, auth_db='admin'):
        password = urllib.parse.quote(password)  # 转义密码里的@特殊字符
        url = 'mongodb://{3}:{4}@{0}:{1}/{2}'.format(host, port, auth_db, user, password)
        self.conn = pymongo.MongoClient(url, serverSelectionTimeoutMS=1000)

    def query(self, db_name, tb_name, filter={}, size=1000):
        "返回dict数据"
        tb = self.conn[db_name][tb_name]
        cursor = tb.find(filter, batch_size=size)
        for row in cursor:
            yield row

    def query_batches(self, db_name, tb_name, filter={}, size=1000):
        "返回raw数据,适用于返回大量数据"
        tb = self.conn[db_name][tb_name]
        cursor = tb.find_raw_batches(filter, batch_size=size)
        for raw_data in cursor:  # rows=bson.decode_all(raw_data)
            if raw_data:
                yield raw_data
            else:
                break

    def count(self, db_name, tb_name, filter={}):
        "返回数据行数"
        tb = self.conn[db_name][tb_name]
        res = tb.count(filter)
        return res

    def get_collection_names(self, db_name):
        "获取集合名"
        table_list = self.conn[db_name].list_collection_names()
        # table_list.sort()
        return table_list

    def close(self):
        self.conn.close()


def get_table_names(conf, db_name):
    "获取集合名"
    mg = Mongo(**conf)
    table_list = mg.get_collection_names(db_name)
    mg.close()
    return table_list


def get_tables():
    "获取核对的表"
    global CONFIG
    source_conf = {'host': CONFIG.source_host, 'port': CONFIG.source_port, 'auth_db': 'admin',
                   'user': CONFIG.user, 'password': CONFIG.password}
    target_conf = {'host': CONFIG.target_host, 'port': CONFIG.target_port, 'auth_db': 'admin',
                   'user': CONFIG.user, 'password': CONFIG.password}
    source_tables = get_table_names(source_conf, CONFIG.source_db)
    target_tables = get_table_names(target_conf, CONFIG.target_db)

    if CONFIG.execlude_tables:
        logging.info("排除不需要核对的表：{}".format(CONFIG.execlude_tables))
        source_tables = [tb for tb in source_tables if tb not in CONFIG.execlude_tables]
        target_tables = [tb for tb in target_tables if tb not in CONFIG.execlude_tables]

    source_del_tb = [i for i in target_tables if i not in source_tables]
    target_del_tb = [i for i in source_tables if i not in target_tables]
    check_tb_list = [i for i in target_tables if i in source_tables]
    if len(source_tables) == len(target_tables) and len(source_del_tb) == 0 and len(target_del_tb) == 0:
        logging.info(
            "DB:[{}]    RESULT:[YES]    TBCNT:[ source:{}  target:{} common:{}]".format(CONFIG.database,
                                                                                        len(source_tables),
                                                                                        len(target_tables),
                                                                                        len(check_tb_list)))
    else:
        logging.info(
            "DB:[{}]    RESULT:[NO]    TBCNT:[ source:{}  target:{} common:{}]".format(CONFIG.database,
                                                                                       len(source_tables),
                                                                                       len(target_tables),
                                                                                       len(check_tb_list)))
        if source_del_tb:
            logging.info("source端不存在的表：{}".format(source_del_tb))
        if target_del_tb:
            logging.info("target端不存在的表：{}".format(target_del_tb))
    return check_tb_list


class Table:
    def __init__(self, table_name):
        global CONFIG
        self.source_db = CONFIG.source_db
        self.target_db = CONFIG.target_db
        self.name = table_name
        self.table_name = table_name
        self.source_conf = {'host': CONFIG.source_host, 'port': CONFIG.source_port, 'auth_db': 'admin',
                            'user': CONFIG.user, 'password': CONFIG.password}
        self.target_conf = {'host': CONFIG.target_host, 'port': CONFIG.target_port, 'auth_db': 'admin',
                            'user': CONFIG.user, 'password': CONFIG.password}
        self.source_datafile = self.name + '.source'
        self.target_datafile = self.name + '.target'
        self.where = CONFIG.where
        self.recheck_times = CONFIG.recheck_times
        self.max_recheck_rows = CONFIG.max_recheck_rows
        self.source_rowcnt = 0
        self.target_rowcnt = 0
        self.chg_rowcnt = 0
        self.del_rowcnt = 0
        self.add_rowcnt = 0
        self.diff_rowcnt = 0
        self.recheck_pass_rowcnt = -1

    def init(self):
        "初始化实例"
        pass


def get_source_data(obj):
    "获取source数据"
    rowcnt = 0
    begin_ts = time.time()
    logging.info('开始下载 [{}]'.format(obj.name))
    f = open(obj.source_datafile, 'w', encoding='utf8')
    source_conn = Mongo(**obj.source_conf)
    i_rows = source_conn.query_batches(obj.source_db, obj.name, filter=obj.where, size=1000)
    for raw_data in i_rows:
        rows = crcsum_bson_v2(raw_data)
        for row in rows:
            rowcnt += 1
            f.write(row + '\n')
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
    target_conn = Mongo(**obj.target_conf)
    i_rows = target_conn.query_batches(obj.target_db, obj.name, filter=obj.where, size=1000)
    for raw_data in i_rows:
        rows = crcsum_bson_v2(raw_data)
        for row in rows:
            rowcnt += 1
            f.write(row + '\n')
    target_conn.close()
    end_ts = time.time()
    seconds = int(end_ts - begin_ts)
    logging.info('下载完成 [{0}]  耗时{1}s,写入 {2} rows'.format(obj.name, seconds, rowcnt))


def download_data(obj):
    "下载数据"
    # 创建子进程
    source_downloader = Process(name='source_downloader.' + obj.name, target=get_source_data, args=(obj,))  # 创建子进程
    source_downloader.daemon = True  # 设置为守护
    target_downloader = Process(name='target_downloader.' + obj.name, target=get_target_data, args=(obj,))  # 创建子进程
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

    cmd0 = """sort {0} >{0}.sort""".format(obj.source_datafile)
    cmd1 = """sort {0} >{0}.sort""".format(obj.target_datafile)
    cmd2 = """diff -y --suppress-common-lines  {0}  {1} >{2}.diff""".format(obj.source_datafile + '.sort',
                                                                            obj.target_datafile + '.sort',
                                                                            obj.name)
    cmd3 = """grep "|" {0}.diff|awk '{{print $1}}' >{0}.chg""".format(obj.name)
    cmd4 = """grep "<" {0}.diff|awk '{{print $1}}' >{0}.del""".format(obj.name)
    cmd5 = """grep ">" {0}.diff|awk '{{print $2}}' >{0}.add""".format(obj.name)

    os.system(cmd0)
    os.system(cmd1)
    os.system(cmd2)
    os.system(cmd3)
    os.system(cmd4)
    os.system(cmd5)

    obj.source_rowcnt = int(os.popen('cat {0}| wc -l'.format(obj.source_datafile)).readlines()[0])
    obj.target_rowcnt = int(os.popen('cat {0}| wc -l'.format(obj.target_datafile)).readlines()[0])
    obj.chg_rowcnt = int(os.popen('cat {0}.chg| wc -l'.format(obj.name)).readlines()[0])
    obj.del_rowcnt = int(os.popen('cat {0}.del| wc -l'.format(obj.name)).readlines()[0])
    obj.add_rowcnt = int(os.popen('cat {0}.add| wc -l'.format(obj.name)).readlines()[0])
    obj.diff_rowcnt = obj.chg_rowcnt + obj.del_rowcnt + obj.add_rowcnt

    if obj.source_rowcnt == obj.target_rowcnt and obj.diff_rowcnt == 0:
        os.remove(obj.source_datafile)
        os.remove(obj.target_datafile)
        os.remove(obj.source_datafile + '.sort')
        os.remove(obj.target_datafile + '.sort')
        os.remove(obj.name + '.diff')
        os.remove(obj.name + '.chg')
        os.remove(obj.name + '.del')
        os.remove(obj.name + '.add')


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
    source_conn = Mongo(**obj.source_conf)
    target_conn = Mongo(**obj.target_conf)
    for pk_id_text in recheck_pk_list:
        oid = ObjectId.get_objectid(pk_id_text)
        filter = {"_id": oid}
        logging.info("TABLE:[{}] filter: {}".format(obj.name, filter))
        row_is_same = False
        for i in range(obj.recheck_times):
            i_source_rows = source_conn.query(obj.source_db, obj.table_name, filter=filter, size=10)
            source_rows = [i for i in i_source_rows]
            source_rows_crcsum = binascii.crc32(str(source_rows).encode('utf-8'))
            i_target_rows = target_conn.query(obj.target_db, obj.table_name, filter=filter, size=10)
            target_rows = [i for i in i_target_rows]
            target_rows_crcsum = binascii.crc32(str(target_rows).encode('utf-8'))
            # 对比
            if len(source_rows) == 0 and len(target_rows) == 0:
                obj.recheck_pass_rowcnt += 1
                row_is_same = True
                logging.info("TABLE:[{}] filter:{} 复核通过".format(obj.name, filter))
                break

            elif len(source_rows) == 1 and len(target_rows) == 1 and source_rows_crcsum == target_rows_crcsum:
                obj.recheck_pass_rowcnt += 1
                row_is_same = True
                logging.info("TABLE:[{}] filter:{} 复核通过".format(obj.name, filter, i + 1))
                break
            else:
                logging.info("TABLE:[{}] filter:{} 第{}次复核不通过".format(obj.name, filter, i + 1))
                time.sleep(1)
        if row_is_same is False:
            logging.info("TABLE:[{}] source_rows: {}".format(obj.name, source_rows))
            logging.info("TABLE:[{}] target_rows: {}".format(obj.name, target_rows))
            break

    # 关闭连接
    source_conn.close()
    target_conn.close()


def check_table(table_name, is_parallel=False):
    "检查表明细"
    global CONFIG
    ts = time.time()
    logging.info("开始核对 [{}]".format(table_name))
    try:
        obj = Table(table_name)
        obj.init()

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

    except Exception as e:
        logging.error("TABLE:[{}]    RESULT:[NO]    INFO:[ {} ]".format(table_name, str(e)), exc_info=True)
    seconds = int(time.time() - ts)
    logging.info("核对结束 [{}] ,耗时{}s".format(table_name, seconds))


def check_table_count(table_name):
    "检查表行数"
    global CONFIG
    ts = time.time()
    logging.info("开始核对 [{}]".format(table_name))
    try:
        obj = Table(table_name)
        obj.init()
        source_conn = Mongo(**obj.source_conf)
        obj.source_rowcnt = source_conn.count(obj.source_db, obj.name, filter=obj.where)
        target_conn = Mongo(**obj.target_conf)
        obj.target_rowcnt = target_conn.count(obj.target_db, obj.name, filter=obj.where)
        obj.diff_rowcnt = obj.target_rowcnt - obj.source_rowcnt

        # 打印核对结果
        if obj.diff_rowcnt == 0:
            logging.info(
                "TABLE:[{}]  RESULT:[YES]  INFO:[ source:{}  target:{}  diff:{} ]".format(obj.name, obj.source_rowcnt,
                                                                                          obj.target_rowcnt,
                                                                                          obj.diff_rowcnt))
        else:
            logging.info(
                "TABLE:[{}]  RESULT:[NO]  INFO:[ source:{}  target:{}  diff:{} ]".format(obj.name, obj.source_rowcnt,
                                                                                         obj.target_rowcnt,
                                                                                         obj.diff_rowcnt))

    except Exception as e:
        logging.error("TABLE:[{}]    RESULT:[NO]    INFO:[ {} ]".format(table_name, str(e)), exc_info=True)
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

    if len(tables) > 1 and CONFIG.mode != "count":
        # 创建进程池
        logging.info("创建进程池，并行度：{}".format(CONFIG.parallel))
        pool = Pool(processes=CONFIG.parallel)
        for tb in tables:
            pool.apply_async(check_table, (tb, True))
        pool.close()
        pool.join()
    else:
        for tb in tables:
            if CONFIG.mode == "count":
                check_table_count(tb)
            else:
                check_table(tb)
