# -*- coding: UTF-8 -*-
"""
ETL 参数解析，以及 Web3 初始化
@Time    : 2021/3/13 7:28 下午
@Author  : zhangguanghui
"""
import logging
import argparse
import configparser
from pathlib import Path

__all__ = ['conf']

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def combine_config(c, a):
    # 合并配置项
    return {
        # geth.ipc
        'ipc': c['geth']['ipc'],
        # input
        'start': a.start,
        'end': a.end,
        # output
        'output': c['output']['path'],
        'format': c['output']['format'],
        'compression': None if c['output']['compression'] == 'None' else c['output']['compression'],
        'batch': a.batch or c.getint('output', 'batch'),
        **{key: value for key, value in c['output'].items() if key.startswith('table_name')},
        # action
        'continue': c.getboolean('action', 'continue'),
    }


def print_config(c):
    # 打印配置
    for k, v in c.items():
        print(f'[config] {k:>30s} = {v}')


# 读取 config.ini
path_config = Path(__file__).parent / 'config.ini'
assert path_config.exists(), '配置文件 config.ini 不存在'
config = configparser.ConfigParser()
config.read(path_config)

# 读取入参
parser = argparse.ArgumentParser()
parser.add_argument('--start', '-s', required=True, type=int, help='开始区块高度')
parser.add_argument('--end', '-e', required=True, type=int, help='结束区块高度')
parser.add_argument('--batch', '-b', type=int, help='输出到一个文件中的区块数')
args = parser.parse_args()

# 合并配置项
conf = combine_config(config, args)
print_config(conf)

# 检查 ipc
path_ipc = conf['ipc']
assert Path(path_ipc).name == 'geth.ipc', 'config.ini 中的 geth.ipc 文件名错误'
assert Path(path_ipc).exists(), f'config.ini 中的 geth.ipc 文件不存在：{path_ipc}'
# 检查 output
dir_output = Path(conf['output'])
dir_output = dir_output if dir_output.is_absolute() else (Path(__file__).parent / dir_output)
dir_output.mkdir(exist_ok=True)
assert conf['format'] in ['csv', 'parquet'], f'不支持自定义格式 {conf["format"]}，仅支持 csv 或 parquet'
valid_comps = {'snappy', 'gzip', 'brotli', None}
assert conf['format'] != 'parquet' or conf['compression'] in valid_comps, f'parquet 格式下，压缩方式仅支持 {valid_comps}'
# 检查区块高度
assert 0 <= conf['start'] <= conf['end'], '开始或结束区块高度异常'
assert 0 < conf['batch'] <= 10000, '一个文件中的区块高度不在合理范围 (0, 10000]'
assert (conf['end'] - conf['start'] + 1) % conf['batch'] == 0, '总区块数量要能够被 batch 整除'
