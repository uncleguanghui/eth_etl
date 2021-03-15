# -*- coding: UTF-8 -*-
"""
ETL 参数解析，以及 Web3 初始化
@Time    : 2021/3/13 7:28 下午
@Author  : zhangguanghui
"""
import os
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--start', '-s', required=True, type=int, help='开始区块高度')
parser.add_argument('--end', '-e', required=True, type=int, help='结束区块高度')
parser.add_argument('--batch', '-b', required=True, type=int, help='输出到一个文件中的区块数')
parser.add_argument('--ipc', '-p', required=True, type=str, help='geth.ipc文件路径')
parser.add_argument('--output', '-o', required=True, type=str, help='输出结果目录')
parser.add_argument('--continue_', '-c', action='store_true', help='是否继续输出（在上一次结果的基础上）')
parser.add_argument('--waiting', '-w', action='store_true', help='当区块高度没有达到结束高度时，是否等待')
args = parser.parse_args()

assert 0 <= args.start <= args.end, '开始或结束区块高度异常'
assert 0 < args.batch <= 10000, '一个文件中的区块高度不在合理范围 (0, 10000]'
assert (args.end - args.start + 1) % args.batch == 0, '总区块数量要能够被 batch 整除'
assert os.path.exists(args.ipc), 'geth.ipc 路径 ' + args.ipc + ' 不存在'
os.makedirs(args.output, exist_ok=True)
