# -*- coding: UTF-8 -*-
"""
导出 logs 和 token_transfers 数据
@Time    : 2021/3/15 11:00 上午
@Author  : zhangguanghui
"""
import os
import time
import logging
import pandas as pd
from util import Web3, TOPIC_TRANSFER, get_logs, word_to_address, to_normalized_address

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def log_to_dict(log):
    return {
        'log_index': log.get('logIndex'),
        'transaction_hash': log.get('transactionHash').hex(),
        'transaction_index': log.get('transactionIndex'),
        'block_hash': log.get('blockHash').hex(),
        'block_number': log.get('blockNumber'),
        'address': to_normalized_address(log.get('address')),
        'data': log.get('data'),
        'topics': ','.join([i.hex() for i in log.get('topics')])
    }


def transfer_to_dict(transfer):
    return {
        'token_address': to_normalized_address(transfer.get('address')),
        'from_address': word_to_address(transfer.get('topics')[1].hex()) if len(transfer.get('topics')) > 1 else None,
        'to_address': word_to_address(transfer.get('topics')[2].hex()) if len(transfer.get('topics')) > 2 else None,
        'value': int(transfer.get('data'), 16) if transfer.get('data') != '0x' else 0,
        'transaction_hash': transfer.get('transactionHash').hex(),
        'log_index': transfer.get('logIndex'),
        'block_number': transfer.get('blockNumber'),
    }


def export(web3, start, end, batch, output, continue_=False, waiting=False):
    for start_block in range(start, end, batch):
        end_block = start_block + batch - 1

        # 等待到达最新区块高度
        current_block_index = web3.eth.blockNumber
        while current_block_index < end_block:
            if not waiting:
                raise StopIteration(f'待处理区块少于目标值 {batch}，停止处理')
            logger.info(f'当前区块 {current_block_index}，期望处理 {start_block}~{end_block}，等待 60 秒')
            time.sleep(60)
            current_block_index = web3.eth.blockNumber

        # 递归创建 logs 目录
        dir_logs = os.path.join(output, 'logs', f'start={start_block:08d}/end={end_block:08d}')
        os.makedirs(dir_logs, exist_ok=True)
        path_logs = os.path.join(dir_logs, f'logs_{start_block:08d}_{end_block:08d}.csv')
        # 递归创建 token_transfers 目录
        dir_transfers = os.path.join(output, 'token_transfers', f'start={start_block:08d}/end={end_block:08d}')
        os.makedirs(dir_transfers, exist_ok=True)
        path_transfers = os.path.join(dir_transfers, f'token_transfers_{start_block:08d}_{end_block:08d}.csv')
        # 如果设置 continue_=True，且两个文件都处理过了，则不重复处理
        if continue_ and os.path.exists(path_logs) and os.path.exists(path_transfers):
            logger.info(f'区块 {start_block}~{end_block} 已处理，跳过')
            continue

        # 获取 logs 和 token_transfers
        batch_size = max(batch // 100, 10)  # 获取日志的批大小
        logs = sum([get_logs(web3, i, i + batch_size - 1) for i in range(start_block, end_block + 1, batch_size)], [])
        transfers = [i for i in logs if i.topics and i.topics[0].hex().startswith(TOPIC_TRANSFER)]

        # 保存 logs
        data_logs = [log_to_dict(i) for i in logs]
        df_logs = pd.DataFrame(data_logs)
        df_logs.to_csv(path_logs, index=False, encoding='utf-8-sig')
        logger.info(f'logs -> {path_logs}')

        # 保存 token_transfers
        data_transfers = [transfer_to_dict(i) for i in transfers]
        df_transfers = pd.DataFrame(data_transfers)
        df_transfers.to_csv(path_transfers, index=False, encoding='utf-8-sig')
        logger.info(f'token_transfers -> {path_transfers}')


if __name__ == '__main__':
    from init import args

    w3 = Web3(args.ipc)
    export(w3, args.start, args.end, args.batch, args.output, args.continue_, args.waiting)
