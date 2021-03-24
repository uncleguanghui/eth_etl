# -*- coding: UTF-8 -*-
"""
导出 blocks 和 transactions 数据
@Time    : 2021/3/15 10:01 上午
@Author  : zhangguanghui
"""
import os
import time
import logging
import pandas as pd
from util import Web3, to_normalized_address

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def block_to_dict(block):
    return {
        'number': block.get('number'),
        'hash': block.get('hash').hex(),
        'parent_hash': block.get('parentHash').hex(),
        'nonce': block.get('nonce').hex(),
        'sha3_uncles': block.get('sha3Uncles').hex(),
        'logs_bloom': block.get('logsBloom').hex(),
        'transactions_root': block.get('transactionsRoot').hex(),
        'state_root': block.get('stateRoot').hex(),
        'receipts_root': block.get('receiptsRoot').hex(),
        'miner': block.get('miner'),
        'difficulty': block.get('difficulty'),
        'total_difficulty': block.get('totalDifficulty'),
        'size': block.get('size'),
        'extra_data': block.get('proofOfAuthorityData').hex(),
        'gas_limit': block.get('gasLimit'),
        'gas_used': block.get('gasUsed'),
        'timestamp': block.get('timestamp'),
        'transaction_count': len(block.get('transactions')),
    }


def tx_to_dict(transaction, block_timestamp):
    return {
        'hash': transaction.get('hash').hex(),
        'nonce': transaction.get('nonce'),
        'block_hash': transaction.get('blockHash').hex(),
        'block_number': transaction.get('blockNumber'),
        'transaction_index': transaction.get('transactionIndex'),
        'from_address': to_normalized_address(transaction.get('from')),
        'to_address': to_normalized_address(transaction.get('to')),
        'value': transaction.get('value'),
        'gas': transaction.get('gas'),
        'gas_price': transaction.get('gasPrice'),
        'input': transaction.get('input'),
        'block_timestamp': block_timestamp,
    }


def export(web3, start, end, batch, output, continue_=False, waiting=False):
    for start_block in range(start, end, batch):
        end_block = start_block + batch - 1

        # 等待到达最新区块高度
        current_block_index = web3.eth.blockNumber
        while current_block_index < end_block:
            if not waiting:
                raise StopIteration(f'待处理区块少于目标值 {batch}，停止处理')
            logger.info(f'最新区块 {current_block_index}，期望处理 {start_block}~{end_block}，等待 60 秒')
            time.sleep(60)
            current_block_index = web3.eth.blockNumber

        # 递归创建 blocks 目录
        dir_blocks = os.path.join(output, 'blocks', f'start_block={start_block:08d}/end_block={end_block:08d}')
        os.makedirs(dir_blocks, exist_ok=True)
        path_blocks = os.path.join(dir_blocks, f'blocks_{start_block:08d}_{end_block:08d}.csv')
        # 递归创建 transactions 目录
        dir_txs = os.path.join(output, 'transactions', f'start_block={start_block:08d}/end_block={end_block:08d}')
        os.makedirs(dir_txs, exist_ok=True)
        path_txs = os.path.join(dir_txs, f'transactions_{start_block:08d}_{end_block:08d}.csv')
        # 如果设置 continue_=True，且文件都处理过了，则不重复处理
        if continue_ and os.path.exists(path_blocks) and os.path.exists(path_txs):
            logger.info(f'区块 {start_block}~{end_block} 已处理，跳过')
            continue

        # 同时获取 blocks 和 transactions
        blocks = [web3.eth.get_block(i, True) for i in range(start_block, end_block + 1)]

        # 保存 blocks
        data_blocks = [block_to_dict(i) for i in blocks]
        df_blocks = pd.DataFrame(data_blocks)
        df_blocks.to_csv(path_blocks, index=False, encoding='utf-8-sig')
        logger.info(f'blocks -> {path_blocks}')

        # 保存 transactions
        data_txs = sum([[tx_to_dict(j, i.get('timestamp')) for j in i.transactions] for i in blocks], [])
        df_txs = pd.DataFrame(data_txs)
        df_txs.to_csv(path_txs, index=False, encoding='utf-8-sig')
        logger.info(f'transactions -> {path_txs}')


if __name__ == '__main__':
    from check_config import conf

    w3 = Web3(conf['ipc'])
    export(w3, conf['start'], conf['end'], conf['batch'], conf['output'], conf['continue'], conf['waiting'])
