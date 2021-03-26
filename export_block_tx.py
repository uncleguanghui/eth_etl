# -*- coding: UTF-8 -*-
"""
导出 blocks 和 transactions 数据
@Time    : 2021/3/15 10:01 上午
@Author  : zhangguanghui
"""
import os
import logging
from util import Web3, to_normalized_address, export_data, wait_until_reach, get_path

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


def export(web3, config: dict):
    start, end = config['start'], config['end']
    continue_ = config['continue']
    output, fmt, compression, batch = config['output'], config['format'], config['compression'], config['batch']

    for start_block in range(start, end, batch):
        end_block = start_block + batch - 1
        path_blocks = get_path(output, 'blocks', start_block, end_block)
        path_txs = get_path(output, 'transactions', start_block, end_block)

        # 等待到达最新区块高度
        wait_until_reach(web3, start_block, batch)

        # 如果设置 continue_=True，且文件都处理过了，则不重复处理
        if continue_ and os.path.exists(path_blocks) and os.path.exists(path_txs):
            logger.info(f'区块 {start_block}~{end_block} 已处理，跳过')
            continue

        # 同时获取 blocks 和 transactions
        blocks = [web3.eth.get_block(i, True) for i in range(start_block, end_block + 1)]

        # 保存 blocks
        data_blocks = [block_to_dict(i) for i in blocks]
        export_data('blocks', data_blocks, path_blocks, fmt, compression)

        # 保存 transactions
        data_txs = sum([[tx_to_dict(j, i.get('timestamp')) for j in i.transactions] for i in blocks], [])
        export_data('transactions', data_txs, path_txs, fmt, compression)


if __name__ == '__main__':
    from check_config import conf

    w3 = Web3(conf['ipc'])
    export(w3, conf)
