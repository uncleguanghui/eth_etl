# -*- coding: UTF-8 -*-
"""
导出 logs 和 token_transfers 数据
@Time    : 2021/3/15 11:00 上午
@Author  : zhangguanghui
"""
import os
import logging
from util import Web3, TOPIC_TRANSFER, get_logs, word_to_address, to_normalized_address, export_data, \
    wait_until_reach, get_path

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


def export(web3, config: dict):
    start, end = config['start'], config['end']
    continue_ = config['continue']
    output, fmt, compression, batch = config['output'], config['format'], config['compression'], config['batch']

    for start_block in range(start, end, batch):
        end_block = start_block + batch - 1
        path_logs = get_path(output, 'logs', start_block, end_block, fmt)
        path_transfers = get_path(output, 'token_transfers', start_block, end_block, fmt)

        # 等待到达最新区块高度
        wait_until_reach(web3, start_block, batch)

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
        export_data('logs', data_logs, path_logs, fmt, compression)

        # 保存 token_transfers
        data_transfers = [transfer_to_dict(i) for i in transfers]
        export_data('token_transfers', data_transfers, path_transfers, fmt, compression)


if __name__ == '__main__':
    from check_config import conf

    w3 = Web3(conf['ipc'])
    export(w3, conf)
