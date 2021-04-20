# -*- coding: UTF-8 -*-
"""
导出 tokens 数据
@Time    : 2021/3/15 11:53 上午
@Author  : zhangguanghui
"""
import os
import logging
from util import Web3, get_first_result, ERC20_ABI, TOPIC_TRANSFER, get_logs, to_normalized_address, export_data, \
    wait_until_reach, get_path

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def token_to_dict(web3, token_addr, block_number=None):
    contract = web3.eth.contract(token_addr, abi=ERC20_ABI)
    return {
        'address': to_normalized_address(token_addr),
        'symbol': get_first_result(contract.functions.symbol(), contract.functions.SYMBOL()),
        'name': get_first_result(contract.functions.name(), contract.functions.NAME()),
        'decimals': get_first_result(contract.functions.decimals(), contract.functions.DECIMALS()),
        'total_supply': str(get_first_result(contract.functions.totalSupply())),
        'block_number': block_number
    }


def export(web3, config: dict):
    start, end = config['start'], config['end']
    continue_ = config['continue']
    output, fmt, compression, batch = config['output'], config['format'], config['compression'], config['batch']

    for start_block in range(start, end, batch):
        end_block = start_block + batch - 1
        path_tokens = get_path(output, 'tokens', start_block, end_block, fmt)

        # 等待到达最新区块高度
        wait_until_reach(web3, start_block, batch)

        # 如果设置 continue_=True，且文件都处理过了，则不重复处理
        if continue_ and os.path.exists(path_tokens):
            logger.info(f'区块 {start_block}~{end_block} 已处理，跳过')
            continue

        # 获取 token_transfers 和 token 地址
        batch_size = max(batch // 100, 10)  # 获取日志的批大小
        logs = sum([get_logs(web3, i, i + batch_size - 1) for i in range(start_block, end_block + 1, batch_size)], [])
        transfers = [i for i in logs if i.topics and i.topics[0].hex().startswith(TOPIC_TRANSFER)]
        token_addrs = set([i.address for i in transfers])

        # 保存 tokens
        data_tokens = [token_to_dict(web3, token_addr, block_number=None) for token_addr in token_addrs]
        export_data('tokens', data_tokens, path_tokens, fmt, compression)


if __name__ == '__main__':
    from check_config import conf

    w3 = Web3(conf['ipc'])
    export(w3, conf)
