# -*- coding: UTF-8 -*-
"""
导出 tokens 数据
@Time    : 2021/3/15 11:53 上午
@Author  : zhangguanghui
"""
import os
import time
import logging
import pandas as pd
from util import Web3, get_first_result, ERC20_ABI, TOPIC_TRANSFER, get_logs

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def token_to_dict(web3, token_addr, block_number=None):
    contract = web3.eth.contract(token_addr, abi=ERC20_ABI)
    return {
        'address': token_addr,
        'symbol': get_first_result(contract.functions.symbol(), contract.functions.SYMBOL()),
        'name': get_first_result(contract.functions.name(), contract.functions.NAME()),
        'decimals': get_first_result(contract.functions.decimals(), contract.functions.DECIMALS()),
        'total_supply': get_first_result(contract.functions.totalSupply()),
        'block_number': block_number
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

        # 递归创建 tokens 目录
        dir_tokens = os.path.join(output, 'tokens', f'start={start_block:08d}/end={end_block:08d}')
        os.makedirs(dir_tokens, exist_ok=True)
        path_tokens = os.path.join(dir_tokens, f'tokens_{start_block:08d}_{end_block:08d}.csv')
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
        df_tokens = pd.DataFrame(data_tokens)
        df_tokens.to_csv(path_tokens, index=False, encoding='utf-8-sig')
        logger.info(f'tokens -> {path_tokens}')


if __name__ == '__main__':
    from init import args

    w3 = Web3(args.ipc)
    export(w3, args.start, args.end, args.batch, args.output, args.continue_, args.waiting)
