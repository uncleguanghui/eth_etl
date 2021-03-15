# -*- coding: UTF-8 -*-
"""
导出 receipts 和 contracts 数据
@Time    : 2021/3/15 11:37 上午
@Author  : zhangguanghui
"""
import os
import time
import logging
import pandas as pd
from util import Web3, get_function_sighashes, is_erc20_contract, is_erc721_contract

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def receipt_to_dict(receipt):
    return {
        'transaction_hash': receipt.get('transactionHash').hex(),
        'transaction_index': receipt.get('transactionIndex'),
        'block_hash': receipt.get('blockNumber'),
        'block_number': receipt.get('blockNumber'),
        'cumulative_gas_used': receipt.get('cumulativeGasUsed'),  #
        'gas_used': receipt.get('gasUsed'),  #
        'contract_address': receipt.get('contractAddress'),  # ?
        'root': receipt.get('root'),  # ?
        'status': receipt.get('status'),
    }


def contract_to_dict(web3, contract_addr, block_number):
    bytecode = web3.eth.get_code(contract_addr).hex()
    function_sighashes = get_function_sighashes(bytecode)
    return {
        'address': contract_addr,
        'bytecode': bytecode,
        'function_sighashes': ','.join(function_sighashes),
        'is_erc20': is_erc20_contract(function_sighashes),
        'is_erc721': is_erc721_contract(function_sighashes),
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

        # 递归创建 receipts 目录
        dir_receipts = os.path.join(output, 'receipts', f'start={start_block:08d}/end={end_block:08d}')
        os.makedirs(dir_receipts, exist_ok=True)
        path_receipts = os.path.join(dir_receipts, f'receipts_{start_block:08d}_{end_block:08d}.csv')
        # 递归创建 contracts 目录
        dir_contracts = os.path.join(output, 'contracts', f'start={start_block:08d}/end={end_block:08d}')
        os.makedirs(dir_contracts, exist_ok=True)
        path_contracts = os.path.join(dir_contracts, f'contracts_{start_block:08d}_{end_block:08d}.csv')
        # 如果设置 continue_=True，且两个文件都处理过了，则不重复处理
        if continue_ and os.path.exists(path_receipts) and os.path.exists(path_contracts):
            logger.info(f'区块 {start_block}~{end_block} 已处理，跳过')
            continue

        # 获取 transactions ID 和 receipts
        blocks = [web3.eth.get_block(i) for i in range(start_block, end_block + 1)]
        transaction_ids = sum([[i.hex() for i in i.transactions] for i in blocks], [])
        receipts = [web3.eth.getTransactionReceipt(i) for i in transaction_ids]

        # 保存 receipts
        data_receipts = [receipt_to_dict(i) for i in receipts]
        df_receipts = pd.DataFrame(data_receipts)
        df_receipts.to_csv(path_receipts, index=False, encoding='utf-8-sig')
        logger.info(f'receipts -> {path_receipts}')

        # 保存 contracts
        data_contracts = [contract_to_dict(web3, i.contractAddress, i.blockNumber)
                          for i in receipts if i.get('contractAddress')]
        df_contracts = pd.DataFrame(data_contracts)
        df_contracts.to_csv(path_contracts, index=False, encoding='utf-8-sig')
        logger.info(f'contracts -> {path_contracts}')


if __name__ == '__main__':
    from init import args

    w3 = Web3(args.ipc)
    export(w3, args.start, args.end, args.batch, args.output, args.continue_, args.waiting)
