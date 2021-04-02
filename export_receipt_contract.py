# -*- coding: UTF-8 -*-
"""
导出 receipts 和 contracts 数据
@Time    : 2021/3/15 11:37 上午
@Author  : zhangguanghui
"""
import os
import logging
from util import Web3, get_function_sig_hashes, is_erc20_contract, is_erc721_contract, to_normalized_address, \
    export_data, get_receipt, wait_until_reach, get_path

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


def receipt_to_dict(receipt):
    return {
        'transaction_hash': receipt.get('transactionHash').hex(),
        'transaction_index': receipt.get('transactionIndex'),
        'block_hash': receipt.get('blockHash').hex(),
        'block_number': receipt.get('blockNumber'),
        'cumulative_gas_used': receipt.get('cumulativeGasUsed'),
        'gas_used': receipt.get('gasUsed'),
        'contract_address': to_normalized_address(receipt.get('contractAddress')),
        'root': receipt.get('root'),
        'status': receipt.get('status'),
    }


def contract_to_dict(web3, contract_addr, block_number):
    bytecode = web3.eth.get_code(contract_addr).hex()
    function_sig_hashes = get_function_sig_hashes(bytecode)
    return {
        'address': to_normalized_address(contract_addr),
        'bytecode': bytecode,
        'function_sighashes': ','.join(function_sig_hashes),
        'is_erc20': is_erc20_contract(function_sig_hashes),
        'is_erc721': is_erc721_contract(function_sig_hashes),
        'block_number': block_number
    }


def export(web3, config: dict):
    start, end = config['start'], config['end']
    continue_ = config['continue']
    output, fmt, compression, batch = config['output'], config['format'], config['compression'], config['batch']

    for start_block in range(start, end, batch):
        end_block = start_block + batch - 1
        path_receipts = get_path(output, 'receipts', start_block, end_block, fmt)
        path_contracts = get_path(output, 'contracts', start_block, end_block, fmt)

        # 等待到达最新区块高度
        wait_until_reach(web3, start_block, batch)

        # 如果设置 continue_=True，且两个文件都处理过了，则不重复处理
        if continue_ and os.path.exists(path_receipts) and os.path.exists(path_contracts):
            logger.info(f'区块 {start_block}~{end_block} 已处理，跳过')
            continue

        # 获取 transactions ID 和 receipts
        blocks = [web3.eth.get_block(i) for i in range(start_block, end_block + 1)]
        transaction_hashes = sum([[i.hex() for i in i.transactions] for i in blocks], [])
        receipts = [get_receipt(web3, i) for i in transaction_hashes]

        # 保存 receipts
        data_receipts = [receipt_to_dict(i) for i in receipts]
        export_data('receipts', data_receipts, path_receipts, fmt, compression)

        # 保存 contracts
        data_contracts = [contract_to_dict(web3, i.contractAddress, i.blockNumber)
                          for i in receipts if i.get('contractAddress')]
        export_data('contracts', data_contracts, path_contracts, fmt, compression)


if __name__ == '__main__':
    from check_config import conf

    w3 = Web3(conf['ipc'])
    export(w3, conf)
