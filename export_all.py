"""
导出所有数据
@Time    : 2021/3/15 3:57 下午
@Author  : zhangguanghui
"""
from util import Web3
from export_token import export as token_export
from export_block_tx import export as block_tx_export
from export_log_trans import export as log_trans_export
from export_receipt_contract import export as receipt_contract_export

if __name__ == '__main__':
    from check_config import conf

    w3 = Web3(conf['ipc'])
    block_tx_export(w3, conf['start'], conf['end'], conf['batch'], conf['output'], conf['continue'], conf['waiting'])
    log_trans_export(w3, conf['start'], conf['end'], conf['batch'], conf['output'], conf['continue'], conf['waiting'])
    receipt_contract_export(w3, conf['start'], conf['end'], conf['batch'], conf['output'], conf['continue'],
                            conf['waiting'])
    token_export(w3, conf['start'], conf['end'], conf['batch'], conf['output'], conf['continue'], conf['waiting'])
