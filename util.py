# -*- coding: UTF-8 -*-
"""
工具
@Time    : 2021/3/15 11:44 上午
@Author  : zhangguanghui
"""
import os
import web3
import json
import time
import logging
from web3.middleware import geth_poa_middleware
from web3.exceptions import BadFunctionCallOutput
from ethereum_dasm.evmdasm import EvmCode, Contract
from eth_utils import function_signature_to_4byte_selector

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

TOPIC_TRANSFER = '0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'


class Web3:
    def __init__(self, ipc):
        self.w3 = None
        self.ipc = ipc
        assert os.path.exists(self.ipc), 'geth.ipc 路径 ' + self.ipc + ' 不存在'
        self.update()

    def update(self):
        success = False
        max_retry_count = 10
        retry_count = 0
        while retry_count < max_retry_count:
            try:
                self.w3 = web3.Web3(web3.Web3.IPCProvider(self.ipc, timeout=60))
                self.w3.middleware_onion.inject(geth_poa_middleware, layer=0)  # 注入 poa 兼容中间件到最内层
                assert self.w3.isConnected()
                success = True
                break
            except (Exception, AssertionError) as err:
                retry_count += 1
                logger.warning(f'Web3 初始化失败 {retry_count} 次：{str(err)}')
                time.sleep(60)

        if not success:
            raise ValueError(f'Web3 初始化尝试次数超过最大重试次数 {max_retry_count}')

    @property
    def eth(self):
        if not self.w3.isConnected():
            self.update()
        return self.w3.eth


def to_normalized_address(address):
    if address is None or not isinstance(address, str):
        return address
    return address.lower()


def word_to_address(param):
    if param is None:
        return None
    elif len(param) >= 40:
        return to_normalized_address('0x' + param[-40:])
    else:
        return to_normalized_address(param)


def get_logs(w3, start, end):
    try_max_cnt = 10
    try_index = 0
    wait_seconds = 1  # 等待时长
    error_info = ''
    while try_index < try_max_cnt:
        try:
            return w3.eth.filter({
                'fromBlock': start,
                'toBlock': end,
            }).get_all_entries()
        except Exception as err:
            try_index += 1
            error_info = str(err)
            time.sleep(wait_seconds)
    raise ValueError(f'在 {start}-{end} 获取 logs 失败，超过最大尝试次数 {try_max_cnt}。' + error_info)


def call_contract_function2(func, ignore_errors, default_value=None):
    try:
        result = func.call()
        return result
    except Exception as ex:
        if type(ex) in ignore_errors:
            print('An exception occurred in function {} of contract {}. '.format(func.fn_name, func.address)
                  + 'This exception can be safely ignored.')
            return default_value
        else:
            raise ex


def call_contract_function1(func):
    # BadFunctionCallOutput exception happens if the token doesn't implement a particular function
    # or was self-destructed
    # OverflowError exception happens if the return type of the function doesn't match the expected type
    return call_contract_function2(
        func=func,
        ignore_errors=(BadFunctionCallOutput, OverflowError, ValueError),
        default_value=None)


def get_first_result(*funcs):
    for func in funcs:
        result = call_contract_function1(func)
        if result is not None:
            return result
    return None


def get_function_sighash(signature):
    return '0x' + function_signature_to_4byte_selector(signature).hex()


class ContractWrapper:
    def __init__(self, sighashes):
        self.sighashes = sighashes

    def implements(self, function_signature):
        sighash = get_function_sighash(function_signature)
        return sighash in self.sighashes

    def implements_any_of(self, *function_signatures):
        return any(self.implements(function_signature) for function_signature in function_signatures)


def is_erc20_contract(function_sighashes):
    c = ContractWrapper(function_sighashes)
    return c.implements('totalSupply()') and \
           c.implements('balanceOf(address)') and \
           c.implements('transfer(address,uint256)') and \
           c.implements('transferFrom(address,address,uint256)') and \
           c.implements('approve(address,uint256)') and \
           c.implements('allowance(address,address)')


def is_erc721_contract(function_sighashes):
    c = ContractWrapper(function_sighashes)
    return c.implements('balanceOf(address)') and \
           c.implements('ownerOf(uint256)') and \
           c.implements_any_of('transfer(address,uint256)', 'transferFrom(address,address,uint256)') and \
           c.implements('approve(address,uint256)')


def clean_bytecode(bytecode):
    if bytecode is None or bytecode == '0x':
        return None
    elif bytecode.startswith('0x'):
        return bytecode[2:]
    else:
        return bytecode


def get_function_sighashes(bytecode):
    bytecode = clean_bytecode(bytecode)
    if bytecode is not None:
        evm_code = EvmCode(contract=Contract(bytecode=bytecode), static_analysis=False, dynamic_analysis=False)
        evm_code.disassemble(bytecode)
        basic_blocks = evm_code.basicblocks
        if basic_blocks and len(basic_blocks) > 0:
            init_block = basic_blocks[0]
            instructions = init_block.instructions
            push4_instructions = [inst for inst in instructions if inst.name == 'PUSH4']
            return sorted(list(set('0x' + inst.operand for inst in push4_instructions)))
        else:
            return []
    else:
        return []


ERC20_ABI = json.loads('''
[
    {
        "constant": true,
        "inputs": [],
        "name": "name",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": false,
        "inputs": [
            {
                "name": "_spender",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "approve",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "totalSupply",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": false,
        "inputs": [
            {
                "name": "_from",
                "type": "address"
            },
            {
                "name": "_to",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "transferFrom",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "decimals",
        "outputs": [
            {
                "name": "",
                "type": "uint8"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            }
        ],
        "name": "balanceOf",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "symbol",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": false,
        "inputs": [
            {
                "name": "_to",
                "type": "address"
            },
            {
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "transfer",
        "outputs": [
            {
                "name": "",
                "type": "bool"
            }
        ],
        "payable": false,
        "stateMutability": "nonpayable",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [
            {
                "name": "_owner",
                "type": "address"
            },
            {
                "name": "_spender",
                "type": "address"
            }
        ],
        "name": "allowance",
        "outputs": [
            {
                "name": "",
                "type": "uint256"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "anonymous": false,
        "inputs": [
            {
                "indexed": true,
                "name": "_from",
                "type": "address"
            },
            {
                "indexed": true,
                "name": "_to",
                "type": "address"
            },
            {
                "indexed": false,
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "Transfer",
        "type": "event"
    },
    {
        "anonymous": false,
        "inputs": [
            {
                "indexed": true,
                "name": "_owner",
                "type": "address"
            },
            {
                "indexed": true,
                "name": "_spender",
                "type": "address"
            },
            {
                "indexed": false,
                "name": "_value",
                "type": "uint256"
            }
        ],
        "name": "Approval",
        "type": "event"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "NAME",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "SYMBOL",
        "outputs": [
            {
                "name": "",
                "type": "string"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    },
    {
        "constant": true,
        "inputs": [],
        "name": "DECIMALS",
        "outputs": [
            {
                "name": "",
                "type": "uint8"
            }
        ],
        "payable": false,
        "stateMutability": "view",
        "type": "function"
    }
]
''')

if __name__ == '__main__':
    # import web3

    # w3 = web3.Web3(web3.Web3.IPCProvider('/opt/bsc/build/bin/node/geth.ipc', timeout=60))
    # w3.middleware_onion.inject(geth_poa_middleware, layer=0)  # 注入 poa 兼容中间件到最内层
    # w3.isConnected()
    pass
