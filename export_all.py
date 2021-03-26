"""
多线程输出所有数据
@Time    : 2021/3/26 10:25 上午
@Author  : zhangguanghui
"""
import os
import time
import random
from util import Web3
from check_config import conf
from multiprocessing import Process
from export_token import export as token_export
from export_block_tx import export as block_tx_export
from export_log_trans import export as log_trans_export
from export_receipt_contract import export as receipt_contract_export


def func(target, *args):
    # 记录进程 ID
    time.sleep(random.random())
    with open('pid.txt', 'a') as f:
        f.write('=.' * 20 + '\n')
        f.write(f'{target.__name__}({", ".join([str(i) for i in args])})\n')
        f.write(f'pid: {os.getpid()}\n')
    # 运行函数
    target(*args)


if __name__ == '__main__':
    w3 = Web3(conf['ipc'])

    ps = []
    for function in [
        token_export,
        block_tx_export,
        log_trans_export,
        receipt_contract_export
    ]:
        p = Process(target=func, args=(function, w3, conf))
        p.start()
        ps.append(p)

    for p in ps:
        p.join()
