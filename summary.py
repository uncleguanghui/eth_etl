"""
统计目前的进展
@Time    : 2021/3/18 10:26 上午
@Author  : zhangguanghui
"""
import web3
import configparser
from pathlib import Path

# 读取并检查配置
path_config = Path(__file__).parent / 'config.ini'
assert path_config.exists(), '配置文件 config.ini 不存在'
config = configparser.ConfigParser()
config.read(path_config)
path_ipc = config['geth']['ipc']
assert Path(path_ipc).name == 'geth.ipc', 'geth.ipc 文件名错误'
assert Path(path_ipc).exist(), f'geth.ipc 文件不存在：{path_ipc}'

# 连接 web3
w3 = web3.Web3(web3.Web3.IPCProvider(path_ipc, timeout=10))
assert w3.isConnected()
print(f'当前最新区块高度：{w3.eth.block_number}')


# DFS 获取最新区块高度
def get_max_block_height(path: Path, table_name):
    if not path.exists():
        return
    if path.is_file():
        if path.suffix == '.csv' and path.name.startswith(table_name):
            return int(path.stem.split('_')[-1])
    elif path.is_dir():
        max_height = 0
        for p in path.iterdir():
            height = get_max_block_height(p, table_name)
            if height:
                max_height = max(max_height, height)
        return max_height


# 输出目录的绝对路径
dir_output = Path(config['output']['dir'])
dir_output = dir_output if dir_output.is_absolute() else (Path(__file__).parent / dir_output)
# 遍历
for key in config['output'].keys():
    if key.startswith('table_name'):
        t_name = config['output'][key]
        path_data = dir_output / t_name
        print(f'{t_name} 已处理到：{get_max_block_height(path_data, t_name)}')
