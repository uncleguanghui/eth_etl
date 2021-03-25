# 以太坊 ETL 脚本

本脚本在 ethereum-etl 模块的基础上，做了以下改进：
1. 新增配置文件，提供了一些进阶参数：
    * 增加了 `continue` 参数，不再重复处理已经处理过的数据；
    * 增加了 `waiting` 参数，当最新区块高度没达到期望处理的区块高度（如99999999）时，会等待，直到处理完指定的区块；
2. 简化了目录结构，即取消了 start_block=xxxx/end_block=xxxx 这两级目录，所有数据文件放在一个目录下。若有分区需求则自建目录吧。
3. 简化模块，聚焦于 ETL 功能，去掉了很多关系不大的模块，便于后续的升级和维护。

##  快速开始

请确保 python 是 3.6+。

先重命名配置文件为 `config.ini`

```bash
mv config.ini.example config.ini
```

然后修改配置文件内容，一般来说，只需要改 `[geth]` 下的 ipc 即可。

```text
[geth]
# geth.ipc 绝对路径
ipc = /.../geth.ipc

[output]
# 输出文件的路径（绝对路径或相对路径，可以不用改）
path = output
# 7 张表的文件名（不用改）
table_name_blocks = blocks
table_name_logs = logs
table_name_tokens = tokens
table_name_token_transfers = token_transfers
table_name_transactions = transactions
table_name_contracts = contracts
table_name_receipts = receipts
# 每个文件包含的区块数（按服务器处理能力来改）
batch = 100

[action]
# 是否继续输出（在上一次结果的基础上，可以不用改）
continue = True
# 当区块高度没有达到结束高度时，是否等待（可以不用改）
waiting = True
```

然后，下载依赖：

```bash
pip install -r requirements.txt
```

然后，运行下面的 4 个命令，就可以输出结果到当前目录的 output 文件夹内

1、 输出 blocks 和 transactions

```bash
python export_block_tx.py -s 0 -e 99
```

解释一下参数：
* `-s` `--start`：开始区块高度
* `-e` `--end`：结束区块高度

运行完成后，会在当前目录看到 output 文件夹，在 output 文件夹下会有两个文件夹，分别是 blocks 和 transactions。

每个文件夹下面都有若干个 csv 文件，如 blocks_00000000_00000999.csv。

2、 输出 blocks 和 token_transactions

```bash
python export_log_trans.py -s 0 -e 99
```

3、 输出 receipts 和 contracts

```bash
python export_receipt_contract.py -s 0 -e 99
```

4、 输出 tokens

```bash
python export_token.py -s 0 -e 99
```

如果想要让脚本永远跑下去，最省心的命令就是：

```bash
python export_token.py -s 0 -e 9999999999
```