import json
import logging
import os
import sqlite3
import time
from datetime import datetime
from decimal import Decimal

from logging.handlers import RotatingFileHandler
from urllib import request
from dotenv import load_dotenv
from web3 import Web3

from web3.gas_strategies.rpc import rpc_gas_price_strategy
from web3.gas_strategies.time_based import slow_gas_price_strategy, medium_gas_price_strategy


def adapt_decimal(d):
    return str(d)


def convert_decimal(s):
    return Decimal(s)


def send_assets(arb_w3, to, amt):
    to = Web3.to_checksum_address(to)

    shuttle_address = arb_w3.to_checksum_address(config['addresses']["shuttle"])
    donut_address = arb_w3.to_checksum_address(config["contracts"]["arb1"]["donut"])

    # Load the contract ABI from a file
    with open('abi/erc20.json') as abi_file:
        donut_abi = json.load(abi_file)

    # eth (for gas)
    gas_balance = arb_w3.from_wei(arb_w3.eth.get_balance(shuttle_address), "ether")

    # donut balance
    donut_contract = arb_w3.eth.contract(address=donut_address, abi=donut_abi)
    donut_balance = donut_contract.functions.balanceOf(shuttle_address).call()

    if donut_balance < int(amt):
        logger.warning(f" shuttle does not have enough donuts for this tx. balance: [{arb_w3.from_wei(donut_balance, 'ether')}], needed: [{arb_w3.from_wei(int(amt), 'ether')}]")
        return

    if gas_balance < 0.0013:
        logger.warning(f" shuttle is out of gas. balance: [{gas_balance}]")
        return

    # all checks passed, we are good to drip
    # arb_w3.eth.set_gas_price_strategy(medium_gas_price_strategy)
    # arb_w3.eth.set_gas_price_strategy(slow_gas_price_strategy)
    # arb_w3.eth.set_gas_price_strategy(rpc_gas_price_strategy)

    latest_block = arb_w3.eth.get_block("latest")
    base_fee_per_gas = latest_block.baseFeePerGas
    # max_priority_fee_per_gas = arb_w3.to_wei(1, 'gwei') # Priority fee to include the transaction in the block

    # .1 gwei
    #max_priority_fee_per_gas = 100_000_000
    max_priority_fee_per_gas = 0

    # can put a multiplier on their for higher priority
    max_fee_per_gas = (1 * base_fee_per_gas) + max_priority_fee_per_gas

    transaction = donut_contract.functions.transfer(_to=to, _value=int(amt)).build_transaction({
        # 'chainId': arb_w3.eth.chain_id, # makes a call to infura and uses an API credit - just hardcode
        'chainId': 42161,
        'from': shuttle_address,
        'nonce': arb_w3.eth.get_transaction_count(shuttle_address),
        'gas': 800_000,
        'type': '0x2',
        # 'maxFeePerGas': max_fee_per_gas, # Maximum amount you’re willing to pay
        # 'maxPriorityFeePerGas': max_priority_fee_per_gas, # Priority fee to include the transaction in the blockNumber
        # 'maxFeePerGas': arb_w3.to_wei('10', 'gwei'),
        'maxFeePerGas': 100_000_000,
        # 'maxPriorityFeePerGas': arb_w3.to_wei('1', 'gwei')
        'maxPriorityFeePerGas': 100_000_000
    })

    #transaction['gas'] = arb_w3.eth.estimate_gas(transaction)
    # sign the transaction
    signed = arb_w3.eth.account.sign_transaction(transaction, os.getenv('SHUTTLE_PRIVATE_KEY'))

    # send the transaction
    tx_hash = arb_w3.eth.send_raw_transaction(signed.rawTransaction)
    receipt = arb_w3.eth.wait_for_transaction_receipt(tx_hash)

    human_readable_tx_hash = arb_w3.to_hex(tx_hash)
    logger.info(f" success! tx_hash: [{human_readable_tx_hash}]")

    return {
        "tx_hash" : human_readable_tx_hash,
        "blockNumber": receipt['blockNumber']
    }


def attempt_to_match_users(user_list):
    # attempt to name match any shuttles where we do not have user information
    with sqlite3.connect("arb1.db") as db:
        db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        cur = db.cursor()

        unmatched_sql = '''
        select * from shuttle where processed_at is null and from_user is null;
        '''
        cur.execute(unmatched_sql)
        unmatched = cur.fetchall()

    for u in unmatched:
        username = [x['username'] for x in user_list if x['address'].lower() == u['from_address'].lower()]

        if username:
            with sqlite3.connect("arb1.db") as db:
                db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
                cur = db.cursor()

                # attempt to name match any shuttles where we do not have user information
                match_sql = '''
                    update shuttle set from_user = ? where from_address = ?;
                '''
                cur.execute(match_sql, [username[0], u['from_address']])


def process_shuttles(user_list):
    attempt_to_match_users(user_list)

    arb_w3 = None
    for i in range(10):
        arb_w3 = Web3(Web3.HTTPProvider(os.getenv('INFURA_IO_API')))

        if not arb_w3.is_connected():
            logger.error(f"failed to connect to INFURA: attempt {i}")
            time.sleep(5)
        else:
            break

    if not arb_w3:
        logger.error("exhausted all INFURA attempts, aborting....")
        exit(4)

    # get shuttle records from db
    with sqlite3.connect("arb1.db") as db:
        db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        cur = db.cursor()
        sqlite3.register_adapter(Decimal, adapt_decimal)
        sqlite3.register_converter("Decimal", convert_decimal)

        shuttle_sql = '''
            select * from shuttle where processed_at is null and from_user is not null;
        '''

        cur.execute(shuttle_sql)
        shuttles = cur.fetchall()

    with open(os.path.normpath("abi/distribute.json"), 'r') as f:
        distribute_abi = json.load(f)

    with open('abi/erc20.json') as abi_file:
        donut_abi = json.load(abi_file)

    shuttle_address = arb_w3.to_checksum_address(config['addresses']["shuttle"])
    donut_address = arb_w3.to_checksum_address(config["contracts"]["arb1"]["donut"])
    distribute_address = arb_w3.to_checksum_address(config["contracts"]["arb1"]["distribute"])

    distribute_contract = arb_w3.eth.contract(address=distribute_address,abi=distribute_abi)
    donut_contract = arb_w3.eth.contract(address=donut_address, abi=donut_abi)

    donut_balance = donut_contract.functions.balanceOf(shuttle_address).call()
    gas_balance = arb_w3.from_wei(arb_w3.eth.get_balance(shuttle_address), "ether")

    if gas_balance < 0.001:
        logger.warning(f" shuttle is out of gas. balance: [{gas_balance}]")
        return

    distribute_tx_list = []

    for shuttle in shuttles:
        if donut_balance < int(shuttle["blockchain_amount"]):
            logger.warning(" shuttle does not have enough donuts for this tx.")
            continue

        donut_balance -= int(shuttle["blockchain_amount"])

        distribute_tx_list.append({
            "address": Web3.to_checksum_address(shuttle['from_address']),
            "amt": int(shuttle["blockchain_amount"]),
            "gno_tx_hash": shuttle["gno_tx_hash"]
        })

    # distribute_contract_data = distribute_contract.encodeABI("distribute", [
    #     [d['address'] for d in distribute_tx_list],
    #     [d['amt'] for d in distribute_tx_list],
    #     donut_address
    # ])

    transaction = distribute_contract.functions.distribute([d['address'] for d in distribute_tx_list], [d['amt'] for d in distribute_tx_list], donut_address).build_transaction({
        'from': shuttle_address,
        'nonce': arb_w3.eth.get_transaction_count(shuttle_address)
    })

    # sign the transaction
    signed = arb_w3.eth.account.sign_transaction(transaction, os.getenv('SHUTTLE_PRIVATE_KEY'))

    # send the transaction
    tx_hash = arb_w3.eth.send_raw_transaction(signed.rawTransaction)
    receipt = arb_w3.eth.wait_for_transaction_receipt(tx_hash)

    human_readable_tx_hash = arb_w3.to_hex(tx_hash)
    logger.info(f" success! tx_hash: [{human_readable_tx_hash}]")

    for distribute_tx in distribute_tx_list:
        with sqlite3.connect("arb1.db") as db:
            db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
            c = db.cursor()

            update_sql = '''
                update shuttle set processed_at = ?, arb_tx_hash = ? where gno_tx_hash = ?;
            '''
            c.execute(update_sql, [datetime.now(), human_readable_tx_hash, distribute_tx["gno_tx_hash"]])


if __name__ == '__main__':
    # load environment variables
    load_dotenv()

    # load config
    with open(os.path.normpath("config.json"), 'r') as f:
        config = json.load(f)

    # set up logging
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_name = os.path.basename(__file__)[:-3]
    logger = logging.getLogger(log_name)
    logger.setLevel(logging.INFO)

    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_path = os.path.join(base_dir, f"logs/{log_name}.log")
    file_handler = RotatingFileHandler(os.path.normpath(log_path), maxBytes=2500000, backupCount=4)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    logger.info("begin...")
    logger.info("create db (if needed) and find most recently processed block in the db")

    # setup db
    with sqlite3.connect("arb1.db") as db:
        sql_create = """
        CREATE TABLE IF NOT EXISTS
        `run_data` (
            `id` integer not null primary key autoincrement,
            `latest_block` BIGINT not null default 0,
            `created_at` datetime not null default CURRENT_TIMESTAMP
        );
        
        CREATE TABLE IF NOT EXISTS
        shuttle (
            id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
            from_user NVARCHAR2 COLLATE NOCASE,
            from_address NVARCHAR2 NOT NULL COLLATE NOCASE,
            blockchain_amount text NOT NULL,
            readable_amount DECIMAL(8, 7) NOT NULL,
            block_number INTEGER NOT NULL,
            gno_tx_hash NVARCHAR2 NOT NULL COLLATE NOCASE,
            arb_tx_hash NVARCHAR2 COLLATE NOCASE,
            processed_at DATETIME,
            created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        """

        db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        cur = db.cursor()
        cur.executescript(sql_create)

        sql_latest_block = """
            select max(latest_block) latest_block
            from run_data;
        """

        cur.execute(sql_latest_block)
        starting_block = cur.fetchone()['latest_block']

    if not starting_block:
        starting_block = 0
        # starting_block = 31762742
    else:
        starting_block = int(starting_block) + 1
        # starting_block = 31762742

    # get users file that will be used for any user <-> address lookups
    logger.info("grabbing users.json file...")
    users = json.load(request.urlopen(f"https://ethtrader.github.io/donut.distribution/users.json"))

    logger.info(f"querying gnosisscan with starting block: {starting_block}...")
    json_result = json.load(request.urlopen(
    f"https://api.gnosisscan.io/api?module=account&action=tokentx&address={config['multisig']['gnosis']}"
    f"&startblock={starting_block}&endblock=99999999&page=1&offset=10000&sort=asc"
    f"&apikey={os.getenv('GNOSIS_SCAN_IO_API_KEY')}"))

    logger.info(f"{len(json_result['result'])} transaction(s) found")

    if not len(json_result['result']):
        process_shuttles(users)
        logger.info("complete.")
        exit(0)

    # donut
    valid_tokens = [config["contracts"]["gnosis"]["donut"]]
    valid_tokens = [v.lower() for v in valid_tokens]

    # EthTraderCommunity
    ignored_addresses = ["0xf7927bf0230c7b0e82376ac944aeedc3ea8dfa25"]

    w3 = None

    for i in range(10):
        w3 = Web3(Web3.HTTPProvider(os.getenv('ANKR_API_PROVIDER')))

        if not w3.is_connected():
            logger.error(f"failed to connect to ANKR: attempt {i}")
            time.sleep(5)
        else:
            break

    if not w3:
        logger.error("exhausted all ANKR attempts, aborting....")
        exit(4)

    for tx in json_result["result"]:
        if tx["contractAddress"].lower() not in valid_tokens and tx["from"].lower():
            continue

        if tx["from"].lower() in ignored_addresses:
            continue

        if tx["to"].lower() != config['multisig']['gnosis'].lower():
            continue

        tx_hash = tx["hash"]
        w3_tx = w3.eth.get_transaction(tx_hash)
        inpt = w3_tx.input.hex()

        # ensure at least 100 confirmations
        if not int(tx['confirmations']) >= 100:
            logger.info(f"[tx_hash]: {tx_hash} does not have 100 confirmations yet ...")
            continue

        # not a transfer event
        if not inpt[:10] == "0xa9059cbb":
            logger.debug(f"not a transfer transaction [tx_hash]: {tx_hash}")
            continue

        # confirm the status of the tx hash = 1 (success)
        tx_receipt_url = f"https://api.gnosisscan.io/api?module=transaction&action=gettxreceiptstatus&txhash={tx_hash}&apikey={os.getenv('GNOSIS_SCAN_IO_API_KEY')}"
        tx_receipt = json.load(request.urlopen(tx_receipt_url))
        if not (tx_receipt["result"]["status"]):
            logger.warning(f"[tx_hash]: {tx_hash} indicates a failed status, skipping ...")
            continue

        from_address = tx["from"]
        to_address = tx["to"]
        blockchain_amount = tx["value"]
        amount = w3.from_wei(int(tx["value"]), "ether")
        block = tx["blockNumber"]
        timestamp = datetime.fromtimestamp(int(tx["timeStamp"]))

        from_user = [u['username'] for u in users if u['address'].lower() == from_address.lower()]
        if from_user:
            from_user = from_user[0]
        else:
            from_user = None

        with sqlite3.connect("arb1.db") as db:
            db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
            cur = db.cursor()
            sqlite3.register_adapter(Decimal, adapt_decimal)
            sqlite3.register_converter("Decimal", convert_decimal)

            sql_insert = """
                INSERT INTO shuttle (from_user, from_address, blockchain_amount, readable_amount, block_number,
                    gno_tx_hash, created_at)
                SELECT ?, ?, ?, ?, ?, ?, ?
                WHERE NOT EXISTS (select 1 from shuttle where gno_tx_hash = ?);
            """

            cur.execute(sql_insert, [from_user, from_address, blockchain_amount, amount, block,
            tx_hash, datetime.now(), tx_hash])

            sql_update = """
                UPDATE run_data set latest_block = ?;
            """

            cur.execute(sql_update, [block])

    process_shuttles(users)