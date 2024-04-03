import json
import os
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from web3 import Web3

TX_HASH = '0xedf1fc2e7eb9aafe5c6ada43ec91a143923d9a191a607cb7e27bb1e61d8d65d4'
SHUTTLE_START_BLOCK = 33043953

if __name__ == '__main__':
    # load .env file
    load_dotenv()

    # load config
    with open(os.path.normpath("config.json"), 'r') as f:
        config = json.load(f)

    with sqlite3.connect(config["db_location"]) as db:
        db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        tx_exists_query = '''
            select * from shuttle 
            where gno_tx_hash = ?
        '''
        cur = db.cursor()
        cur.execute(tx_exists_query, [TX_HASH])
        tx_exists = cur.fetchall()

    if tx_exists:
        print('transaction has already been processed')
        print(tx_exists)
        exit(4)

    w3 = Web3(Web3.HTTPProvider(os.getenv('ANKR_API_PROVIDER')))

    if not w3.is_connected():
        print('w3 not connected .. abort')
        exit(4)

    tx = w3.eth.get_transaction(TX_HASH)
    receipt = w3.eth.get_transaction_receipt(TX_HASH)

    if not receipt.status:
        print('transaction is in a failed status')
        exit(4)

    block = int(tx['blockNumber'])

    if block < SHUTTLE_START_BLOCK:
        print('transaction submitted prior to shuttle starting')
        exit(4)

    tx_block = w3.eth.get_block(block)

    # get the number of confirmations
    confirmations = int(w3.eth.block_number) - int(tx['blockNumber'])

    if confirmations < 100:
        print('< 100 confirmations')
        exit(4)

    tx_hash = tx_block['hash']
    from_address = tx['from']

    to_address = tx['to']
    if to_address.lower() != config["multisig"]["gnosis"].lower():
        print('transaction not sent to Multisig...')
        exit(4)

    timestamp = datetime.fromtimestamp(tx_block['timestamp'])

    input = tx.input.hex()

    if not input[:10] == '0xa9059cbb':
        print('not a transfer transaction')
        exit(4)

    blockchain_amount = int(input[74:], 16)
    friendly_amount = w3.from_wei(blockchain_amount, "ether")

    sql_insert = """
        INSERT INTO shuttle (from_address, blockchain_amount, readable_amount, block_number,
            gno_tx_hash, gno_timestamp, created_at)
        SELECT ?, ?, ?, ?, ?, ?, ?
        WHERE NOT EXISTS (select 1 from shuttle where gno_tx_hash = ?);
    """

    with sqlite3.connect(config["db_location"]) as db:
        cur = db.cursor()
        cur.execute(sql_insert, [from_address, blockchain_amount, friendly_amount, block,
                                 tx_hash, timestamp, datetime.now(), tx_hash])

