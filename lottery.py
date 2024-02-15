import json
import logging
import os
import secrets
import sqlite3
from decimal import Decimal
from logging.handlers import RotatingFileHandler

import praw
from dotenv import load_dotenv
from web3 import Web3

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

    # set up praw
    reddit = praw.Reddit(client_id=os.getenv('REDDIT_CLIENT_ID'),
                         client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                         username=os.getenv('REDDIT_USERNAME'),
                         password=os.getenv('REDDIT_PASSWORD'),
                         user_agent="arb1 lottery by u/mattg1981")

    with open(os.path.normpath("abi/distribute.json"), 'r') as f:
        distribute_abi = json.load(f)

    with open('abi/erc20.json') as abi_file:
        donut_abi = json.load(abi_file)

    w3 = Web3(Web3.HTTPProvider(os.getenv('INFURA_IO_API')))
    shuttle_address = w3.to_checksum_address(config['addresses']["shuttle"])
    donut_address = w3.to_checksum_address(config["contracts"]["arb1"]["donut"])
    distribute_address = w3.to_checksum_address(config["contracts"]["arb1"]["distribute"])

    distribute_contract = w3.eth.contract(address=distribute_address, abi=distribute_abi)
    donut_contract = w3.eth.contract(address=donut_address, abi=donut_abi)
    donut_balance = donut_contract.functions.balanceOf(shuttle_address).call()

    logger.info("begin...")

    # setup db
    sqlite3.register_adapter(Decimal, lambda s: str(s))
    sqlite3.register_converter("Decimal", lambda s: Decimal(s))

    with sqlite3.connect(config["db_location"]) as db:
        db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        lottery_members_sql = """        
        select from_user, from_address
        from shuttle
        where readable_amount <= 30
        group by from_user;
        """

        lottery_amount_sql = """
            select sum(readable_amount) 'amount' 
            from shuttle 
            where readable_amount <= 30;
        """
        cur = db.cursor()
        cur.execute(lottery_members_sql)
        lottery_members = cur.fetchall()
        #lottery_members = [x['from_user'] for x in lottery_members]

        cur.execute(lottery_amount_sql)
        lottery_amount = cur.fetchone()['amount']

    logger.info(f"lottery members are: {lottery_members}")
    logger.info(f"lottery amount is: {lottery_amount}")

    # The secrets module is used for generating cryptographically strong random numbers suitable for managing data such
    # as passwords, account authentication, security tokens, and related secrets.  The secrets module provides access
    # to the most secure source of randomness that your operating system provides.

    # https://docs.python.org/3/library/secrets.html#module-secrets

    winner = secrets.choice(lottery_members)
    logger.info(f"winner is: [{winner['from_user']}]")

    # send the transaction
    logger.info("building blockchain transaction...")

    transaction = distribute_contract.functions.distribute(
        [w3.to_checksum_address(winner['from_address'])],
        [w3.to_wei(Decimal(lottery_amount), "ether")],
        donut_address
    ).build_transaction({
        'from': w3.to_checksum_address(shuttle_address),
        'nonce': w3.eth.get_transaction_count(shuttle_address)
    })

    # sign the transaction
    signed = w3.eth.account.sign_transaction(transaction, os.getenv('SHUTTLE_PRIVATE_KEY'))

    # send the transaction
    logger.info("sending blockchain transaction...")
    tx_hash = w3.eth.send_raw_transaction(signed.rawTransaction)
    receipt = w3.eth.wait_for_transaction_receipt(tx_hash)

    if receipt.status:
        human_readable_tx_hash = w3.to_hex(tx_hash)
        logger.info(f" success! tx_hash: [{human_readable_tx_hash}]")
    else:
        logger.error("  transaction failed!")
        logger.error(f"  receipt {receipt}")
        exit(4)

    # notify the user on reddit
    message = ("Congratulations #NAME#, you have won the r/EthTrader Gnosis -> ARB 1 Shuttle Lottery worth #AMOUNT# "
               "Donut!  The ARB1 transaction hash is: #TX_HASH#.")

    message = (message
               .replace("#NAME#", winner['from_user'])
               .replace("#TX_HASH#", human_readable_tx_hash)
               .replace("#AMOUNT#", str(lottery_amount)))

    # send message
    try:
        reddit.redditor(winner['from_user']).message(subject="Gnosis -> ARB 1 Lottery Winner!", message=message)
    except Exception as e:
        logger.error(f"  could not send notification to [{winner['from_user']}]")

