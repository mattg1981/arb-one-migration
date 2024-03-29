import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timedelta
from decimal import Decimal

from logging.handlers import RotatingFileHandler
from urllib import request

import praw
from dotenv import load_dotenv
from web3 import Web3
from blockscan import Blockscan

MAX_SHUTTLES_ALLOWED_PER_USER = 1
MAX_HOURS_FOR_OLDEST_TX = 3
MIN_SHUTTLE_AMOUNT = 30
SHUTTLES_NEEDED_FOR_TX = 4
SHUTTLE_STARTING_BLOCK = 33043953
# SHUTTLE_STARTING_BLOCK = 33072948


def do_shuttle():
    with sqlite3.connect(config["db_location"]) as db:
        db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        cur = db.cursor()
        cur.execute("select gno_tx_hash from shuttle;")
        saved_db_transactions = cur.fetchall()

    saved_db_transactions = [x['gno_tx_hash'] for x in saved_db_transactions]

    # get users file that will be used for any user <-> address lookups
    # this file performs all ENS lookups and is updated 3 times per day
    logger.info("grabbing users.json file...")
    users = json.load(request.urlopen(f"https://ethtrader.github.io/donut.distribution/users.json"))

    # donut on gnosis
    valid_tokens = [config["contracts"]["gnosis"]["donut"].lower()]

    # EthTraderCommunity
    ignored_addresses = ["0xf7927bf0230c7b0e82376ac944aeedc3ea8dfa25"]

    # get gnosis transactions
    logger.info(f"querying blockscout...")

    client = Blockscan(100, os.getenv('BLOCKSCOUT_API_KEY'), is_async=False)

    try:
        transfers = client.accounts.get_tokentx_token_transfer_events_by_address(config["multisig"]["gnosis"], SHUTTLE_STARTING_BLOCK, 99999999, "asc")
    except Exception as e:
        logger.error(e)
        transfers = None

    if not transfers:
        logger.info("no results ...")
        # return

    # connect to ankr api
    logger.info("connecting to ANKR api...")
    w3_gno = Web3(Web3.HTTPProvider(os.getenv('ANKR_API_PROVIDER')))
    if w3_gno.is_connected():
        logger.info("  success.")
    else:
        logger.error(f"  failed to connect to ANKR: aborting....")
        return

    dt_process_runtime = datetime.now()

    # iterate the gnosis transactions
    if transfers:
        for tx in transfers:
            tx_hash = tx["hash"]

            logger.debug(f"[tx_hash]: {tx_hash}")

            if tx_hash in saved_db_transactions:
                continue

            if tx["contractAddress"].lower() not in valid_tokens:
                continue

            if tx["from"].lower() in ignored_addresses:
                continue

            if tx["to"].lower() != config['multisig']['gnosis'].lower():
                continue

            w3_transaction = w3_gno.eth.get_transaction(tx_hash)
            inpt = w3_transaction.input.hex()

            # not a transfer event
            if not inpt[:10] == "0xa9059cbb":
                logger.debug(f"  not a transfer transaction [tx_hash]: {tx_hash}")
                continue

            logger.info(f"processing [tx_hash]: {tx_hash}")

            # ensure at least 100 confirmations
            if not int(tx['confirmations']) >= 100:
                logger.info(f"  transaction does not have 100 confirmations yet ...")
                continue

            # confirm the status of the tx hash = 1 (success)
            logger.info(f"  confirming status...")
            if not client.transactions.get_tx_receipt_status(tx_hash):
                logger.warning(f"    [tx_hash]: {tx_hash} indicates a failed status, skipping ...")
                continue
            else:
                logger.info("    success.")

            from_address = tx["from"]
            to_address = tx["to"]
            blockchain_amount = tx["value"]
            amount = w3_gno.from_wei(int(tx["value"]), "ether")
            block = tx["blockNumber"]
            timestamp = datetime.fromtimestamp(int(tx["timeStamp"]))

            logger.info("  save tx to db if not already present...")

            sql_insert = """
                    INSERT INTO shuttle (from_address, blockchain_amount, readable_amount, block_number,
                        gno_tx_hash, gno_timestamp, created_at)
                    SELECT ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (select 1 from shuttle where gno_tx_hash = ?);
                """

            with sqlite3.connect(config["db_location"]) as db:
                cur = db.cursor()
                cur.execute(sql_insert, [from_address, blockchain_amount, amount, block,
                                         tx_hash, timestamp, dt_process_runtime, tx_hash])

    # attempt to name match any shuttles where we do not have user information
    # (including new records just inserted)
    logger.info("attempt to match any addresses without usernames...")

    unmatched_sql = '''
            select * from shuttle where processed_at is null and from_user is null;
            '''
    with sqlite3.connect(config["db_location"]) as db:
        db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        cur = db.cursor()
        cur.execute(unmatched_sql)
        unmatched = cur.fetchall()

    for record in unmatched:
        username = [u['username'] for u in users if u['address'].lower() == record['from_address'].lower()]

        # if we have a match, update the database.
        if username:
            match_sql = '''
                        update shuttle set from_user = ? where from_address = ?;
                    '''
            with sqlite3.connect(config["db_location"]) as db:
                cur = db.cursor()
                cur.execute(match_sql, [username[0], record['from_address']])

            logger.info(f"notify {username[0]} about gnosis transaction being discovered...")

            if Decimal(record['readable_amount']) < Decimal(MIN_SHUTTLE_AMOUNT):
                message = config["lottery_message"]
                subject = 'Arb1 Shuttle - Lottery Entry!'
            else:
                message = config["gno_confirmation_message"]
                subject = 'Arb1 Shuttle - Gnosis deposit found!'

            try:
                # create message
                message = (message
                           .replace("#NAME#", username[0])
                           .replace("#GNO_TX_HASH#", record['gno_tx_hash'])
                           .replace("#AMOUNT#", str(record['readable_amount']))
                           .replace("#TOKEN#", "DONUT"))

                # send message

                reddit.redditor(username[0]).message(subject=subject, message=message)
                logger.error(f"  notified [{username[0]}]...")
            except Exception as e:
                logger.error(f"  could not send notification to [{username[0]}]")

    # gno_notify_sql = '''
    #             select * from shuttle where created_at = ?;
    #             '''
    #
    # with sqlite3.connect(config["db_location"]) as db:
    #     db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
    #     cur = db.cursor()
    #     cur.execute(gno_notify_sql, [dt_process_runtime])
    #     gno_notifications = cur.fetchall()
    #
    # for gno_notification in gno_notifications:
    #     if Decimal(gno_notification['readable_amount']) < Decimal(MIN_SHUTTLE_AMOUNT):
    #         message = config["lottery_message"]
    #         subject = 'Arb1 Shuttle - Lottery Entry!'
    #     else:
    #         message = config["gno_confirmation_message"]
    #         subject = 'Arb1 Shuttle - Gnosis deposit found!'
    #
    #     try:
    #         # create message
    #         message = (message
    #                .replace("#NAME#", gno_notification['from_user'])
    #                .replace("#GNO_TX_HASH#", gno_notification['gno_tx_hash'])
    #                .replace("#AMOUNT#", str(gno_notification['readable_amount']))
    #                .replace("#TOKEN#", "DONUT"))
    #
    #     # send message
    #
    #         reddit.redditor(gno_notification['from_user']).message(subject=subject, message=message)
    #     except Exception as e:
    #         logger.error(f"  could not send notification to [{gno_notification['from_user']}]")

    logger.info("begin processing shuttles...")
    logger.info("connect to INUFRA...")

    # switch w3 provider over to Arb1
    w3_arb = Web3(Web3.HTTPProvider(os.getenv('INFURA_IO_API')))

    if not w3_arb.is_connected():
        logger.error("failed to connect to INFURA")
        return
    else:
        logger.info("  success.")

    # get shuttle records from db
    logger.info("get shuttles from db...")

    # select the first shuttle transaction per user
    shuttle_sql = '''
            select * 
            from
              (select *, row_number() over (partition by from_address order by created_at asc) rank
              from shuttle
              where readable_amount >= ? and processed_at is null)
            where rank = 1 and from_address not in (select from_address from shuttle where processed_at is not null);
            '''

    with sqlite3.connect(config["db_location"]) as db:
        db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        cur = db.cursor()
        cur.execute(shuttle_sql, [MIN_SHUTTLE_AMOUNT])
        shuttles = cur.fetchall()

    logger.info(f"  {len(shuttles)} shuttles found...")

    with open(os.path.normpath("abi/distribute.json"), 'r') as f:
        distribute_abi = json.load(f)

    with open('abi/erc20.json') as abi_file:
        donut_abi = json.load(abi_file)

    shuttle_address = w3_arb.to_checksum_address(config['addresses']["shuttle"])
    donut_address = w3_arb.to_checksum_address(config["contracts"]["arb1"]["donut"])
    distribute_address = w3_arb.to_checksum_address(config["contracts"]["arb1"]["distribute"])

    distribute_contract = w3_arb.eth.contract(address=distribute_address, abi=distribute_abi)
    donut_contract = w3_arb.eth.contract(address=donut_address, abi=donut_abi)
    donut_balance = donut_contract.functions.balanceOf(shuttle_address).call()

    distribute_tx_list = []
    current_batch_amt = 0

    for shuttle in shuttles:
        logger.info(f"processing address [addr={shuttle['from_address']}]")

        if int(shuttle["readable_amount"] < MIN_SHUTTLE_AMOUNT):
            logger.info(
                f" shuttle amount too small: "
                f"shuttle amount [{shuttle['readable_amount']}] - minimum required [{MIN_SHUTTLE_AMOUNT}]")
            continue

        if donut_balance < int(shuttle["blockchain_amount"]):
            logger.info(" shuttle does not have enough donuts for this tx.")
            continue

        donut_balance -= int(shuttle["blockchain_amount"])
        current_batch_amt += int(shuttle["blockchain_amount"])

        distribute_tx_list.append({
            "address": Web3.to_checksum_address(shuttle['from_address']),
            "amt": int(shuttle["blockchain_amount"]),
            "gno_tx_hash": shuttle["gno_tx_hash"],
            "gno_timestamp": shuttle["gno_timestamp"]
        })

    logger.info(f"{len(distribute_tx_list)} of {len(shuttles)} shuttles can be processed at this time")

    if len(distribute_tx_list):
        do_blockchain_tx = False

        logger.info("check to see if we should perform the blockchain transaction...")
        if len(distribute_tx_list) >= SHUTTLES_NEEDED_FOR_TX:
            logger.info("  enough shuttles in this transaction, proceed...")
            do_blockchain_tx = True
        else:
            min_date = datetime.fromisoformat(min([s['gno_timestamp'] for s in distribute_tx_list]))
            if datetime.now() - timedelta(hours=MAX_HOURS_FOR_OLDEST_TX) >= min_date:
                logger.info(f"  oldest shuttle is more than {str(MAX_HOURS_FOR_OLDEST_TX)} hours, proceed...")
                do_blockchain_tx = True

        if not do_blockchain_tx:
            logger.info("  criteria not met, will not perform the transaction...")
        else:
            logger.info("checking gas balance...")
            gas_balance = w3_arb.from_wei(w3_arb.eth.get_balance(shuttle_address), "ether")

            if gas_balance < 0.0005:
                logger.error(f" shuttle is out of gas. balance: [{gas_balance}]")
                exit(4)

            logger.info("building blockchain transaction...")
            transaction = distribute_contract.functions.distribute(
                [d['address'] for d in distribute_tx_list],
                [d['amt'] for d in distribute_tx_list],
                donut_address
            ).build_transaction({
                'from': shuttle_address,
                'nonce': w3_arb.eth.get_transaction_count(shuttle_address)
            })

            # sign the transaction
            signed = w3_arb.eth.account.sign_transaction(transaction, os.getenv('SHUTTLE_PRIVATE_KEY'))

            # send the transaction
            logger.info("sending blockchain transaction...")
            tx_hash = w3_arb.eth.send_raw_transaction(signed.rawTransaction)
            receipt = w3_arb.eth.wait_for_transaction_receipt(tx_hash)

            if receipt.status:
                human_readable_tx_hash = w3_arb.to_hex(tx_hash)
                logger.info(f" success! tx_hash: [{human_readable_tx_hash}]")
            else:
                logger.error("  transaction failed!")
                logger.error(f"  receipt {receipt}")
                return

            logger.info("update db...")
            try:
                for distribute_tx in distribute_tx_list:
                    update_sql = '''
                            update shuttle set processed_at = ?, arb_tx_hash = ? where gno_tx_hash = ?;
                        '''
                    with sqlite3.connect(config["db_location"]) as db:
                        cur = db.cursor()
                        cur.execute(update_sql, [datetime.now(), human_readable_tx_hash, distribute_tx["gno_tx_hash"]])
            except Exception as e:
                logger.critical(e)
                exit(4)

    # find transactions that need to be notified (if any)
    logger.info("finding transactions that need notifications ...")

    notification_sql = '''
                        select * 
                        from shuttle 
                        where processed_at is not null 
                          and notified_at is null 
                          and from_user is not null;
                    '''

    with sqlite3.connect(config["db_location"]) as db:
        db.row_factory = lambda c, r: dict(zip([col[0] for col in c.description], r))
        cur = db.cursor()
        cur.execute(notification_sql)
        notifications = cur.fetchall()

    if not notifications:
        logger.info("  none needed")

    for n in notifications:
        logger.info(f"notifying ::: [user]: {n['from_user']} [amount]: {n['readable_amount']}")

        try:
            # create message
            message = (config["shuttle_message"]
                   .replace("#NAME#", n['from_user'])
                   .replace("#ARB_TX_HASH#", n['arb_tx_hash'])
                   .replace("#GNO_TX_HASH#", n['gno_tx_hash'])
                   .replace("#AMOUNT#", str(n['readable_amount']))
                   .replace("#SHUTTLE_MIN#", str(MIN_SHUTTLE_AMOUNT))
                   .replace("#TOKEN#", "DONUT"))

            # send message
            reddit.redditor(n['from_user']).message(subject="Arb1 Shuttle Successful!", message=message)
            logger.info("  successfully notified on reddit...")
        except Exception as e:
            logger.error(f"  could not send notification to [{n['from_user']}]")

        logger.info("updating sql notified_at")

        notification_update_sql = '''
                update shuttle 
                set notified_at = ?
                where gno_tx_hash = ?
            '''

        with sqlite3.connect(config["db_location"]) as db:
            cur = db.cursor()
            cur.execute(notification_update_sql, [datetime.now(), n['gno_tx_hash']])

        logger.info("  successfully updated db... ")

        time.sleep(1)

    logger.info("complete.")


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
    file_handler = RotatingFileHandler(os.path.normpath(log_path), maxBytes=2500000, backupCount=100)
    file_handler.setFormatter(formatter)
    console_handler = logging.StreamHandler()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # set up praw
    reddit = praw.Reddit(client_id=os.getenv('REDDIT_CLIENT_ID'),
                         client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
                         username=os.getenv('REDDIT_USERNAME'),
                         password=os.getenv('REDDIT_PASSWORD'),
                         user_agent="arb1 shuttle by u/mattg1981")

    # setup db
    sqlite3.register_adapter(Decimal, lambda s: str(s))
    sqlite3.register_converter("Decimal", lambda s: Decimal(s))

    with sqlite3.connect(config["db_location"]) as db:
        sql_create = """        
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
                    gno_timestamp DATETIME NOT NULL,
                    processed_at DATETIME,
                    notified_at DATETIME,
                    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
            """
        cur = db.cursor()
        cur.executescript(sql_create)

    logger.info("begin...")

    while True:
        try:
            do_shuttle()
        except Exception as e:
            logger.error(e)
        finally:
            logger.info("sleep 240 ....")
            time.sleep(240)
