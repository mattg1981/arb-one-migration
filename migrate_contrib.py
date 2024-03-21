import csv
import json
import logging
import os
import pathlib
import time

from logging.handlers import RotatingFileHandler
from os import path
from urllib import request
from dotenv import load_dotenv
from web3 import Web3

from safe.safe_tx import SafeTx
from safe.safe_tx_builder import build_tx_builder_json

if __name__ == '__main__':
    # load environment variables
    load_dotenv()

    # load config
    with open(path.normpath("config.json"), 'r') as f:
        config = json.load(f)

    # set up logging
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("migrate_contrib")
    logger.setLevel(logging.INFO)

    base_dir = path.dirname(path.abspath(__file__))
    log_path = path.join(base_dir, "logs/migrate_contrib.log")
    handler = RotatingFileHandler(path.normpath(log_path), maxBytes=2500000, backupCount=4)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info("connecting to provider - ankr.com...")
    web3 = Web3(Web3.HTTPProvider(os.getenv('ANKR_API_PROVIDER')))
    if not web3.is_connected():
        logger.error("  failed to connect to ankr node [gnosis]")
        exit(4)
    else:
        logger.info("  success.")

    # abi
    with open(os.path.join(pathlib.Path().resolve(), "abi/contrib_gno.json"), 'r') as f:
        contrib_abi_gno = json.load(f)

    with open(os.path.join(pathlib.Path().resolve(), "abi/contrib_arb1.json"), 'r') as f:
        contrib_abi_arb1 = json.load(f)

    # contrib gnosis and arb are slightly different
    contrib_contract_gno = web3.eth.contract(address=web3.to_checksum_address(
        config["contracts"]["gnosis"]["contrib"]), abi=contrib_abi_gno)

    contrib_contract_arb1 = web3.eth.contract(address=web3.to_checksum_address(
        config["contracts"]["arb1"]["contrib"]), abi=contrib_abi_arb1)

    # ---- build the final file and produce a csv file ----------

    # logger.info("grabbing users.json file...")
    # users = json.load(request.urlopen(f"https://ethtrader.github.io/donut.distribution/users.json"))
    #
    #
    # user_contrib = []
    #
    # for user in users:
    #     logger.info(f"lookup contrib for user: {user['username']}")
    #     address = web3.to_checksum_address(user['address'])
    #
    #     was_success = False
    #     for j in range(1, 8):
    #         current_contrib = 0
    #         try:
    #             current_contrib = contrib_contract_gno.functions.balanceOf(address).call()
    #             logger.info(f"  contrib - {current_contrib}")
    #
    #             user_contrib.append({
    #                 'user': user['username'],
    #                 'address': user['address'],
    #                 'contrib': web3.from_wei(current_contrib, "ether"),
    #                 'contrib_gwei': current_contrib
    #             })
    #
    #             was_success = True
    #             break
    #         except Exception as e:
    #             logger.error(e)
    #             time.sleep(15)
    #
    #     if not was_success:
    #         logger.error("  unable to query at this time, attempt at a later time...")
    #         exit(4)
    #
    # with open(os.path.join("out", f"final_gnosis_migration.csv"), 'w') as migration_file:
    #     writer = csv.DictWriter(migration_file, user_contrib[0].keys(), extrasaction='ignore')
    #     writer.writeheader()
    #     writer.writerows(user_contrib)


    # ---- read the final file and produce the resulting safe tx(s) ----------

    with open(os.path.join("out", f"final_gnosis_migration.csv"), newline='') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        records = list(reader)

    start = 0
    size = 850
    batch = 1

    for i in range(start, len(records), size):
        x = i
        current_batch = records[x:x + size]

        contrib_contract_data = contrib_contract_arb1.encodeABI("mintMany", [
            [web3.to_checksum_address(u['address']) for u in current_batch if int(u['contrib_gwei']) > 0],
            [int(u['contrib_gwei']) for u in current_batch if int(u['contrib_gwei']) > 0]
        ])

        transactions = [
            SafeTx(to=web3.to_checksum_address(config["contracts"]["arb1"]["contrib"]), value=0, data=contrib_contract_data),
        ]

        tx = build_tx_builder_json(f"Arb One Contrib Migration", transactions)

        tx_file_path = os.path.join("out", f"arb1_contrib_migration_{batch}.json")

        batch += 1

        if os.path.exists(tx_file_path):
            os.remove(tx_file_path)

        with open(tx_file_path, 'w') as f:
            json.dump(tx, f, indent=4)



