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

ARB1_CONTRIB_CONTRACT_ADDRESS = "0xF28831db80a616dc33A5869f6F689F54ADd5b74C"

if __name__ == '__main__':
    # load environment variables
    load_dotenv()

    # load config
    with open(path.normpath("config.json"), 'r') as f:
        config = json.load(f)

    # set up logging
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("ban_bot")
    logger.setLevel(logging.INFO)

    base_dir = path.dirname(path.abspath(__file__))
    log_path = path.join(base_dir, "logs/migrate_contrib.log")
    handler = RotatingFileHandler(path.normpath(log_path), maxBytes=2500000, backupCount=4)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    logger.info("grabbing users.json file...")
    users = json.load(request.urlopen(f"https://ethtrader.github.io/donut.distribution/users.json"))

    logger.info("connecting to provider - ankr.com...")
    web3 = Web3(Web3.HTTPProvider(os.getenv('ANKR_API_PROVIDER')))
    if not web3.is_connected():
        logger.error("  failed to connect to ankr node [gnosis]")
        exit(4)
    else:
        logger.info("  success.")

    # lookup abi
    with open(os.path.join(pathlib.Path().resolve(), "abi/contrib_gno.json"), 'r') as f:
        contrib_abi_gno = json.load(f)

    with open(os.path.join(pathlib.Path().resolve(), "abi/contrib_arb1.json"), 'r') as f:
        contrib_abi_arb1 = json.load(f)

    # contrib gnosis and arb are slightly different
    contrib_contract_gno = web3.eth.contract(address=web3.to_checksum_address(
        config["contracts"]["gnosis"]["contrib"]), abi=contrib_abi_gno)

    contrib_contract_arb1 = web3.eth.contract(address=web3.to_checksum_address(
        ARB1_CONTRIB_CONTRACT_ADDRESS), abi=contrib_abi_arb1)

    for user in users:
        logger.info(f"lookup contrib for user: {user['username']}")
        address = web3.to_checksum_address(user['address'])

        was_success = False
        for j in range(1, 8):
            try:
                current_contrib = contrib_contract_gno.functions.balanceOf(address).call()
                logger.info(f"  contrib - {current_contrib}")
                user['current_contrib'] = current_contrib
                was_success = True
                break
            except Exception as e:
                logger.error(e)
                time.sleep(15)

        if not was_success:
            logger.error("  unable to query at this time, attempt at a later time...")
            exit(4)

    contrib_contract_data = contrib_contract_arb1.encodeABI("mintMany", [
        [web3.to_checksum_address(u['address']) for u in users if float(u['current_contrib']) > 0],
        [u['current_contrib'] for u in users if float(u['current_contrib']) > 0]
    ])

    transactions = [
        SafeTx(to=web3.to_checksum_address(ARB1_CONTRIB_CONTRACT_ADDRESS), value=0, data=contrib_contract_data),
    ]

    tx = build_tx_builder_json(f"Arb One Contrib Migration", transactions)

    tx_file_path = os.path.join("out", f"contrib_migration.json")

    if os.path.exists(tx_file_path):
        os.remove(tx_file_path)

    with open(tx_file_path, 'w') as f:
        json.dump(tx, f, indent=4)



