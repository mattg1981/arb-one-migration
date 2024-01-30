// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import "@openzeppelin/contracts/utils/Strings.sol";

contract BridgeArb1 {
    address public constant _multisigAddress = 0x439ceE4cC4EcBD75DC08D9a17E92bDdCc11CDb8C;
    uint256 public _bridgePrice = 0.0005 ether;

    event BridgeFinalize(uint256 transactionId, address sender);

    constructor() {}

    function fundBridgeTransaction(uint256 transactionId, address sender)
        public
        payable
    {
        require(
            msg.value >= _bridgePrice,
            string.concat(
                "Not enough ETH included in the transaction; needed amount: ",
                Strings.toString(_bridgePrice)
            )
        );

        // send msg.value to the multisig
        payable(_multisigAddress).transfer(msg.value);

        emit BridgeFinalize(transactionId, sender);
    }
}
