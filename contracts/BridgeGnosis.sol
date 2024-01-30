// SPDX-License-Identifier: MIT
pragma solidity ^0.8.0;

import {IERC20} from "@openzeppelin/contracts/token/ERC20/IERC20.sol";

contract BridgeGnosis {
    address[] public acceptedTokens;
    address public constant multiSig = 0x682b5664C2b9a6a93749f2159F95c23fEd654F0A;
    uint256 transactionId = 0;

    event BridgeInit(uint256 transactionId, address from, uint256 value);

    uint private unlocked = 1;
    modifier lock() {
        require (unlocked == 1, "LOCKED");
        unlocked = 0;
        _;
        unlocked = 1;
    }

    constructor() {
        acceptedTokens = [
            // Donut
            address(0x524B969793a64a602342d89BC2789D43a016B13A)
        ];
    }

    // note that msg.sender will need to approve this smart contract to spend
    // on their behalf prior to calling this function
    function deposit(
        address _from,
        uint256 _value,
        address _token
    ) public lock returns (uint256) {
        // require that the asset to bridge is an accepted ERC20 token
        require(isAcceptedToken(_token), "ERC20: token not accepted");

        // set the token instance to the provided token address
        IERC20 token = IERC20(_token);

        // require that the payment is fully made
        require(
            token.transferFrom(_from, multiSig, _value),
            "ERC20: transfer failed"
        );

        // generate a unique transaction ID that will be used as an input
        // for Arb1 side of the bridge.
        transactionId++;
        emit BridgeInit(transactionId, _from, _value);
        return transactionId;
    }

    function isAcceptedToken(address _token) public view returns (bool) {
        // Check if the provided token address is in the acceptedTokens array
        for (uint256 i = 0; i < acceptedTokens.length; i++) {
            if (acceptedTokens[i] == _token) {
                return true;
            }
        }

        return false;
    }
}