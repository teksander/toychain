from toychain.src.utils import constants
from toychain.src.utils.constants import MEMPOOL_SYNC_TAG, CHAIN_SYNC_TAG, BLOCK_REQUEST_TAG, DEBUG
from toychain.src.utils.helpers import dict_to_transaction, transaction_to_dict, block_to_list

import logging
logger = logging.getLogger('w3')

class MessageHandler:

    def __init__(self, node_server):

        self.node_server = node_server
        self.node = node_server.node
        self.enode = self.node.enode

        # Message type to handler  mappings 
        self.requests_handler_mapping = {
            MEMPOOL_SYNC_TAG: self.handle_request_mempool,
            CHAIN_SYNC_TAG: self.handle_request_sync,
            BLOCK_REQUEST_TAG: self.handle_request_block,
            }

        self.answers_handler_mapping = { 
            CHAIN_SYNC_TAG: self.handle_answer_sync,
            MEMPOOL_SYNC_TAG: self.handle_answer_mempool,
            BLOCK_REQUEST_TAG: self.handle_answer_block,
            }

    def handle_request(self, msg):
        """
        Returns a message containing the requested information
        """
        if not self.validate_message(msg):
            return

        handler = self.requests_handler_mapping.get(msg["type"])
        content = handler(msg)
        return self.construct_message(content, msg["type"])

    def handle_answer(self, msg):
        """
        Processes the answer based on its type.
        """
        if not self.validate_message(msg):
            return

        handler = self.answers_handler_mapping.get(msg["type"])
        handler(msg)

    def construct_message(self, data, msg_type, receiver=None):
        return {
                "type": msg_type,
                "receiver": receiver,
                "sender": self.enode,
                "data": data
                }

    def validate_message(self, msg):
        mandatory_keys = {"data", "type", "receiver", "sender"}
        if not isinstance(msg, dict):
            logger.error(f"Invalid message format: {msg}")
            return False

        missing_keys = mandatory_keys - msg.keys()
        if missing_keys:
            logger.error(f"Message missing keys {missing_keys}: {msg}")
            return False

        return True

    ################# REQUEST HANDLERS ########################
    def handle_request_sync(self, msg):
        """ Returns the latest hash and difficulty """
        return (self.node.get_block('last').get_header_hash(), self.node.get_block('last').total_difficulty)

    def handle_request_mempool(self, msg):
        """ Returns the current mempool as a list """
        return [transaction_to_dict(t) for t in self.node.mempool.values()]

    def handle_request_block(self, msg):
        """ Checks if one of the indicated blocks is in its chain """
           
        for header_hash, height in msg["data"]:

            potential_common_block = self.node.get_block(height)
            if potential_common_block is None:
                return None, None

            if header_hash == potential_common_block.get_header_hash():
                # Common block found
                partial_chain = []
                i = height + 1
                while i < self.node.current_height:
                    partial_chain.append(block_to_list(self.node.get_block(i)))
                    i += 1
                return height, partial_chain
        return height, None

    ################# ANSWER HANDLERS  ########################

    def handle_answer_sync(self, msg):

        peer_hash, peer_difficulty   = msg["data"]
        local_hash, local_difficulty = self.node.get_sync_info()

        # Case 1: My chain is already synchronized with the peer
        if local_hash == peer_hash:
            return

        # Case 2: My chain is longer or has equal difficulty
        elif local_difficulty >= peer_difficulty:
            return

        # Case 3: Peer has longer chain
        else:
            self.request_block(self.node.current_height, msg["sender"])

    def handle_answer_mempool(self, msg):
        
        self.node.sync_mempool([dict_to_transaction(d) for d in msg["data"]])

    def handle_answer_block(self, msg):

        height, partial_chain = msg["data"]

        if height is None:
            return

        if partial_chain is None:
            self.request_block(height, msg["sender"])
            
        elif len(partial_chain) > 0:
            self.node.sync_chain(partial_chain, height)

    def request_block(self, current_height, enode):
        """ Send the last 5 blocks header hash """
    
        content = []
        # Sends the block header + height of the last 5 blocks before the precised height
        for block in reversed(self.node.chain[max(0, current_height - 5):current_height]):
            content.append((block.get_header_hash(), block.height))

        request = self.construct_message(content, BLOCK_REQUEST_TAG, enode)
        self.node_server.send_request(enode, request)






# OLD VERSIONS

    # def handle_answer_sync(self, msg):
    #     last_block = self.node.get_block('last')
    #     if msg["data"] == (last_block.get_header_hash(), last_block.total_difficulty):
    #         # Chains are synchronised
    #         if constants.DEBUG:
    #             logger.debug(f"Node {self.node.id} is chain sync")
    #         return

    #     if last_block.total_difficulty <= msg["data"][1]:
    #         # If the chains have equal sizes, node keeps his
    #         # If the chain of the node is longer than the received one, let him do the work
    #         self.request_block(len(self.node.chain), msg["sender"])
    #     else:
    #         if constants.DEBUG:
    #             logger.debug(f"Node {self.node.id} has a current diff of {last_block.total_difficulty}")
