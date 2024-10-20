import copy
import threading
from random import randint
from time import time, sleep

from toychain.src.utils import constants
from toychain.src.utils.helpers import gen_enode

from toychain.src.Block import Block, State

import logging
logger = logging.getLogger('pow')

# Parameters for Proof-of-Work
MINING_DIFFICULTY = 99900  
DIFF_CAP = 100000
ROBOT_HASHPOWER = 1

# Default genesis block when argument is not passed when creating node
GENESIS_BLOCK = Block(0, 0000, [], 0, 0, 0, 0, nonce = 1, state = State())

class ProofOfWork:
    def __init__(self, genesis = GENESIS_BLOCK):
        self.genesis = genesis
        self.block_generation = VirtualMining

        # Boolean to check or not the block states
        self.trust = True
        self.trust_mining = True # must be true for virtual mining

    def verify_chain(self, chain, previous_state):
        last_block = chain[0]
        if not self.verify_block(last_block, previous_state):
            return False
        i = 1
        while i < len(chain):
            last_block_hash = last_block.compute_block_hash()

            # Check the block
            if not self.verify_block(chain[i], last_block.state):
                logging.error("Block error")
                logging.error(chain[i].__repr__())
                return False

            # Check the parent hash
            elif chain[i].parent_hash != last_block_hash:
                logging.error("Error in the blockchain")
                logging.error(chain[i].parent_hash + "###" + last_block_hash)
                return False

            else:
                last_block = chain[i]
            i += 1
        return True

    def verify_block(self, block, previous_state):

        # Verify block state
        if not self.trust:
            s = copy.deepcopy(previous_state)
            for transaction in block.data:
                s.apply_transaction(transaction)
            if s.state_hash != block.state.state_hash:
                logging.error(f"Invalid state {previous_state.state_variables}")
                logging.error(f"{s.state_variables}")
                logging.error(f"{block.data}")
                return False

        # Verify the difficulty of the mining
        if not self.trust_mining:
            target_string = '1' * (256 - block.difficulty)
            target_string = target_string.zfill(256)

            hash_int = int(block.hash, 16)
            binary_hash = bin(hash_int)
            binary_hash = binary_hash[3:]

            if target_string <= binary_hash:
                return False

        return True
        
class VirtualMining():
    """
    Thread class that generate blocks that answer to the consensus rules
    This block generation is done according to the proof of work
    """

    def __init__(self, node):

        self.node = node
        self.difficulty = MINING_DIFFICULTY
        self.timer = self.node.custom_timer

        self.flag = False
        self.sleep = 0
        
    def run(self):
        """
        Perform Virtual Mining (lottery)
        """

        difficulty = randint(0, DIFF_CAP)
        if difficulty <= ROBOT_HASHPOWER*MINING_DIFFICULTY:  
            return

        # I won the mining lottery 
        print('CREATED A BLOCK')
        timestamp = self.timer.time()      
        previous_block = copy.deepcopy(self.node.get_block('last'))
        previous_state = previous_block.state
        mempool = list((self.node.mempool.copy().values()))

        # Filter out transactions already on the blockchain
        data = [tx for tx in mempool if tx.id not in self.node.previous_transactions_id]

        # Generate the new block
        block = Block(
                    previous_block.height+1, 
                    previous_block.hash, 
                    data,
                    self.node.enode,
                    timestamp, 
                    difficulty, 
                    previous_block.total_difficulty, 
                    state = previous_state)

        # Apply transactions to obtain the new state variables
        for transaction in block.data:
            block.state.apply_transaction(transaction, block)

        # Update the blockchain and mempool
        self.node.chain.append(block)
        self.node.previous_transactions_id.update([tx.id for tx in block.data])
        self.node.mempool.clear()

        logger.info(f"Block produced by Node {self.node.id}: ")
        logger.info(f"{repr(block)}")
        logger.info(f"{block.state.state_variables} \n")

    def step(self):
        if self.flag:
            if self.sleep > 0:
                self.sleep -= 1
            else:
                self.run()

    def start(self):
        self.flag = True

    def stop(self):
        self.flag = False

    def update_block(self, block, previous_block):
        """
        Updates the block by refreshing its information

        Args:
            block(Block): block in the production process
            previous_block(Block): the previous block in the chain, it should be the last block
        """
        block.height = previous_block.height + 1
        block.timestamp = self.timer.time()
        block.parent_hash = previous_block.hash
        block.total_difficulty = previous_block.total_difficulty + self.difficulty

        # Reset the state variables and apply them
        block.state.state_variables = previous_block.state.state_variables
        for transaction in block.data:
            block.state.apply_transaction(transaction)

        block.compute_block_hash()

class Mining():
    """
    Thread class that generate blocks that answer to the consensus rules
    This block generation is done according to the proof of work
    NOT FULLY IMPLEMENTED
    """

    def __init__(self, node):

        self.node = node
        self.difficulty = MINING_DIFFICULTY
        self.timer = self.node.custom_timer

        self.flag = False
        
    def run(self):
        """
        Increase the nonce until the hash of the block has the expected number of zeros at the front of the hash
        """
        timestamp = self.timer.time()

        # Get the current block, state and mempool
        previous_block = copy.deepcopy(self.node.get_block('last'))
        previous_state = previous_block.state
        mempool = list((self.node.mempool.copy().values()))

        # Filter out transactions already on the blockchain
        data = [tx for tx in mempool if tx.id not in self.node.previous_transactions_id]

        # Generate the new block
        block = Block(
                    previous_block.height+1, 
                    previous_block.hash, 
                    data,
                    self.node.enode,
                    timestamp, 
                    self.difficulty, 
                    previous_block.total_difficulty, 
                    nonce=randint(0,1000),
                    state = previous_state)

        # Apply transactions to obtain the new state variables
        for transaction in block.data:
            block.state.apply_transaction(transaction, block)

        attempt = 0
        while attempt < 10:
            self.update_block(block, previous_block)

            target_string = '1' * (256 - self.difficulty)
            target_string = target_string.zfill(256)

            hash_int = int(block.compute_block_hash(), 16)
            binary_hash = bin(hash_int)
            binary_hash = binary_hash[3:]

            if binary_hash > target_string:
                block.increase_nonce()

            else:
                self.node.chain.append(block)
                self.node.mempool.clear()
                logging.info(f"Block produced by Node {self.node.id}: ")
                logging.info(f"{repr(block)}")
                logging.info(f"###{block.state.state_variables}### \n")

    def stop(self):
        self.flag.set()

    def update_block(self, block, previous_block):
        """
        Updates the block by refreshing its information

        Args:
            block(Block): block in the production process
            previous_block(Block): the previous block in the chain, it should be the last block
        """
        block.height = previous_block.height + 1
        block.timestamp = self.timer.time()
        block.parent_hash = previous_block.hash
        block.total_difficulty = previous_block.total_difficulty + self.difficulty

        # Reset the state variables and apply them
        block.state.state_variables = previous_block.state.state_variables
        for transaction in block.data:
            block.state.apply_transaction(transaction)

        block.compute_block_hash()

class MiningThread(threading.Thread):
    """
    Thread class that generate blocks that answer to the consensus rules
    This block generation is done according to the proof of work
    """

    def __init__(self, node):
        super().__init__()
        self.node = node
        self.difficulty = MINING_DIFFICULTY

        self.flag = threading.Event()

        self.timer = node.custom_timer

    def run(self):
        """
        Increase the nonce until the hash of the block has the expected number of zeros at the front of the hash
        """
        timestamp = self.timer.time()

        previous_block = copy.deepcopy(self.node.get_block('last'))
        data = list((self.node.mempool.copy().values()))
        state_var = previous_block.state.state_variables
        block = Block(len(self.node.chain), previous_block.compute_block_hash(), data, self.node.id, timestamp,
                      self.difficulty, previous_block.total_difficulty, nonce=randint(0,1000), state_var=state_var)

        while not self.flag.is_set():
            previous_block = copy.deepcopy(self.node.get_block('last'))
            self.update_block(block, previous_block)

            target_string = '1' * (256 - self.difficulty)
            target_string = target_string.zfill(256)

            hash_int = int(block.compute_block_hash(), 16)
            binary_hash = bin(hash_int)
            binary_hash = binary_hash[3:]

            if binary_hash > target_string:
                block.increase_nonce()

            else:
                self.node.chain.append(block)
                self.node.mempool.clear()
                logging.info(f"Block produced by Node {self.node.id}: ")
                logging.info(f"{repr(block)}")
                logging.info(f"###{block.state.state_variables}### \n")

    def stop(self):
        self.flag.set()

    def update_block(self, block, previous_block):
        """
        Updates the block by refreshing its information

        Args:
            block(Block): block in the production process
            previous_block(Block): the previous block in the chain, it should be the last block
        """
        block.height = previous_block.height + 1
        block.timestamp = self.timer.time()
        block.parent_hash = previous_block.hash
        block.total_difficulty = previous_block.total_difficulty + self.difficulty

        # Reset the state variables and apply them
        block.state.state_variables = previous_block.state.state_variables
        for transaction in block.data:
            block.state.apply_transaction(transaction)

        block.compute_block_hash()
