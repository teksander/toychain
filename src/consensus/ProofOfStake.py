import copy
import threading
from random import random


from toychain.src.Block import Block, State

import logging

from utils.helpers import compute_hash
logger = logging.getLogger('pos')

# Parameters for Proof-of-Stake
BLOCK_PERIOD = 100

# Default genesis block when argument is not passed when creating node
GENESIS_BLOCK = Block(0, 0000, [], 0, 0, 0, 0, nonce = 1, state = State())

class ProofOfStake:
    """
    Consensus protocol based on https://eips.ethereum.org/EIPS/eip-225
    """

    def __init__(self, genesis = GENESIS_BLOCK):
        self.genesis = genesis
        self.block_generation = VirtualProofOfStake

        # Boolean to check or not the block states
        self.trust = True

    def verify_chain(self, chain, previous_state):
        last_block = chain[0]
        if not self.verify_block(last_block, last_block):
            return False
        i = 1
        while i < len(chain):
            last_block_hash = last_block.compute_block_hash()

            # Check the block
            if not self.verify_block(chain[i], last_block):
                logger.error("Block error")
                logger.error(chain[i].__repr__())
                return False

            # Check the parent hash
            elif chain[i].parent_hash != last_block_hash:
                logger.error("Error in the blockchain")
                logger.error(chain[i].parent_hash + "###" + last_block_hash)
                return False
            else:
                last_block = chain[i]
            i += 1
        return True

    def verify_block(self, block, previous_block):
        # Verify signer was designated to forge the block
        designated_forger = self.get_forger(previous_block, block.timestamp)
        if not designated_forger or block.signer != designated_forger:
            logger.error("Invalid signer")
            return False

        # Verify block state
        if not self.trust:
            s = copy.deepcopy(previous_block.state)
            for transaction in block.data:
                s.apply_transaction(transaction)
            if s.state_hash != block.state.state_hash:
                logger.error(f"Invalid state {previous_block.state.state_variables}")
                logger.error(f"{s.state_variables}")
                logger.error(f"{block.state.state_variables}")
                logger.error(f"{block.data}")
                return False

        return True
        
    def get_forger(self, previous_block, timestamp):
        """
        Randomely choose a forger each BLOCK_PRIOD from the "lottery" (last block state variable) lots based on stake
        let him forge a new block
        """
        # lottery = lots of the nodes 
        lottery = previous_block.state.state_variables['lottery']
        
        # make sure chains with the same state choose the same  
        random.seed(previous_block.state.state_hash)
        
        # Calculate the number of missed blocks
        time_difference = timestamp - previous_block.timestamp
        missed_blocks = time_difference // BLOCK_PERIOD
        
        if missed_blocks == 0:
            return False
        
        # calculate the forger of the next block
        for i in range(missed_blocks):
            forger = random.choice(lottery)
        
        return forger
        
class VirtualProofOfStake():
    """
    Generates a block every X seconds
    """

    def __init__(self, node, period=BLOCK_PERIOD):

        self.node = node
        self.period = period
        self.timer = self.node.custom_timer

        self.flag = False
        self.sleep = 0
        
            
    def run(self):
        """"
        Randomely choose a forger each BLOCK_PRIOD from the "lottery" (last block state variable) lots based on stake
        let him forge a new block
        """
        last_block = copy.deepcopy(self.node.get_block('last'))
        next_block_number = last_block.height+1 
        timestamp = self.timer.get_time()
        
        forger = self.node.consensus.get_forger(last_block, timestamp)
        
        # Still in the Block Period of last block
        if not forger:
            return
        
        # let only the designated forger forge the block
        if self.node.enode == forger:

            # Get the current block, state and mempool
            previous_block = copy.deepcopy(self.node.get_block('last'))
            previous_state = previous_block.state
            mempool = list((self.node.mempool.copy().values()))

            # Filter out transactions already on the blockchain
            data = [tx for tx in mempool if tx.id not in self.node.previous_transactions_id]
            
            # Generate the new block
            block = Block(
                        next_block_number, 
                        previous_block.hash, 
                        data,
                        self.node.enode,
                        timestamp, 
                        1, 
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
        
class proofOfStakeThread(threading.Thread):
    """
    Generates a block every X seconds
    """

    def __init__(self, node, period=BLOCK_PERIOD):
        super().__init__()
        self.node = node
        self.period = period
        self.flag = threading.Event()

        self.timer = self.node.custom_timer

    def run(self):
        while not self.flag.is_set():
            last_block = copy.deepcopy(self.node.get_block('last'))
            next_block_number = last_block.height+1 
            timestamp = self.timer.get_time()
            
            forger = self.node.consensus.get_forger(last_block, timestamp)
            
            # Still in the Block Period of last block
            if not forger:
                continue
            
            # let only the designated forger forge the block
            if self.node.id == forger:

                # Get the current block, state and mempool
                previous_block = copy.deepcopy(self.node.get_block('last'))
                previous_state = previous_block.state
                mempool = list((self.node.mempool.copy().values()))

                # Filter out transactions already on the blockchain
                data = [tx for tx in mempool if tx.id not in self.node.previous_transactions_id]
                
                # Generate the new block
                block = Block(
                            next_block_number, 
                            previous_block.hash, 
                            data,
                            self.node.enode,
                            timestamp, 
                            1, 
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
                logger.info(f"###{block.state.state_variables}### \n")
        self.flag.set()