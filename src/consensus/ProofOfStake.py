import copy
import threading
import random 
import logging
from toychain.src.Block import Block
from toychain.src.State import Ledger as State
from toychain.src.utils.helpers import gen_enode

logger = logging.getLogger('pos')

# Parameters for Proof-of-Stake
BLOCK_PERIOD = 10

# Default genesis block when argument is not passed when creating node
auth_signers = [gen_enode(i) for i in range(1,26)]
GENESIS_BLOCK = Block(0, 0000, [], 0, 0, 0, 0, nonce = 1, state = State({'lottery': auth_signers}))

class ProofOfStake:
    """
    Consensus protocol based on https://eips.ethereum.org/EIPS/eip-225
    """

    def __init__(self, genesis = GENESIS_BLOCK):
        self.genesis = genesis
        self.block_generation = VirtualProofOfStake
        # genesis.state.state_variable must contain a 'lottery' list with valid enodes in it else no block will be produced.
        if not self.genesis.state.state_variables.get('lottery'):
            raise ValueError("Genesis block must have a 'lottery' state variable")

        # Boolean to check or not the block states
        self.trust = True

    def verify_chain(self, chain, previous_state):
        last_block = chain[0]
        if not self.verify_block(last_block, previous_state):
            return False

        i = 1
        while i < len(chain):
            last_block_hash = last_block.compute_block_hash()
            # Verify signer was designated to forge the block
            designated_forger = self.get_forger(last_block, chain[i].timestamp)
            if not designated_forger or chain[i].miner_id != designated_forger:
                logger.error(f"Invalid signer {chain[i].miner_id} instead of {designated_forger}")
                return False

            # Verify the difficulty
            expected_difficulty = self.get_difficulty(last_block, chain[i].timestamp)
            if chain[i].difficulty != expected_difficulty:
                logger.error(f"Invalid difficulty {chain[i].difficulty} instead of {expected_difficulty}")
                return False
            
            # Check the block
            if not self.verify_block(chain[i], last_block.state):
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

    def verify_block(self, block, previous_state):
        
        # Verify block state
        if not self.trust:
            s = copy.deepcopy(previous_state)
            for transaction in block.data:
                s.apply_transaction(transaction)
            if s.state_hash != block.state.state_hash:
                logger.error(f"Invalid state {previous_state.state_variables}")
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
        random.seed(previous_block.hash)
        
        # Calculate the number of missed blocks
        time_difference = timestamp - previous_block.timestamp
        missed_blocks = time_difference // BLOCK_PERIOD
        
        if missed_blocks < 1:
            return False
        
        # calculate the forger of the next block
        for i in range(missed_blocks):
            forger = random.choice(lottery)
        
        return forger
    
    def get_difficulty(self, previous_block, timestamp):
        # Calculate the number of missed blocks
        time_difference = timestamp - previous_block.timestamp
        missed_blocks = time_difference // BLOCK_PERIOD
        
        # was not not time to forge a new block yet
        if missed_blocks < 1:
            return 0
        
        # fixed difficulty for PoS
        return 1
        
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
        node = self.node
        last_block = copy.deepcopy(node.get_block('last'))
        next_block_number = last_block.height + 1 
        timestamp = self.timer.time()
        
        forger = node.consensus.get_forger(last_block, timestamp)
        difficulty = node.consensus.get_difficulty(last_block, timestamp)
        
        # Still in the Block Period of last block
        if not forger:
            return
        
        # let only the designated forger forge the block
        if node.enode == forger:

            # Get the current block, state and mempool
            previous_block = copy.deepcopy(node.get_block('last'))
            previous_state = previous_block.state
            mempool = list((node.mempool.copy().values()))

            # Filter out transactions already on the blockchain
            data = [tx for tx in mempool if tx.id not in node.previous_transactions_id]
            
            # Generate the new block
            block = Block(
                        next_block_number, 
                        previous_block.hash, 
                        data,
                        node.enode,
                        timestamp, 
                        difficulty, 
                        previous_block.total_difficulty, 
                        state = previous_state)

            # Apply transactions to obtain the new state variables
            for transaction in block.data:
                block.state.apply_transaction(transaction, block)
            
            # Genrateblock reward for last block (except genesis block)    
            if block.height > 1:
                block.state.payout_block_reward(previous_block)

            # Update the blockchain and mempool
            node.add_block(block, source="local")
            node.previous_transactions_id.update([tx.id for tx in block.data])
            node.mempool.clear()

            logger.info(f"Block produced by Node {node.id}: ")
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
                
                # Genrateblock reward for last block (except genesis block)    
                if block.height > 1:
                    block.state.payout_block_reward(previous_block)

                # Update the blockchain and mempool
                self.node.add_block(block, source="local")
                self.node.previous_transactions_id.update([tx.id for tx in block.data])
                self.node.mempool.clear()

                logger.info(f"Block produced by Node {self.node.id}: ")
                logger.info(f"{repr(block)}")
                logger.info(f"###{block.state.state_variables}### \n")
        
    def stop(self):
        self.flag.set()
