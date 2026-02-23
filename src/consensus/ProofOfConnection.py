import copy
import threading
import random 
import logging
from toychain.src.Block import Block
from toychain.src.State import Ledger as State
from toychain.src.utils.helpers import gen_enode

logger = logging.getLogger('poc')

# Parameters for Proof-of-Connection
BLOCK_PERIOD = 10
DELAY_NOTURN = 25

# Default genesis block when argument is not passed when creating node
auth_signers = [gen_enode(i) for i in range(1,26)]
GENESIS_BLOCK = Block(0, 0000, [], 0, 0, 0, 0, nonce = 1, state = State({'connectivity':{key: 0 for key in auth_signers} }))

class ProofOfConnection:
    """
    Consensus protocol based on https://eips.ethereum.org/EIPS/eip-225
    """

    def __init__(self, genesis = GENESIS_BLOCK):
        self.genesis = genesis
        self.block_generation = VirtualProofOfConnection
        # genesis.state.state_variable must contain a 'connectivity' list with valid enodes in it else no block will be produced.
        if not self.genesis.state.state_variables.get('connectivity'):
            raise ValueError("Genesis block must have a 'connectivity' state variable")

        # Boolean to check or not the block states
        self.trust = True

    def verify_chain(self, chain, previous_state):
        last_block = chain[0]
        if not self.verify_block(last_block, previous_state):
            return False

        i = 1
        while i < len(chain):
            last_block_hash = last_block.compute_block_hash()
            
            # Check Timestamp difference
            if chain[i].timestamp - last_block.timestamp < BLOCK_PERIOD // 2:
                logger.error("Timestamp error in the blockchain")
                logger.error(len(chain))
                logger.error(f"Previous: {last_block.timestamp}, Current: {chain[i].timestamp}")
                logger.error(chain)
                return False
            
            # Check the block (includes signer and difficulty verification)
            elif not self.verify_block(chain[i], last_block.state, last_block):
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

    def verify_block(self, block, previous_state, last_block=None):
        
        # Verify signer was designated to forge the block
        if last_block is not None:
            designated_forger = self.get_forger(last_block, block.timestamp)
            if not (block.miner_id == designated_forger or designated_forger == True):
                logger.error(f"Invalid signer {block.miner_id} instead of {designated_forger}")
                return False
            
            # Verify the difficulty
            expected_difficulty = self.get_difficulty(last_block, block.miner_id)
            if block.difficulty != expected_difficulty:
                logger.error(f"Invalid difficulty {block.difficulty} instead of {expected_difficulty}")
                return False
        
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
        choose the best connected aviable node as a forger each BLOCK_PERIOD. Let him forge a new block
        if the DELAY_NOTURN time has passed, let any node forge the block
        
        returns False (no forger), True (any node can forge) or the enode of the designated forger
        """
        # Calculate the number of missed blocks
        time_difference = timestamp - previous_block.timestamp
        
        # its not time to forge a new block yet
        if time_difference < BLOCK_PERIOD:
            return False
        
        # connectivity = lots of the nodes 
        connectivity = previous_block.state.state_variables['connectivity']
        # change the dict into a list of tuples for shuffling and sorting
        connectivity = list(connectivity.items())
        # make sure chains with the same state choose the same shuffle 
        random.seed(previous_block.hash)
        # shuffle to prevent stable sorting order for nodes with same connectivity
        random.shuffle(connectivity)
        # sort connectivity dict by descending connectivity value
        connectivity.sort(key=lambda item: item[1], reverse=True)
            
        # the preferred forger gets to forge the block
        if time_difference < DELAY_NOTURN:
            # calculate the forger of the next block
            return connectivity[0][0]
        
        # the preferred forger missed his chance, the next best connected node gets to forge the block
        if time_difference >= DELAY_NOTURN:
            return True
    
    def get_difficulty(self, previous_block, enode):
        """
        return the connectivity of the forger as difficulty
        """
        connectivity = previous_block.state.state_variables['connectivity'] 
        # Return False if enode is not in the connectivity dictionary (e.g., when forger == True and any node can forge)
        return connectivity.get(enode,-1)+1
        
class VirtualProofOfConnection():
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
        choose the best connected aviable node as a forger each BLOCK_PERIOD. Let him forge a new block
        """
        node = self.node
        last_block = copy.deepcopy(node.get_block('last'))
        next_block_number = last_block.height + 1 
        timestamp = self.timer.time()
        
        forger = node.consensus.get_forger(last_block, timestamp)
        
        # Still in the Block Period of last block
        if forger is False:
            return
        
        # No signing if already signed in last N/2+1 blocks
        connectivity = last_block.state.state_variables['connectivity']
        signer_count = len(connectivity)
        last_signed_block = node.get_last_signed_block()
            
        if last_signed_block == 0:
            pass
        elif next_block_number - last_signed_block < (signer_count + 1) // 2 + (signer_count + 1) % 2:
            return
        
        # let only the designated forger forge the block or if the designated forger missed his chance, let any node forge the block
        if node.enode == forger or forger == True:
            
            # calculate the difficulty
            difficulty = node.consensus.get_difficulty(last_block,node.enode)
            # check if there is a connectivity value for the node in the state variables, if not the node is not allowed to forge a block
            if difficulty == 0:
                #logger.error(f"Node {node.id} is not allowed to forge a block")
                return

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
            node.chain.append(block)
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
        
class proofOfConnectionThread(threading.Thread):
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
                self.node.chain.append(block)
                self.node.previous_transactions_id.update([tx.id for tx in block.data])
                self.node.mempool.clear()

                logger.info(f"Block produced by Node {self.node.id}: ")
                logger.info(f"{repr(block)}")
                logger.info(f"###{block.state.state_variables}### \n")
        
    def stop(self):
        self.flag.set()