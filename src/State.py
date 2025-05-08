from toychain.src.utils.helpers import compute_hash,enode_to_id

import logging
logger = logging.getLogger('sc')

class StateMixin:
    @property
    def getBalances(self):
        return self.balances
    
    @property
    def getN(self):
        return self.n
        
    @property
    def call(self):
        return None
    
    @property
    def state_variables(self):
        return {k: v for k, v in vars(self).items() if not (k.startswith('_') or k == 'msg' or k == 'block' or k == 'private')}
    
    @property
    def state(self):
        return {k: v for k, v in vars(self).items() if not (k.startswith('_') or k == 'msg' or k == 'block' or k == 'private')}

    @property
    def state_hash(self):
        return compute_hash(self.state.values())
    
    def apply_transaction(self, tx, block):
        self.msg = tx
        self.block = block

        # Initialize funds of unused addresses
        self.balances.setdefault(tx.sender, 0)
        self.balances.setdefault(tx.receiver, 0)

        # Check sender funds
        if tx.value and self.balances[tx.sender] < tx.value:
            logger.info("Insufficient Balance")
            return
        
        # Apply the transfer of funds
        self.balances[tx.sender] -= tx.value
        self.balances[tx.receiver] += tx.value
        
        # Increment the transaction counter
        self.n += 1

        # Apply the other functions contained in data
        if tx.data and 'function' in tx.data and 'inputs' in tx.data:
            function = getattr(self, tx.data.get("function"))
            inputs   = tx.data.get("inputs")
            try:
                function(*inputs)
            except Exception as e:
                raise e
    
    def payout_block_reward(self,block):
        # Check if the block reward is defined
        if not hasattr(self, 'get_block_reward'):
            return
        
        node_id = str(enode_to_id(block.miner_id))
        # Initialize funds of unused addresses
        self.balances.setdefault(node_id, 0)
        # Apply the block reward
        self.balances[node_id] += self.get_block_reward(block)
        # Increment the transaction counter
        self.n += 1

class Ledger(StateMixin):

    def __init__(self, state_variables = None):

        if state_variables is not None:
            for var, value in state_variables.items(): setattr(self, var, value)     

        else:
            # Init the basic state variables
            self.private     = {}
            self.n           = 0
            self.balances    = {}
