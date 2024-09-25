from toychain.src.utils.helpers import compute_hash

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