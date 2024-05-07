from random import randint
from json import loads as jsload
from copy import copy
from toychain.src.utils import compute_hash, transaction_to_dict
from os import environ

import logging
logger = logging.getLogger('sc')

class Block:
    """
    Class representing a block of a blockchain containing transactions
    """

    def __init__(self, height, parent_hash, data, miner_id, timestamp, difficulty, total_diff, nonce=None,
                 state_var=None, state = None):
        self.height = height
        self.number = height
        self.parent_hash = parent_hash
        self.data = data
        self.miner_id = miner_id
        self.timestamp = timestamp
        self.difficulty = difficulty
        self.total_difficulty = total_diff + difficulty

        if state:
            self.state = state
        else:
            self.state = State(state_var)

        self.nonce = nonce
        if nonce is None:
            self.nonce = randint(0, 1000)

        self.transactions_root = self.transactions_hash()
        self.hash = self.compute_block_hash()

    def compute_block_hash(self):
        """
        computes the hash of the block header
        :return: hash of the block
        """
        _list = [self.height, self.parent_hash, self.transactions_hash(), self.miner_id, self.timestamp,
                 self.difficulty,
                 self.total_difficulty, self.nonce]

        self.hash = compute_hash(_list)

        return self.hash

    def transactions_hash(self):
        """
        computes the hash of the block transactions
        :return: the hash of the transaction list
        """
        transaction_list = [transaction_to_dict(t) for t in self.data]
        self.transactions_root = compute_hash(transaction_list)
        return self.transactions_root

    def get_header_hash(self):
        header = [self.parent_hash, self.transactions_hash(), self.timestamp, self.difficulty, self.nonce]
        return compute_hash(header)

    def increase_nonce(self):  ###### POW
        self.nonce += 1

    def __repr__(self):
        """
        Translate the block object in a string object
        """
        return f"## H: {self.height}, D: {self.difficulty}, TD: {self.total_difficulty}, P: {self.miner_id}, BH: {self.hash[0:5]}, TS:{self.timestamp}, #T:{len(self.data)}, SH:{self.state.state_hash[0:5]}##"


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

class State(StateMixin):

    def __init__(self, state_variables = None):

        if state_variables is not None:
            for var, value in state_variables.items(): setattr(self, var, value)     

        else:
            self.private     = {}
            self.n           = 0
            self.balances    = {}

            # Init your own state variables
            self.patches     = []
            self.robots      = {}


    def robot(self):
        return {'task': -1}
    
    def patch(self, x, y, qtty, util, qlty, json):
        return {
            'x': x,
            'y': y,
            'qtty': qtty,
            'util': util,
            'qlty': qlty,
            'json': json,
            'id': len(self.patches),
            'maxw': int(environ['MAXWORKERS']),
            'totw': 0,
            'last_assign': -1,
            'epoch': self.epoch(0,0,[],[],[],self.linearDemand(0)),
            'robots': [],
            'allepochs': []
        }
    
    def epoch(self, number, start, Q, TC, ATC, price):
        return {
            'number': number,
            'start': start,
            'Q': Q,
            'TC': TC,
            'ATC': ATC,
            'price': price,
            'robots': [],
            'TQ': 0,
            'AATC': 0,
            'AP': 0
        }

    def register(self, task = -1):

        if self.msg.sender not in self.robots:
            self.robots[self.msg.sender] = self.robot()

        if task != -1:
            self.joinPatchFixed(task)

    def updatePatch(self, x, y, qtty, util, qlty, json):
        # x, y, qtty, util, qlty, json, id, maxw, totw, last_assign, epoch, robots, allepochs

        i, _ = self.findByPos(x, y)

        if i < 9999:
            self.patches[i]["qtty"] = qtty
            self.patches[i]["util"] = util
            self.patches[i]["qlty"] = qlty
            self.patches[i]["json"] = json

        else:
            new_patch = self.patch(x, y, qtty, util, qlty, json)
            self.patches.append(new_patch)

    def dropResource(self, x, y, qtty, util, qlty, json, Q, TC):
        
        i, _ = self.findByPos(x, y)

        if i < 9999:

            # Update patch information
            self.updatePatch(x, y, qtty, util, qlty, json)

            # Pay the robot
            self.balances[self.msg.sender] += Q*util*self.patches[i]['epoch']['price']

            # Fuel purchase
            self.balances[self.msg.sender] -= TC
            
            self.patches[i]['epoch']['Q'].append(Q)
            self.patches[i]['epoch']['TC'].append(TC)
            self.patches[i]['epoch']['ATC'].append(TC/Q)
            self.patches[i]['epoch']['robots'].append(self.msg.sender)

            logger.info(f"Drop #{len(self.patches[i]['epoch']['Q'])}/{self.patches[i]['totw']} @ Epoch #{self.patches[i]['epoch']['number']}")

            if len(self.patches[i]['epoch']['Q']) >= self.patches[i]['totw']:

                self.patches[i]['epoch']['TQ'] = sum(self.patches[i]['epoch']['Q'])
                self.patches[i]['epoch']['AATC'] = sum(self.patches[i]['epoch']['ATC'])/len(self.patches[i]['epoch']['ATC'])
                self.patches[i]['epoch']['AP']   = self.patches[i]['util'] * self.patches[i]['epoch']['price'] - self.patches[i]['epoch']['AATC']
 
                old_epoch  = copy(self.patches[i]['epoch'])
                new_price  = self.linearDemand(old_epoch['TQ'])

                self.patches[i]['allepochs'].append(old_epoch)
                # self.private['last_epoch'] = (i, old_epoch)

                # Init new epoch
                logger.info(f"New epoch #{self.patches[i]['epoch']['number']+1} started")
                self.patches[i]['epoch'] = self.epoch(old_epoch['number']+1, self.block.height, [], [], [], new_price)
                
        else:
            print(f'Patch {x},{y} not found')

    def assignPatch(self): # Assign the first availiable patch to a robot
        for i, patch in enumerate(self.patches):
            if patch['totw'] < patch['maxw'] and patch['epoch']['number']>patch['last_assign']:
                self.robots[self.msg.sender]['task'] = patch['id']
                self.patches[i]["totw"] += 1
                self.patches[i]["last_assign"] = patch['epoch']['number']

    def joinPatchFixed(self, i): # Robot joins a specific patch

        patch = self.patches[i]

        print(f"fixed joining {patch['epoch']['number']} {patch['last_assign']}")

        self.robots[self.msg.sender]['task'] = self.patches[i]['id']
        self.patches[i]["totw"] += 1
        self.patches[i]["last_assign"] = patch['epoch']['number']

    def joinPatch(self, x, y): # Robot joins a specific patch

        i, patch = self.findByPos(x, y)

        print(f"joining {patch['epoch']['number']} {patch['last_assign']}")
        if patch and patch['totw'] < patch['maxw'] and patch['epoch']['number']>patch['last_assign']:
            self.robots[self.msg.sender]['task'] = self.patches[i]['id']
            self.patches[i]["totw"] += 1
            self.patches[i]["last_assign"] = patch['epoch']['number']

    def leavePatch(self): # Robot leave his current patch
        i = self.robots[self.msg.sender]['task']
        if self.patches[i]['epoch']['number']>self.patches[i]['last_assign']:
            self.robots[self.msg.sender]['task'] = -1
            self.patches[i]["totw"] -= 1
            self.patches[i]["last_assign"] = self.patches[i]['epoch']['number']
            
    def getPatches(self):
       return self.patches

    def getMyPatch(self, id):
        if id not in self.robots:
            return None
        
        if self.robots[id]['task'] == -1:
            return None
        
        return self.patches[self.robots[id]['task']]
        
    def getAvailiable(self):
        for i, patch in enumerate(self.patches):
            if patch['totw'] < patch['maxw'] and patch['epoch']['number']>patch['last_assign']:
                return patch
        return False

    def findByPos(self, _x, _y):
        for i in range(len(self.patches)):
            if _x == self.patches[i]['x'] and _y == self.patches[i]['y']:
                return i, self.patches[i]
        return 9999, None

    # def getEpochs(self, id):
    #     my_patch = self.patches[self.robots[id]['task']]
    #     if my_patch:
    #         return my_patch['allepochs']

    # def getEpoch(self, index = 'latest'):
    #     if index == 'latest' and self.patches:
    #         last_patch = max(self.patches, key=lambda patch: max((epoch['start'] for epoch in patch['allepochs']), default=0), default=None)
            
    #         if last_patch and last_patch['allepochs']:
    #             last_epoch = max(last_patch['allepochs'], key=lambda epoch: epoch['start'])
    #             return last_epoch, last_patch
    #         else:
    #             return None, None
    #     else:
    #         return None, None
    
    def getEpoch(self, index = 'latest'):
        if index == 'latest' and self.patches:
            last_patch = max(self.patches, key=lambda patch: max((epoch['start'] for epoch in patch['allepochs']), default=0))
            
            if last_patch and last_patch['allepochs']:
                last_epoch = max(last_patch['allepochs'], key=lambda epoch: epoch['start'])
                return last_epoch, last_patch
            else:
                return None, None
        else:
            return None, None

    def getAllEpochs(self):
        return {patch['id']: patch['allepochs'] for patch in self.patches}

    def linearDemand(self, Q):
        P = 0
        demandA = 0 
        demandB = 1
        
        if demandB > demandA * Q:
            P = demandB - demandA * Q
        return P