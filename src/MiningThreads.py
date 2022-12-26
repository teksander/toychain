
import threading
from random import randint
from time import sleep, time

from PROJH402.src import constants
from PROJH402.src.constants import *
from PROJH402.src.Block import Block


class MiningThread(threading.Thread):
    """
    Thread class that generate blocks that answer to the consensus rules
    This block generation is done according to the proof of work
    """

    def __init__(self, node):
        super().__init__()
        self.node = node
        self.difficulty = node.difficulty

        self.flag = threading.Event()

    def run(self):
        """
        Increase the nonce until the hash of the block has the expected number of zeros at the front of the hash
        """
        timestamp = time()

        block = Block(len(self.node.chain), self.node.get_block('last').hash, self.node.mem_pool.copy(), self.node.id,
                      timestamp, self.difficulty, self.node.get_block('last').total_difficulty, randint(0, 1000))

        while not self.flag.is_set():
            last_block = self.node.get_block("last")
            block.update(last_block.height+1, last_block.hash, last_block.data, self.node.difficulty,
                         last_block.total_difficulty + self.node.difficulty)

            if block.compute_hash()[:self.difficulty] != "0" * self.difficulty:
                block.increase_nonce()

            else:
                self.node.chain.append(block)
                self.node.mem_pool.clear()
                if constants.DEBUG:
                    print("Block added: " + str(block.compute_hash()))
                    print(repr(block) + "\n")

                block = Block(len(self.node.chain), self.node.get_block('last').hash, self.node.mem_pool.copy(),
                              self.node.id, timestamp, self.difficulty, self.node.get_block('last').total_difficulty,
                              randint(0, 1000))

    def stop(self):
        self.flag.set()


class ProofOfAuthThread(threading.Thread):
    """
    Generates a block every X seconds
    """

    def __init__(self, node, time=10):
        super().__init__()
        self.node = node
        self.time = time
        self.flag = threading.Event()

        self.difficulty = node.difficulty

    def run(self):
        while not self.flag.is_set():
            sleep(self.time)
            timestamp = time()
            block = Block(len(self.node.chain), self.node.get_block('last').hash, self.node.mem_pool.copy(),
                          self.node.id, timestamp, self.difficulty, self.node.get_block('last').total_difficulty)
            self.node.chain.append(block)
            self.node.mem_pool.clear()
            print("Block added: " + str(block.compute_hash()))
            print(repr(block) + "\n")
