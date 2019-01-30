import json
import os
import networkx as nx
from natsort import natsorted

jackshit = [] 
#debugging list

#Sum of imputs needs to be collected and then distributed to output
# Have each input pass its value to the n outputs by sending each
# its value/n 
class Transaction():

    def __init__(self, transaction):
        self.n_inputs = transaction["vin_sz"]
        self.inputs = transaction["inputs"]
        self.input_addresses = {}
        self.n_outputs = transaction["vin_sz"]
        self.outputs = transaction["out"]
        self.output_addresses = {}
        self.input_sum = None
        self.output_sum = None
        self.possible_coinbase = False
        self.possible_change_addresses = set()
        self.jackshit = False
        self.tx_in_addr_set = set() #Used to verify change addresses
        self.transaction_fee = None
        self.tx_val_vectors = {}
        self.load_txs()

    def load_txs(self):
        for input_tx in self.inputs:
            try:
                self.input_addresses[input_tx["prev_out"]["addr"]] = input_tx["prev_out"]["value"]
                self.tx_in_addr_set.add(input_tx["prev_out"]["addr"])
                
                if self.input_sum == None:
                    self.input_sum = input_tx["prev_out"]["value"]
                else:
                    self.input_sum += input_tx["prev_out"]["value"]
            except:
                if self.n_inputs ==1 and self.n_outputs == 1: ##Write about this heuristic xD xD :D
                    self.possible_coinbase = True

                print("----------------WAAAAAAAAAAAAAAAAAAAAAA !")     
    
        for output_tx in self.outputs:
            try:
                if output_tx["addr"] in self.tx_in_addr_set:
                    self.possible_change_addresses.add(output_tx["addr"])

                self.output_addresses[output_tx["addr"]] = output_tx["value"]
                
                if self.output_sum == None:
                    self.output_sum = output_tx["value"]
                else:
                    self.output_sum += output_tx["value"]


                if self.possible_coinbase:
                    print("possible Coinbase with value " + str(output_tx["value"]) )
                    self.input_addresses["Coinbase"] = output_tx["value"]
        
        self.transaction_fee = self.input_sum - self.output_sum

            except KeyError:
                jackshit.append([self.inputs, self.outputs])
                self.jackshit = True
                print("----------------JAAAAAAAAAAAAAACK SHIT !")

               


class Block():
 
    def __init__(self, block):
        self.height = block["height"]
        self.n_of_transactions = block["n_tx"]
        self.main_chain = block["main_chain"]
        self.raw_transactions = block["tx"]
        self.transactions = []
        self.load_transactions()

    def load_transactions(self, ):
        for transaction in self.raw_transactions:
            print("--------Analyzing Transaction: " + str(transaction["tx_index"]))
            self.transactions.append( Transaction(transaction) )


class BlockChain():

    def __init__(self):
        self.dict_of_blocks = {}
        self.blocks_path = os.path.join( os.path.dirname(os.path.realpath(__file__)), "blocks_mlsl")
        self.blocks_file_list = os.listdir(self.blocks_path)
        self.load_blocks()


    def load_blocks(self):
        os.chdir(self.blocks_path)
        for batch_block_file in self.blocks_file_list:
            print("Processing Batch: " + str(batch_block_file))
            with open(batch_block_file) as batch_block_file_desc:
                batch = json.load(batch_block_file_desc)
                batch = batch[0]
            for block in batch:
                block, block_height = self.batch_analyzer(block)
                print("----Analyzing Block: " + str(block_height))
                self.dict_of_blocks[block_height] = Block(block)

    @staticmethod
    def batch_analyzer(block):
            block = block["blocks"][0]
            block_height = int(block["height"])
            return block, block_height        
            

class TxGraph():

    def __init__(self):
        self.bitcoin = BlockChain()
        self.tx_graph = nx.MultiDiGraph()
        
    def populate_graph(self):
        for block in self.bitcoin.dict_of_blocks.values():
            for transaction in block.transactions:
                self.tx_graph.

import pdb;pdb.set_trace()