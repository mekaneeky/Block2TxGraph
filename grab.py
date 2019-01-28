import grequests
import requests
import os, sys
import json

from math import ceil
from timeit import default_timer as timer
from time import sleep

#just for debugging
import pickle

#add metas
#refactor as functions
class grabber:
    def __init__(self,start_number = 100410, end_number = 336860, batch_size = 50, blocks_path = "C:\\Code\\Block2TxGraph\\blocks_2011_2014"):
        self.blockchain_info_url = "https://blockchain.info/block-height/"
        self.blockchain_info_url_suffix = "?format=json"
        self.block_file_header = "batch_"
        self.block_file_end = "blocks.json"
        self.blocks_path = blocks_path
        self.start_time = timer()
        self.end_time = None
        self.received = False
        self.fixed = False #flag to know whether grabbed blocks are fixed
        self.start_number = start_number
        self.end_number = end_number
        self.batch_size = batch_size
        self.n_of_batches = int(ceil((end_number+1 - start_number)/batch_size))
        self.received_blocks = {i:[] for i in range(1,1+self.n_of_batches+1)}


    #custom exception handler for grequests
    @staticmethod
    def exception_handler(request, exception):
        print(request, exception)
        #import pdb;pdb.set_trace()
        sleep(1)

    def grab_blocks(self):

        #assert (self.end_number-self.start_number+1)%self.batch_size == 0

        for i in range(1, 1 + self.n_of_batches):
            print("Grabbing batch " + str(i) )
            #sleep(0.1)
            urls = [self.blockchain_info_url + str(j) + self.blockchain_info_url_suffix for j in range(self.start_number + self.batch_size*(i-1), 
                self.start_number + self.batch_size*i) ]
            print("Starting at block: " + str(self.start_number + self.batch_size*(i-1)) )
            print("Ending at block: " + str(self.start_number + self.batch_size*(i)) )
            rs = (grequests.get(u, stream = False, timeout = 5) for u in urls) #add timeout=60 in get
            batch_to_process = grequests.map(rs,size = self.batch_size, exception_handler=self.exception_handler)
            batch_to_append = []
            for process in batch_to_process:
                #import pdb; pdb.set_trace()
                #integrate checks here and add an off switch for re-checks
                if process == None: ## add fuckup counter
                    batch_to_append.append(None)
                elif process.status_code != 200:
                    process.close()
                    batch_to_append.append(None)
                else:
                    try:
                        batch_to_append.append(process.json())
                        process.close()
                    except:
                        batch_to_append.append(None)
                        process.close()
            self.received_blocks[i].append(batch_to_append)
            #print("Current batch " + str((i-start_number+batch_size)//batch_size))
            print("Blocks grabbed so far: " + str( i* len(self.received_blocks[i][0] )))
            #import pdb; pdb.set_trace()
        self.received = True
        sleep(2)

#Make recursive

    def check_blocks(self):
        #assert len(received_blocks) == ceil(end_number-start_number/batch_size)

        while True:
            #this is problematic since it will scan the good blocks as well
            bad_requests = []
            fixed_blocks = []
            #import pdb; pdb.set_trace()
            for batch in range(1,self.n_of_batches+1):
                for i in range(len(self.received_blocks[batch][0])):
                    #try:
                    if self.received_blocks[batch][0][i] == None:
                        #import pdb; pdb.set_trace()
                        bad_requests.append((batch)*self.batch_size + i + self.start_number+1)
                    #except AttributeError:
                    #    if received_blocks[batch][i] == None:
                    #        bad_requests.append((batch)*batch_size + i+1)
                    #    else:
                    #        print("Weird Value")
                    #        print(received_blocks[batch][i])
                    #        sys.exit()
            if len(bad_requests) == 0:
                break
                
            print("Cleaning " + str(len(bad_requests)) + " bad requests")    
            urls = [self.blockchain_info_url + str(i) + self.blockchain_info_url_suffix for i in bad_requests]

            rs = (grequests.get(u, stream = False ,  timeout = 30) for u in urls)
            blocks_to_fix = grequests.map(rs)
            
            for process in blocks_to_fix:
                #import pdb; pdb.set_trace()
                try:
                    fixed_blocks.append(process.json())
                    process.close()

                except:
                    fixed_blocks.append(None)
            
            assert len(fixed_blocks) == len(bad_requests) #assure that the fixed blocks 
            #are the same number as the bad ones

            for fixed_block in range(len(bad_requests)):
                batch_number = (bad_requests[fixed_block]-1-self.start_number) // self.batch_size
                if ((bad_requests[fixed_block]-self.start_number-1) // self.batch_size) == ((bad_requests[fixed_block]-self.start_number -1)/ self.batch_size):
                    fixed_block_in_batch = 0
                else:
                    fixed_block_in_batch = (( bad_requests[fixed_block]-self.start_number-1 )%self.batch_size)
                print("Block Number:"  + str(bad_requests[fixed_block]))
                print("Batch number: " + str(batch_number))
                print("Block's number in batch: " + str(fixed_block_in_batch))
                #import pdb;pdb.set_trace()
                self.received_blocks[batch_number][0][fixed_block_in_batch] = fixed_blocks[fixed_block]
                print(self.received_blocks[batch_number][0][fixed_block_in_batch])
        
        self.fixed = True
    #        if received_blocks[block].status_code != 200:
    #            bad_requests.append(block)
    #            corrected_block = requests.get(blockchain_info_url + str(block) + blockchain_info_url_suffix)
    #            print("Correct block " + str(block))

    def write_blocks(self):        

        if self.received == True:
            pass
        else:
            return "Not received"
        if self.fixed == False:
            print("Warning: Blocks not checked")
        try:
            os.chdir(self.blocks_path)
        except FileNotFoundError:
            os.mkdir(self.blocks_path)
            os.chdir(self.blocks_path)

        for block_batch in range(1, self.n_of_batches+1):
            print(len(self.received_blocks))
            with open(self.block_file_header + str(block_batch*self.batch_size) + self.block_file_end, "w+") as block_file:
                #assert self.received_blocks[batch_number][fixed_block_in_batch]["blocks"][0]["height"] == block+start_number+1
                print(self.block_file_header + str(block_batch*self.batch_size) + self.block_file_end)
                print("batch number" ,block_batch)
                json.dump(self.received_blocks[block_batch], block_file)

"""                 if self.received_blocks[batch_number][fixed_block_in_batch] == None:
                    print("----------------------------------------------------")
                    print("----------------------------------------------------")
                    print("----------------------------------------------------")
                    print("Failed bad block repair function")
                    print("----------------------------------------------------")
                    print("----------------------------------------------------")
                    break
 """

        #print("Time taken to write " +str(self.end_number-self.start_number+1) + 
        #    " blocks to memory is: " + str(end_time-start_time) )
        # FIXME

test = grabber()
test.grab_blocks()
test.check_blocks()
test.write_blocks()
import pdb;pdb.set_trace()