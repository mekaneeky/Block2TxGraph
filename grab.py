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



blockchain_info_url = "https://blockchain.info/block-height/"
blockchain_info_url_suffix = "?format=json"
block_file_header = "block_"

start_time = timer()


def exception_handler(request, exception):
    print(request, exception)
    #import pdb;pdb.set_trace()
    sleep(1)


def grab_blocks(start_number = 1, end_number = 1000,batch_size = 25):

    assert (end_number-start_number+1)%batch_size == 0

    received_blocks = []
    n_of_batches = ceil((end_number - start_number +1)/batch_size)

    for i in range(start_number, end_number + 1, batch_size):
        #for debugging purposes only
    #    if "pickled" in os.listdir():
    #        with open("pickled","rb") as me5alela:    
    #            received_blocks = pickle.load(me5alela)
    #        break
        
        print("Grabbing batch " + str((i-start_number+batch_size)//batch_size))
        #sleep(0.1)
        urls = [blockchain_info_url + str(i) + blockchain_info_url_suffix for i in range(i, i+batch_size)]
        rs = (grequests.get(u, stream = False, timeout = 60) for u in urls) #add timeout=60 in get
        batch_to_process = grequests.map(rs,size = batch_size, exception_handler=exception_handler)
        batch_to_append = []
        for process in batch_to_process:
            #import pdb; pdb.set_trace()
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
        
        received_blocks.append(batch_to_append)
            
        #print("Current batch " + str((i-start_number+batch_size)//batch_size))
        print("Blocks grabbed so far: " + str(len(received_blocks)))

    sleep(2)
    #import pdb;pdb.set_trace()
    #print(received_blocks)
    #Make recursive
    while True:
        #this is problematic since it will scan the good blocks as well
        bad_requests = []
        fixed_blocks = []
        #import pdb; pdb.set_trace()
        for batch in range(n_of_batches):
            for i in range(len(received_blocks[batch])):
                #try:
                if received_blocks[batch][i] == None:
                    #import pdb; pdb.set_trace()
                    bad_requests.append((batch)*batch_size + i + start_number+1)
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
        urls = [blockchain_info_url + str(i) + blockchain_info_url_suffix for i in bad_requests]

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
            batch_number = (bad_requests[fixed_block]-1-start_number) // batch_size
            if ((bad_requests[fixed_block]-start_number-1) // batch_size) == ((bad_requests[fixed_block]-start_number -1)/ batch_size):
                fixed_block_in_batch = 0
            else:
                fixed_block_in_batch = (( bad_requests[fixed_block]-start_number-1 )%batch_size)
            print("Block Number:"  + str(bad_requests[fixed_block]))
            print("Batch number: " + str(batch_number))
            print("Block's number in batch: " + str(fixed_block_in_batch))
            #import pdb;pdb.set_trace()
            received_blocks[batch_number][fixed_block_in_batch] = fixed_blocks[fixed_block]
            print(received_blocks[batch_number][fixed_block_in_batch])
        

    #        if received_blocks[block].status_code != 200:
    #            bad_requests.append(block)
    #            corrected_block = requests.get(blockchain_info_url + str(block) + blockchain_info_url_suffix)
    #            print("Correct block " + str(block))

        

    blocks_path = "blocks_parallel"


    if blocks_path in os.listdir():
        pass
    else:
        os.mkdir(blocks_path)

    os.chdir(blocks_path)
    for block in range(0,end_number-start_number+1):
        print(len(received_blocks))
        with open(block_file_header + str(block+start_number) + ".json", "w+") as block_file:
            print(block_file_header + str(block+start_number) + ".json")
            batch_number = (block) // batch_size
            print("batch number" ,batch_number)
            fixed_block_in_batch = ((block)%batch_size)
            print("pos in batch",fixed_block_in_batch)
            print(received_blocks[batch_number][fixed_block_in_batch])
            print(received_blocks[batch_number][fixed_block_in_batch]["blocks"][0]["height"])

            if received_blocks[batch_number][fixed_block_in_batch] == None:
                print("----------------------------------------------------")
                print("----------------------------------------------------")
                print("----------------------------------------------------")
                print("Failed bad block repair function")
                print("----------------------------------------------------")
                print("----------------------------------------------------")
                break
            json.dump(received_blocks[batch_number][fixed_block_in_batch], block_file)

    end_time = timer()
    print("Time take to download and write " +str(end_number-start_number+1) + 
        " blocks to memory is: " + str(end_time-start_time) )

for i in range(1,400000,1000):
    grab_blocks(i, i+1000-1, 25)