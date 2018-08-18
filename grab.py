import grequests
import requests
import os
import json

from math import ceil
from timeit import default_timer as timer
from time import sleep

#just for debugging
import pickle

start_number = 1
end_number = 500
received_blocks = []
batch_size = 50

assert (end_number-start_number+1)%batch_size == 0
n_of_batches = ceil((end_number - start_number +1)/batch_size)

blockchain_info_url = "https://blockchain.info/block-height/"
blockchain_info_url_suffix = "?format=json"
block_file_header = "block_"

start_time = timer()


for i in range(start_number, end_number + 1, batch_size):

    #for debugging purposes only
#    if "pickled" in os.listdir():
#        with open("pickled","rb") as me5alela:    
#            received_blocks = pickle.load(me5alela)
#        break

    print("Grabbing batch " + str((i+batch_size)//batch_size))
    sleep(0.1)
    urls = [blockchain_info_url + str(i) + blockchain_info_url_suffix for i in range(i, i+batch_size)]

    rs = (grequests.get(u, timeout=30) for u in urls)

    received_blocks.append(grequests.map(rs))



print(received_blocks)
#Make recursive
while True:
    #this is problematic since it will scan the good blocks as well
    bad_requests = []
    
    for batch in range(n_of_batches):
        for i in range(len(received_blocks[batch])):
            try:
                if received_blocks[batch][i].status_code != 200:
                    bad_requests.append((batch)*batch_size + i+1)
            except AttributeError:
                if received_blocks[batch][i] == None:
                    bad_requests.append((batch)*batch_size + i+1)
    if len(bad_requests) == 0:
        break
        
    print("Cleaning " + str(len(bad_requests)) + " bad requests")    
    urls = [blockchain_info_url + str(i) + blockchain_info_url_suffix for i in bad_requests]

    rs = (grequests.get(u, timeout = 30) for u in urls)
    fixed_blocks = grequests.map(rs)

    assert len(fixed_blocks) == len(bad_requests) #assure that the fixed blocks 
    #are the same number as the bad ones

    for fixed_block in range(len(bad_requests)):
        batch_number = (bad_requests[fixed_block]-1) // batch_size
        if ((bad_requests[fixed_block]-1) // batch_size) == ((bad_requests[fixed_block] -1)/ batch_size):
            fixed_block_in_batch = 0
        else:
            fixed_block_in_batch = (bad_requests[fixed_block]%batch_size) - 1
        print("Batch number: " + str(batch_number))
        print("Block's number in batch: " + str(fixed_block_in_batch))
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
for block in range(start_number,end_number+1):
    print(len(received_blocks))
    with open(block_file_header + str(block) + ".json", "w+") as block_file:
        batch_number = (block-1) // batch_size
        print("batch number" ,batch_number)
        fixed_block_in_batch = ((block-1)%batch_size)
        print("pos in batch",fixed_block_in_batch)
        print(received_blocks[batch_number][fixed_block_in_batch])
        if received_blocks[batch_number][fixed_block_in_batch].status_code != 200:
            print("----------------------------------------------------")
            print("----------------------------------------------------")
            print("----------------------------------------------------")
            print("Failed bad block repair function")
            print("----------------------------------------------------")
            print("----------------------------------------------------")
            break
        json.dump(received_blocks[batch_number][fixed_block_in_batch].json(), block_file)

end_time = timer()
print("Time take to download and write " +str(end_number-start_number+1) + 
    " blocks to memory is: " + str(end_time-start_time) )
