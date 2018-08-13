""" Here is where we grab the block data from blockchain.info
"""
import networkx as nx
import requests
import json
import os
from time import sleep, time, gmtime
import sys
import re
from natsort import natsorted

# Note to self: please remember that the input values are spent completely or
# returned to the change address ! 
# 
# Note 2: Block 171000 contains multiple inputs from the same address to an output
# 
# Note 3: Block 546 contains a tx where there are 2 different inputs and one of them 
# is the two outputs? 


def grab_block(start_number = 0, end_number = 70000, blocks_path = os.path.dirname(os.path.realpath(__file__)), overwrite = True, continuation = True ):#should I add a store boolean?

    #Make sure not to set a start value explicitly if you're continuing
    #assert ((continuation == True and start_number == 0) or (continuation == False))
    
    blockchain_info_url = "https://blockchain.info/block-height/"
    blockchain_info_url_suffix = "?format=json"
    block_file_header = "block_"
    meta = {}
    meta["current_block"] = 0
    meta["retries"] = 0
    meta["attempts"] = []
    #Move to the specified directory
    os.chdir(blocks_path)
    if not ("blocks" in os.listdir()):
        os.mkdir("blocks")
    os.chdir("blocks")

    if continuation == True:
        if "meta.json" in os.listdir():
            with open("meta.json") as json_meta_file:
                meta = json.load(json_meta_file)
    
    start_number = meta["current_block"]+1
    #meta["attempts"].append(len(meta["attempts"])+1, start_number, gmtime(time())) 
    #fix tomorrow

    for block in range(start_number, end_number+1):
        if overwrite == True and ((block_file_header + str(block) + ".json") in os.listdir()):
            print("Block " + str(block) + " will be overwritten")
        elif overwrite == False and ((block_file_header + str(block) + ".json") in os.listdir()):
            print("Block " + str(block) + " already exists")
            continue
        print("Grabbing block: " + str(block))
        #Grab the json block data
        while True:
            #req_response = [] # holder for variable
            try:
                req_response = requests.get(blockchain_info_url + str(block) + blockchain_info_url_suffix)
                #import pdb;pdb.set_trace()
                #Add a timeout setting with a re-request loop
                # check if the requests.get has a timeout thing
                # If not then use another (maybe urllib.get?)
                # or edit requests's source
            except:
                sleep(1)
                print("hiccup in requests.get")
                continue
            if req_response.status_code == 200:
                break
            else:
                print("Status not 200, retrying....")
                meta["retries"] += 1
                sleep(1)
        block_json = req_response.json()

        #Write block to file
        with open(block_file_header + str(block) + ".json", "w+") as block_file:
            json.dump(block_json, block_file)

        #write current block to meta_file
        meta["current_block"] = block
        with open("meta.json", "w+") as json_meta_file:
            json.dump(meta, json_meta_file)


def generate_graph(graph = "new", blocks_path = os.path.dirname(os.path.realpath(__file__)), continuation = True):
    
    # Add the ability to split a number of blocks into a many graphs then combine to save
    # memory and prevent hangups

    blocks_added = []    # list of added blocks for the graph_meta.json
    block_file_list = [] # file, so that added blocks are ignored
                         # 

    os.chdir(blocks_path + "/blocks")
    if ("meta.json" in os.listdir()):
        print(str(len(os.listdir())-1) + " Blocks Found")
        block_file_list = natsorted(os.listdir())
        print("Popped" ,block_file_list.pop(), block_file_list.pop()) #popping the meta.json file

    else:
        print(str(len(os.listdir())) + "Blocks Found")


    if graph == "new":
        graph = nx.MultiDiGraph()
        graph.add_node("Coinbase")
        graph_meta = {}
        graph_meta["blocks_added"] = blocks_added
        with open("graph_meta.json", "w+") as graph_meta_file:
            json.dump(graph_meta, graph_meta_file)
    else:
        with open("graph_meta.json") as graph_meta_file:
            graph_meta = json.load(graph_meta_file)
            blocks_added = graph_meta["blocks_added"]

    for block_file2open in block_file_list:
        
        # Make sure to remove the blocks from memory every X blocks

        block_number = block_file2open.split('_')[1].split('.')[0]

        #if graph != "new" and (block_number in blocks_added): # If block is already added  
        #    continue                                          # and we aren't creating a
        # reimplemented                                        # new graph, then the 
                                                              # 
                                                              # 
                                                              # 
                                                            
        print("Graphing block " + block_number)

        with open(block_file2open) as block_file:
            current_block_file = json.load(block_file)
            current_ntxs = current_block_file['blocks'][0]['n_tx']
            current_block = current_block_file['blocks'][0]["height"]
            
            current_input_address = None
            current_input_value = None
            current_output_address = None
            current_output_value = None

            if (current_block in blocks_added) and (continuation == True):
                continue


            for tx in range(current_ntxs): # Add support for coinbase txs,
                                            # Find out what to do with segregated
                                            # witness (are there other weird tx 
                                            # types ?)

            #note to self: nx doesn't add duplicate nodes
                current_transaction = current_block_file['blocks'][0]["tx"][tx]
                inputs = current_block_file['blocks'][0]["tx"][tx]["inputs"]
                outputs = current_block_file['blocks'][0]["tx"][tx]["out"]
                n_of_inputs = int(current_block_file['blocks'][0]["tx"][tx]["vin_sz"]) #faster than len?
                n_of_outputs = int(current_block_file['blocks'][0]["tx"][tx]["vout_sz"]) #faster than len?
                # it is worse because all changes in inputs need to be manually adjusted
                
                is_coinbase = False
 
                input_vals = {}
                output_vals = {}
                tx_vals_temp = []
                tx_vals = []

                for inputx in range(len(inputs)):
                    try:
                        
                        current_input_address = inputs[inputx]["prev_out"]["addr"]
                        current_input_value = inputs[inputx]["prev_out"]["value"]
                        input_vals[current_input_address] = current_input_value
                        graph.add_node(current_input_address)
                    
                    except:
                        
                        if (n_of_inputs == 1) and (n_of_outputs == 1):
                        
                            #input_vals["Coinbase"] = outputs[0]["value"] #--> which is output value
                            is_coinbase = True
                            graph.add_weighted_edges_from([("Coinbase",outputs[0]["addr"] ,outputs[0]["value"])])
                        
                        else:
                            continue                                     # Add checks for values of difficulty vs reward 
                                                                         # for checking authenticity and to catch simmilar txs that
                                                                         # are not Coinbase
                        
                         # No need to add Coinbase multiple times
                                # already added when creating graph
                    # only add inputs here then add outputs and edges together 
                    # this seems to be rather inefficient O(n^2)
                    # Be careful not to assume seg witness et al are
                    # coinbase just because there might not be an input address
                    # or an input value

                    #Adding input address to graph, doesn't duplicate
                    

                    if is_coinbase == True: #Then skip output loop because we already added the Tx
                        break

                    for out in range(len(outputs)):
                        current_output_address = outputs[out]["addr"]
                        current_output_value = outputs[out]["value"]

                        if current_output_address in input_vals: # remove from output list and remove the returned 
                                                                # change value from the transaction
                            input_vals[current_output_address] -= current_output_value #Removed change from tx
                            n_of_outputs -= 1 #coinbase txs can still be recorded 
                            continue #don't add to output_vals and later to edges
                        
                        else: #If output is not a change tx, add node then generate edges
                            graph.add_node(current_output_address)
                            output_vals[current_output_address] = current_output_value
                            tx_vals_temp.append([current_input_address,current_output_address, None])

                    if n_of_outputs <= 0: # If it is one of the weird reorganizing txs
                        continue          # then skip (refer to block 546)
                    current_input_value = current_input_value/n_of_outputs
                    
                    for tx_tuple in tx_vals:
                        tx_tuple[2] = current_input_value
                        tx_tuple = tuple(tx_tuple)
                        tx_vals.append(tx_tuple)

                    graph.add_weighted_edges_from(tx_vals)


        print("-----Nodes in graph: " + str(graph.number_of_nodes()))
        print("-----Edges in graph: " + str(graph.number_of_edges()))

            #The edge generating loop O(n^2)
            
            #Testing whether a cartesian prodcut would work for the outputs and inputs
            # and whether their values match up
  #          for input_address, input_value in input_vals.items():
 #               input_value = input_value/n_of_outputs #averaged out output
#                for output_address, output_value in output_vals.items():
#                    graph.add_weighted_edge()
                                

                            
            ## Don't forget to add the graph_meta.json updates and checks 


    

#test code
#grab_block(start_number = 1, end_number = 4, overwrite = True, continuation = False)
#grab_block(start_number = 1, end_number = 4, overwrite = False, continuation = False)
#grab_block(start_number = 1, end_number = 4, overwrite = True, continuation = True)
generate_graph()
grab_block(start_number = 1, end_number = 30000, overwrite = False, continuation = True)
