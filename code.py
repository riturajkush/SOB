import pandas as pd
from collections import defaultdict


txs = pd.read_csv('mempool.csv')                                    #   reading csv file with pandas library
txs = txs.fillna("")                                                #   filling missing values with empty string
txs = txs.sort_values('parents ', ascending=False)                  #   sorting the transactions on basis of presence of parents
txs = txs.values.tolist()                                           #   converting to list from dataframe

parents_list = []                                                   # parents_list will store transactions which have child transaction that is dependent transaction
for tx in txs:                                                      
    if tx[3]!='':
        parents = list(tx[3].split(';'))
        for parent in parents:
            parents_list.append(parent)

non_parent = []                                                     # non_parent list will store the transaction that are fully independent 
for tx in txs:
    if tx[0] not in parents_list:
        non_parent.append(tx[0])

ds = []                                                             # creating a data structure which is list of stacks to store the family of dependent transactions
visited = []
for tx in txs:
    if tx[0] not in visited and tx[0] in non_parent:
        if tx[3]!='':
            visited.append(tx[0])
            stk = [tx[0]]
            curr = tx
            stk_par = []
            parents = list(curr[3].split(';'))
            stk_par += parents
            while len(stk_par)!=0:
                curr = stk_par.pop(0)
                for tx_id in txs:
                    if tx_id[0]==curr and tx_id[0] not in visited:
                        visited.append(tx_id[0])
                        if tx_id[3]=='':
                            stk.append(tx_id[0])
                        else:
                            stk.append(tx_id[0])
                            stk_par += list(tx_id[3].split(';'))
            ds.append(stk)

fee_sort = []                                                        # sorting of transactions on basis of maximum fee value and assigning the flag -1 to independent transactions     
for tx in txs:                                                       # and assigning the flag 1 which are part of a family of transactions
    if tx[0] in non_parent and tx[3]=='':
        fee_sort.append([tx[1],-1,tx[0]])
    else:
        fee_sort.append([tx[1],1,tx[0]])
fee_sort = sorted(fee_sort, reverse=True)

sorted_txs = []                                                      # sorting the transactions on basis of maximum fees, keeping the parent transactions above the dependent transactions
inserted_txs= []
for tx in fee_sort:
    if tx[1]==-1:
        sorted_txs.append(tx[2])
    elif tx[1]!=-1 and tx[2] not in inserted_txs:
        for fam in ds:
            if tx[2] in fam:
                for pointer in range(len(fam)):
                    if fam[pointer]==tx[2]:
                        break
                count = len(fam)-pointer
                while(count!=0):
                    val = fam.pop()
                    inserted_txs.append(val)
                    sorted_txs.append(val)
                    count -= 1

with open('block.txt', mode='wt', encoding='utf-8') as myfile:                     # storing output as block.txt
    myfile.write('\n'.join(sorted_txs))  