import pandas as pd
from more_itertools import sliced
import os
import psutil
process = psutil.Process(os.getpid())
import time
import hyperloglog

with open('data-streaming-project.data') as f:
    rows_number=sum(1 for line in f)

x=list(sliced((list(range(rows_number))),1000))    #slice the csv by 1000 lines counter

data_df=pd.read_csv('data-streaming-project.data',encoding='utf-8',delimiter='\t',names=['user','movie', 'rating', 'timestamp'],header=None)       

start_time = time.time()

#HyperLogLog method
start_time = time.time()

           

hll_users = hyperloglog.HyperLogLog(0.01)  # accept 1% counting error
hll_movies = hyperloglog.HyperLogLog(0.01)  # accept 1% counting error

for list in x:                                     #iteration through the lists in thee counter to get each 1000 lines
    for item in list:                              #iteration to the item in each list which represents each row number form the data dataframe
        hll_users.add(data_df.loc[item,'user'])
        hll_movies.add(data_df.loc[item,'movie'])

unique_users_hyper=len(hll_users)
unique_movies_hyper=len(hll_movies)

end_time = time.time()

print("The memory used is:",process.memory_info().rss,"bytes")  # in bytes 
print("Total execution time: {}".format(end_time - start_time))
