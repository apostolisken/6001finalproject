import pandas as pd
from more_itertools import sliced
import os
import psutil
process = psutil.Process(os.getpid())
import time


with open('data-streaming-project.data') as f:
    rows_number=sum(1 for line in f)

x=list(sliced((list(range(rows_number))),1000))    #slice the csv by 1000 lines counter

data_df=pd.read_csv('data-streaming-project.data',encoding='utf-8',delimiter='\t',names=['user','movie', 'rating', 'timestamp'],header=None)       
user_df=data_df['user'].copy()
movie_df=data_df['movie'].copy()

#brute force method to count unique movies and users
start_time = time.time()


counter=[]
for lst in x:
    counter=counter+lst
    unique_users_df=user_df.iloc[counter].unique()
    unique_movies_df=movie_df.iloc[counter].unique()
        
        

unique_users_list=unique_users_df.tolist()
unique_movies_list=unique_movies_df.tolist()

#old method

#for list in x:                                     #iteration through the lists in the counter to get each 1000 lines
#    for item in list:                              #iteration to the item in each list which represents each row number form the data dataframe
#        if data_df.loc[item,'user'] not in unique_users_list:
#            unique_users_list.append(data_df.loc[item,'user'] )
#        if data_df.loc[item,'movie'] not in unique_movies_list:
#            unique_movies_list.append(data_df.loc[item,'movie'])

unique_users=len(unique_users_list)
unique_movies=len(unique_movies_list)


end_time = time.time()

print("The memory used is:",process.memory_info().rss,"bytes")  # in bytes 
print("Total execution time: {}".format(end_time - start_time))



