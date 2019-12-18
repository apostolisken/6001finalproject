import pandas as pd
import os
import psutil
process = psutil.Process(os.getpid())
import time
import sys

with open('data-streaming-project.data') as f:
    rows_number=sum(1 for line in f)

data_df=pd.read_csv('data-streaming-project.data',encoding='utf-8',delimiter='\t',names=['user','movie', 'rating', 'timestamp'],header=None) 

df_movies=data_df['movie']

start_time = time.time()
#split method

counter = 0
for i in range(0,1126747):
    counter+=1
    if counter%1000 == 0:
        df_movies[:counter].value_counts().to_csv('movie_counter1.csv', header = False)


end_time = time.time()

print(sys.getsizeof(df_movies))

print("Total execution time: {}".format(end_time - start_time))
