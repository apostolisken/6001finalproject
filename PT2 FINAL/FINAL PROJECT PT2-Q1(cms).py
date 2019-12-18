import pandas as pd
import os
import psutil
process = psutil.Process(os.getpid())
import time
from probables import (CountMinSketch)
import sys

with open('data-streaming-project.data') as f:
    rows_number=sum(1 for line in f)

data_df=pd.read_csv('data-streaming-project.data',encoding='utf-8',delimiter='\t',names=['user','movie', 'rating', 'timestamp'],header=None)
df_movies=data_df['movie']    



cms=CountMinSketch(width=200,depth=21)

start_time2 = time.time()


counter = 0
for i in range(0,1126747):
    counter+=1
    cms.add(str(df_movies[i]))
    if counter%1000 == 0:
        print(sys.getsizeof(cms))

end_time2 = time.time()

cms.check('592')



print(sys.getsizeof(cms))

print("Total execution time: {}".format(end_time2 - start_time2))

