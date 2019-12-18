import numpy as np
import pandas as pd
import time

start_time = time.time()

def prioritySampling (stream, sampleSize):
    """Sampling method 3: Priority Sampling with sampling window
        'stream' is the stream to sample from
        'sampleSize'is the size of the sample
    """
    
    sample=np.zeros(sampleSize)
    tags=np.zeros(sampleSize)
    
    i=0
    j=0
    while i<len(stream):
        if i<sampleSize:
            sample[i] = stream[i]
            tags[i] = np.random.random()
        else:
            newTag = np.random.random()
            maxTag = np.max(tags)
            idxMaxTag = np.argmax(tags)
            
            if maxTag > newTag:
                sample[idxMaxTag]=stream[i]
                tags[idxMaxTag]=newTag
                j=j+1
        i=i+1
    print ('updates priority=',j)    
    return sample



def reservoirSampling (stream, sampleSize):
    """Sampling method 2: Reservoir sampling
    'stream' is the stream to sample from
    'sampleSize' is the sample szie
    returns the 'sample' as a list
    """    

    sample=[]
    i=0
    while i<len(stream):
        #store the first 'sampleSize' elements in the sample
        if i<sampleSize:
            sample.append(stream[i])
        else:
            #keep the new item
            if np.random.random() <= sampleSize/float(i):
                oldItemIdx=np.random.randint(1,sampleSize)
                #discards randomply an existing item
                sample[oldItemIdx]=stream[i]
        
        i=i+1
    return sample



data_df=pd.read_csv('data-streaming-project.data',encoding='utf-8',delimiter='\t',names=['user','movie', 'rating', 'timestamp'],header=None)       
data_df=data_df.drop('rating', axis=1)
data_df=data_df.drop('timestamp', axis=1)


data_movies_df=data_df['movie'].copy()
data_users_df=data_df['user'].copy()

samlingWindowSize=1000


#frequency of movies

samplePM = prioritySampling (data_movies_df, samlingWindowSize)
p1_movies =pd.DataFrame(samplePM).rename(columns={0:'movie'})
p1_movies_top_10=p1_movies['movie'].value_counts() #top 10 movies of priority sampling


sampleRM = reservoirSampling (data_movies_df, samlingWindowSize)
r1_movies = pd.DataFrame(sampleRM).rename(columns={0:'movie'})
r1_movies_top_10=r1_movies['movie'].value_counts() #top 10 movies of reservoir sampling


act_freq_movie_df = data_movies_df.value_counts() #actual frequency of movies


#frequency of users

samplePU = prioritySampling (data_users_df, samlingWindowSize)
p1_users =pd.DataFrame(samplePU).rename(columns={0:'user'})
p1_users_top_10=p1_users['user'].value_counts() #top 10 users of priority sampling


sampleRU = reservoirSampling (data_users_df, samlingWindowSize)
r1_users = pd.DataFrame(sampleRU).rename(columns={0:'user'})
r1_users_top_10=r1_users['user'].value_counts() #top 10 users of reservoir sampling


act_freq_user_df = data_users_df.value_counts() #actual frequency of users

end_time = time.time()
print("Total execution time: {}".format(end_time - start_time))




