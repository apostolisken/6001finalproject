# -*- coding: utf-8 -*-
"""
Created on Sat Dec 14 18:54:41 2019

@author: Teoi
"""

import pandas as pd
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

#create tags dataframe
tags_df=pd.read_csv('tags.dat',encoding='ISO-8859–1',delimiter='\t')

#create user_friends dataframe
user_friends_df=pd.read_csv('user_friends.dat',encoding='ISO-8859–1',delimiter='\t')

#create user_taggedartists dataframe
user_taggedartists_df=pd.read_csv('user_taggedartists.dat',encoding='ISO-8859–1',delimiter='\t')

#create user_taggedartists-timestamps dataframe#create artists dataframe
user_taggedartists_timestamps_df=pd.read_csv('user_taggedartists-timestamps.dat',encoding='ISO-8859–1',delimiter='\t')

#create artists dataframe
artists = pd.read_csv('artists.dat',encoding='ISO-8859–1',delimiter='\t')
artists.drop('url',axis=1)
artists.drop('pictureURL',axis=1)

#create ratings dataframe
Ratings = pd.read_csv('user_artists.dat',encoding='ISO-8859–1',delimiter='\t')
Ratings.columns=['id','artistID','rating']

Mean = Ratings.groupby(by="id",as_index=False)['rating'].mean()
Rating_avg = pd.merge(Ratings,Mean,on='id')
Rating_avg['adg_rating']=Rating_avg['rating_x']-Rating_avg['rating_y']


check = pd.pivot_table(Rating_avg,values='rating_x',index='id',columns='artistID')

final = pd.pivot_table(Rating_avg,values='rating_x',index='id',columns='artistID')


#replace nan with artist average
final_artist = final.fillna(final.mean(axis=0))


# user similarity on artists
cosine = cosine_similarity(final_artist)
np.fill_diagonal(cosine, 0 )
similarity_with_artist = pd.DataFrame(cosine,index=final_artist.index)
similarity_with_artist.columns=final_artist.index

def find_n_neighbours(df,n):
    order = np.argsort(df.values, axis=1)[:, :n]
    df = df.apply(lambda x: pd.Series(x.sort_values(ascending=False)
           .iloc[:n].index, 
          index=['top{}'.format(i) for i in range(1, n+1)]), axis=1)
    return df

similar_users = find_n_neighbours(similarity_with_artist,5)

#answer the project questions for user similarities
similarity_with_artist.to_csv('user-pairs-lastFM.data')
user_pairs=similarity_with_artist.copy()

#answer the project questions for user similar neighbors
user_neighbors=similar_users.copy()
dicta=similar_users.to_dict(orient='index')

import json

with open ('neighbors-k-lastFM.data','w') as f:
    json.dump(dicta,f)


#predict rating for a user for an artist he has not listened yet
def Predict(user,item):
    a = similar_users[similar_users.index==user].values
    b = a.squeeze().tolist()
    c = final_artist.loc[:,item]
    d = c[c.index.isin(b)]
    f = d[d.notnull()]
    avg_user = Mean.loc[Mean['id'] == user,'rating'].values[0]
    index = f.index.values.squeeze().tolist()
    corr = similarity_with_artist.loc[user,index]
    fin = pd.concat([f, corr], axis=1)
    fin.columns = ['adg_score','correlation']
    fin['score']=fin.apply(lambda x:x['adg_score'] * x['correlation'],axis=1)
    nume = fin['score'].sum()
    deno = fin['correlation'].sum()
    final_score = avg_user + (nume/deno)
    return final_score



#make recomendation algorith
Rating_avg = Rating_avg.astype({"artistID": str})
Artist_user = Rating_avg.groupby(by = 'id')['artistID'].apply(lambda x:','.join(x))


def Recommend(user):
    Artist_heard_by_user = check.columns[check[check.index==user].notna().any()].tolist()
    a = similar_users[similar_users.index==user].values
    b = a.squeeze().tolist()
    d = Artist_user[Artist_user.index.isin(b)]
    l = ','.join(d.values)
    Artist_heard_by_similar_user = l.split(',')
    Artists_under_consideration = list(set(Artist_heard_by_similar_user)-set(list(map(str, Artist_heard_by_user))))
    Artists_under_consideration = list(map(int, Artists_under_consideration))
    score = []
    for item in Artists_under_consideration:
        c = final_artist.loc[:,item]
        d = c[c.index.isin(b)]
        f = d[d.notnull()]
        avg_user = Mean.loc[Mean['id'] == user,'rating'].values[0]
        index = f.index.values.squeeze().tolist()
        corr = similarity_with_artist.loc[user,index]
        fin = pd.concat([f, corr], axis=1)
        fin.columns = ['adg_score','correlation']
        fin['score']=fin.apply(lambda x:x['adg_score'] * x['correlation'],axis=1)
        nume = fin['score'].sum()
        deno = fin['correlation'].sum()
        final_score = avg_user + (nume/deno)
        score.append(final_score)
    data = pd.DataFrame({'artistID':Artists_under_consideration,'score':score})
    top_5_recommendation = data.sort_values(by='score',ascending=False).head(5)
    Artist_Name = top_5_recommendation.merge(artists, how='inner', left_on='artistID', right_on = 'id')
    Artist_Names = Artist_Name.name.values.tolist()
    return Artist_Names


#evaluation of the recommendation

nUsers = Ratings.id.unique().tolist()
nItems = Ratings.artistID.unique().tolist()


mae=0
for i in nUsers:
    for j in nItems:
        rhat=Predict (i,j)
        #print ('prediction for user',i,'for artist',j,'is',rhat,',vs real',final_artist.get_value(i,j))
        mae=mae+np.abs(rhat-final_artist.get_value(i,j))
print ('MAE=',mae)



