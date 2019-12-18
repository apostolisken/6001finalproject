# -*- coding: utf-8 -*-
"""
Created on Sun Nov 24 19:28:06 2019

"""

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#load files in dataframes
fm_df= pd.read_csv("user_artists.dat",delimiter='\t')
artist_df=pd.read_csv("artists.dat", error_bad_lines=False,delimiter='\t')

#rename column of artist df
artist_df2=artist_df.rename(columns = {"id": "artistID"})

#merge df to get all data in on df
fm_df2 = pd.merge(fm_df, artist_df2, on="artistID")

#plot 1:artists-frequency
plt1=fm_df2.groupby('name')['userID'].count()
plt.figure(figsize=(15,15))
plt1.plot()
plt.xticks(rotation=90)

#plot 2:userID and tag frequency
usertag_df=pd.read_csv("user_taggedartists.dat", error_bad_lines=False,delimiter='\t')
plt2=usertag_df.groupby('userID')['tagID'].count()
plt.figure(figsize=(15,15))
plt2.plot()
plt.xticks(rotation=90)


#plot 3: tags and artist frequency
usertag_df=pd.read_csv("user_taggedartists.dat", error_bad_lines=False,delimiter='\t')
plt3=usertag_df.groupby('artistID')['tagID'].count()
plt.figure(figsize=(15,15))
plt3.plot()
plt.xticks(rotation=90)

#users outliers per number of tags
users_per_tag_df=usertag_df.groupby('userID')['tagID'].count().rename('Count').reset_index()

users_per_tag_df[users_per_tag_df.apply(lambda x: np.abs(x - x.mean()) / x.std() < 3).all(axis=1)]
user_outliers=users_per_tag_df[((users_per_tag_df.Count - users_per_tag_df.Count.mean()) / users_per_tag_df.Count.std()).abs() > 3]

#tags outliers per number of users
tags_per_user_df=usertag_df.groupby('tagID')['userID'].count().rename('Count').reset_index()

tags_per_user_df[tags_per_user_df.apply(lambda x: np.abs(x - x.mean()) / x.std() < 3).all(axis=1)]
tags_outliers=tags_per_user_df[((tags_per_user_df.Count - tags_per_user_df.Count.mean()) / tags_per_user_df.Count.std()).abs() > 3]




