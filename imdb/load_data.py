# -*- coding: utf-8 -*-
'''
Data pre process

@author:
Liu Yun

@ created:
07/8/2019
@references:
'''
import os
import pandas as pd
import dill as pickle
import numpy as np
import pymysql
TPS_DIR = '../imdb/data'
np.random.seed(2019)

def read_dataset():
    users_id=[]
    items_id=[]
    ratings=[]
    reviews=[]
    
    db = pymysql.connect("localhost", "root", "302485", "imdb", charset='utf8')
    cursor = db.cursor()
    search_sql = """select * from dataset"""
    try:
       cursor.execute(search_sql)  
    except Exception as e:
        db.rollback() 
        print(str(e))
    finally:
        cursor.close()
        db.close()
    data_result = cursor.fetchall()
    for data in data_result:
        users_id.append(data[0])
        items_id.append(data[1])
        reviews.append(data[2])
        ratings.append(data[3])
    imdb_data=pd.DataFrame({'user_id':pd.Series(users_id),
                   'item_id':pd.Series(items_id),
                   'ratings':pd.Series(ratings),
                   'reviews':pd.Series(reviews)})[['user_id','item_id','ratings','reviews']]
    return imdb_data

def get_count(tp, id):
    playcount_groupbyid = tp[[id, 'ratings']].groupby(id, as_index=False)
    count = playcount_groupbyid.size()
    return count

def numerize(tp, user2id, item2id):
    uid = list(map(lambda x: user2id[x], tp['user_id']))
    sid = list(map(lambda x: item2id[x], tp['item_id']))
    tp['user_id'] = uid
    tp['item_id'] = sid
    return tp

if __name__ == '__main__':
    data = read_dataset()
    usercount, itemcount = get_count(data, 'user_id'), get_count(data, 'item_id')
    unique_uid = usercount.index
    unique_sid = itemcount.index
    item2id = dict((sid, i) for (i, sid) in enumerate(unique_sid))
    user2id = dict((uid, i) for (i, uid) in enumerate(unique_uid))
    data=numerize(data, user2id, item2id)
    tp_rating=data[['user_id','item_id','ratings']]

    n_ratings = tp_rating.shape[0]
    test = np.random.choice(n_ratings, size=int(0.20 * n_ratings), replace=False)
    test_idx = np.zeros(n_ratings, dtype=bool)
    test_idx[test] = True

    tp_1 = tp_rating[test_idx]
    tp_train= tp_rating[~test_idx]

    data2=data[test_idx]
    data=data[~test_idx]

    n_ratings = tp_1.shape[0]
    test = np.random.choice(n_ratings, size=int(0.50 * n_ratings), replace=False)
    
    test_idx = np.zeros(n_ratings, dtype=bool)
    test_idx[test] = True
    
    tp_test = tp_1[test_idx]
    tp_valid = tp_1[~test_idx]
    
    tp_train.to_csv(os.path.join(TPS_DIR, 'imdb_train.csv'), index=False,header=None)
    tp_valid.to_csv(os.path.join(TPS_DIR, 'imdb_valid.csv'), index=False,header=None)
    tp_test.to_csv(os.path.join(TPS_DIR, 'imdb_test.csv'), index=False,header=None)

    user_reviews={}
    item_reviews={}
    user_rid={}
    item_rid={}
    #train dataset
    for i in data.values:
        if user_reviews.__contains__(i[0]):
            user_reviews[i[0]].append(i[3])
            user_rid[i[0]].append(i[1])
        else:
            user_rid[i[0]]=[i[1]]
            user_reviews[i[0]]=[i[3]]
        if item_reviews.__contains__(i[1]):
            item_reviews[i[1]].append(i[3])
            item_rid[i[1]].append(i[0])
        else:
            item_reviews[i[1]] = [i[3]]
            item_rid[i[1]]=[i[0]]

    #test dataset, 
    for i in data2.values:
        if user_reviews.__contains__(i[0]):
            l=1
        else:
            user_rid[i[0]]=[0]
            user_reviews[i[0]]=['0']
        if item_reviews.__contains__(i[1]):
            l=1
        else:
            item_reviews[i[1]] = ['0']
            item_rid[i[1]]=[0]

    pickle.dump(user_reviews, open(os.path.join(TPS_DIR, 'user_review'), 'wb'))
    pickle.dump(item_reviews, open(os.path.join(TPS_DIR, 'item_review'), 'wb'))
    pickle.dump(user_rid, open(os.path.join(TPS_DIR, 'user_rid'), 'wb'))
    pickle.dump(item_rid, open(os.path.join(TPS_DIR, 'item_rid'), 'wb'))

    usercount, itemcount = get_count(data, 'user_id'), get_count(data, 'item_id')
    
    
    print (np.sort(np.array(usercount.values)))
    
    print (np.sort(np.array(itemcount.values)))

