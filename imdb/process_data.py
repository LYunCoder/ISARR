# -*- coding: utf-8 -*-
import numpy as np
import re
import itertools
from collections import Counter
import pymysql

import tensorflow as tf
import pickle
import os

def del_all_flags(FLAGS):
    flags_dict = FLAGS._flags()    
    keys_list = [keys for keys in flags_dict]    
    for keys in keys_list:
        FLAGS.__delattr__(keys)
        
del_all_flags(tf.flags.FLAGS)

tf.flags.DEFINE_string("valid_data", "../imdb/data/imdb_valid.csv", " Data for validation")
tf.flags.DEFINE_string("test_data", "../imdb/data/imdb_test.csv", "Data for testing")
tf.flags.DEFINE_string("train_data", "../imdb/data/imdb_train.csv", "Data for training")
tf.flags.DEFINE_string("user_review", "../imdb/data/user_review", "User's reviews")
tf.flags.DEFINE_string("item_review", "../imdb/data/item_review", "Item's reviews")
tf.flags.DEFINE_string("user_review_id", "../imdb/data/user_rid", "user_review_id")# user_id, item_id
tf.flags.DEFINE_string("item_review_id", "../imdb/data/item_rid", "item_review_id")# item_id, user_id
tf.flags.DEFINE_string("stopwords", "../data/stopwords", "stopwords")

def get_triples(identifier_list):
    db = pymysql.connect("localhost", "root", "302485", "imdb", charset='utf8')
    cursor = db.cursor()
    triples = []
    try:
        for identifier in identifier_list:
            search_sql = "select triples from identifier_triples where identifier='%s'"%identifier
            cursor.execute(search_sql)
            triple_result = cursor.fetchone()
            triples.extend(triple_result[0].split(','))
    except Exception as e:
        db.rollback() 
        print(str(e))
    finally:
        cursor.close()
        db.close()
    return triples
        
def clean_str(string):
    """
    Tokenization/string cleaning for all datasets except for SST.
    Original taken from https://github.com/yoonkim/CNN_sentence/blob/master/process_data.py
    """
    string = re.sub(r"[^A-Za-z]", " ", string)
    string = re.sub(r"\'s", " \'s", string)
    string = re.sub(r"\'ve", " \'ve", string)
    string = re.sub(r"n\'t", " n\'t", string)
    string = re.sub(r"\'re", " \'re", string)
    string = re.sub(r"\'d", " \'d", string)
    string = re.sub(r"\'ll", " \'ll", string)
    string = re.sub(r",", " , ", string)
    string = re.sub(r"!", " ! ", string)
    string = re.sub(r"\(", " \( ", string)
    string = re.sub(r"\)", " \) ", string)
    string = re.sub(r"\?", " \? ", string)
    string = re.sub(r"\s{2,}", " ", string)
    return string.strip().lower()


def pad_sentences(u_text, u_len, u2_len, padding_word="<PAD/>"):
    """
    Pads all sentences to the same length. The length is defined by the longest sentence.
    Returns padded sentences.
    """
    review_num = u_len # the max number of reviews per user
    review_len = u2_len # the max length of review

    u_text2 = {}
    for i in u_text.keys(): # for the current user i
        u_reviews = u_text[i] # the list of reviews of user i
        padded_u_train = []
        for ri in range(review_num): # for the current review of user i
            if ri < len(u_reviews): # the current review belongs to the user
                sentence = u_reviews[ri]
                if review_len > len(sentence):
                    num_padding = review_len - len(sentence)
                    new_sentence = sentence + [padding_word] * num_padding
                    padded_u_train.append(new_sentence)
                else:
                    new_sentence = sentence[:review_len]
                    padded_u_train.append(new_sentence)
            else: #pad new review of user i
                new_sentence = [padding_word] * review_len
                padded_u_train.append(new_sentence)
        u_text2[i] = padded_u_train

    return u_text2


def pad_reviewid(u_train, u_valid, u_len, num): # reid_user_train, reid_user_valid, u_len, item_num + 1
    pad_u_train = []

    for i in range(len(u_train)):
        x = u_train[i] #is a list of item_ids which user i commented.
        while u_len > len(x):   
            x.append(num)
        if u_len < len(x):
            x = x[:u_len]
        pad_u_train.append(x)
    pad_u_valid = []

    for i in range(len(u_valid)):
        x = u_valid[i]
        while u_len > len(x):
            x.append(num)
        if u_len < len(x):
            x = x[:u_len]
        pad_u_valid.append(x)
    return pad_u_train, pad_u_valid


def build_vocab(sentences1, sentences2):
    """
    Builds a vocabulary mapping from word to index based on the sentences.
    Returns vocabulary mapping and inverse vocabulary mapping.
    """
    # Build vocabulary
    word_counts1 = Counter(itertools.chain(*sentences1))
    # Mapping from index to word
    vocabulary_inv1 = [x[0] for x in word_counts1.most_common()]
    vocabulary_inv1 = list(sorted(vocabulary_inv1))
    # Mapping from word to index
    vocabulary1 = {x: i for i, x in enumerate(vocabulary_inv1)}

    word_counts2 = Counter(itertools.chain(*sentences2))
    # Mapping from index to word
    vocabulary_inv2 = [x[0] for x in word_counts2.most_common()]
    vocabulary_inv2 = list(sorted(vocabulary_inv2))
    # Mapping from word to index
    vocabulary2 = {x: i for i, x in enumerate(vocabulary_inv2)}
    return [vocabulary1, vocabulary_inv1, vocabulary2, vocabulary_inv2]


def build_input_data(u_text, i_text, vocabulary_u, vocabulary_i):
    """
    Maps sentencs and labels to vectors based on a vocabulary.
    """
    l = len(u_text)
    u_text2 = {}
    for i in u_text.keys():
        u_reviews = u_text[i]
        u = np.array([[vocabulary_u[word] for word in words] for words in u_reviews])
        u_text2[i] = u
    l = len(i_text)
    i_text2 = {}
    for j in i_text.keys():
        i_reviews = i_text[j]
        i = np.array([[vocabulary_i[word] for word in words] for words in i_reviews])
        i_text2[j] = i
    return u_text2, i_text2


def load_data(train_data, valid_data, user_review, item_review, user_rid, item_rid, stopwords):
    """
    Loads and preprocessed data for the MR dataset.
    Returns input vectors, labels, vocabulary, and inverse vocabulary.
    """
    # Load and preprocess data
    u_text, i_text, y_train, y_valid, u_len, i_len, u2_len, i2_len, uid_train, iid_train, uid_valid, iid_valid, user_num, item_num \
        , reid_user_train, reid_item_train, reid_user_valid, reid_item_valid = \
        load_data_and_labels(train_data, valid_data, user_review, item_review, user_rid, item_rid, stopwords)
    print ("load data done")
    u_text = pad_sentences(u_text, u_len, u2_len)
    reid_user_train, reid_user_valid = pad_reviewid(reid_user_train, reid_user_valid, u_len, item_num + 1)

    print ("pad user done")
    i_text = pad_sentences(i_text, i_len, i2_len)
    reid_item_train, reid_item_valid = pad_reviewid(reid_item_train, reid_item_valid, i_len, user_num + 1)

    print ("pad item done")

    user_triple = [xx for x in u_text.values() for xx in x] # user_triple [[review1_t1,...],[review2_t1,...]...]
    item_triple = [xx for x in i_text.values() for xx in x]

    #vocabulary_user:{word1:inx_0, word2:inx_1,...}. vocabulary_inv_user:[word1,word2,...]. word is unique and sorted
    vocabulary_user, vocabulary_inv_user, vocabulary_item, vocabulary_inv_item = build_vocab(user_triple, item_triple)
    print (len(vocabulary_user))
    print (len(vocabulary_item))
    u_text, i_text = build_input_data(u_text, i_text, vocabulary_user, vocabulary_item)
    y_train = np.array(y_train)
    y_valid = np.array(y_valid)
    uid_train = np.array(uid_train)
    uid_valid = np.array(uid_valid)
    iid_train = np.array(iid_train)
    iid_valid = np.array(iid_valid)
    reid_user_train = np.array(reid_user_train)
    reid_user_valid = np.array(reid_user_valid)
    reid_item_train = np.array(reid_item_train)
    reid_item_valid = np.array(reid_item_valid)

    return [u_text, i_text, y_train, y_valid, vocabulary_user, vocabulary_inv_user, vocabulary_item,
            vocabulary_inv_item, uid_train, iid_train, uid_valid, iid_valid, user_num, item_num, reid_user_train,
            reid_item_train, reid_user_valid, reid_item_valid]


def load_data_and_labels(train_data, valid_data, user_review, item_review, user_rid, item_rid, stopwords):
    """
    Loads MR polarity data from files, splits the data into words and generates labels.
    Returns split sentences and labels.
    """
    # Load data from files


    f_train = open(train_data, "r")
    f1 = open(user_review, "rb")
    f2 = open(item_review, "rb")
    f3 = open(user_rid, "rb")
    f4 = open(item_rid, "rb")

    user_reviews = pickle.load(f1)
    item_reviews = pickle.load(f2)
    user_rids = pickle.load(f3)
    item_rids = pickle.load(f4)

    reid_user_train = []
    reid_item_train = []
    uid_train = []
    iid_train = []
    y_train = []
    u_text = {}
    u_rid = {}
    i_text = {}
    i_rid = {}
    i = 0
    for line in f_train:
        i = i + 1
        line = line.split(',')
        uid_train.append(int(line[0]))
        iid_train.append(int(line[1]))
        if u_text.__contains__(int(line[0])):
            reid_user_train.append(u_rid[int(line[0])])
        else:
            u_text[int(line[0])] = []
            #deal with identifiers
            for s in user_reviews[int(line[0])]:
                s_list = s.split("	")
                triples = get_triples(s_list)
                u_text[int(line[0])].append(triples)
            u_rid[int(line[0])] = []
            for s in user_rids[int(line[0])]:
                u_rid[int(line[0])].append(int(s))
            reid_user_train.append(u_rid[int(line[0])])

        if i_text.__contains__(int(line[1])):
            reid_item_train.append(i_rid[int(line[1])])  #####write here
        else:
            i_text[int(line[1])] = []
            for s in item_reviews[int(line[1])]:
                s_list = s.split("	")
                triples = get_triples(s_list)
                i_text[int(line[1])].append(triples)
            i_rid[int(line[1])] = []
            for s in item_rids[int(line[1])]:
                i_rid[int(line[1])].append(int(s))
            reid_item_train.append(i_rid[int(line[1])])
        y_train.append(float(line[2]))
    print ("valid")
    reid_user_valid = []
    reid_item_valid = []

    uid_valid = []
    iid_valid = []
    y_valid = []
    f_valid = open(valid_data)
    for line in f_valid:
        line = line.split(',')
        uid_valid.append(int(line[0]))
        iid_valid.append(int(line[1]))
        if u_text.__contains__(int(line[0])):
            reid_user_valid.append(u_rid[int(line[0])])
        else:
            u_text[int(line[0])] = [['<PAD/>']]
            u_rid[int(line[0])] = [int(0)]
            reid_user_valid.append(u_rid[int(line[0])])

        if i_text.__contains__(int(line[1])):
            reid_item_valid.append(i_rid[int(line[1])])
        else:
            i_text[int(line[1])] = [['<PAD/>']]
            i_rid[int(line[1])] = [int(0)]
            reid_item_valid.append(i_rid[int(line[1])])

        y_valid.append(float(line[2]))
    print ("len")


    review_num_u = np.array([len(x) for x in u_text.values()]) # the number of reviews of users
    x = np.sort(review_num_u)
    u_len = x[int(0.9 * len(review_num_u)) - 1] # only covering 90% numbers of reviews, the max number of reviews
    review_len_u = np.array([len(j) for i in u_text.values() for j in i]) # the number of words of each review
    x2 = np.sort(review_len_u)
    u2_len = x2[int(0.9 * len(review_len_u)) - 1] # only covering 90% numbers of words, the max number of words

    review_num_i = np.array([len(x) for x in i_text.values()])
    y = np.sort(review_num_i)
    i_len = y[int(0.9 * len(review_num_i)) - 1]
    review_len_i = np.array([len(j) for i in i_text.values() for j in i])
    y2 = np.sort(review_len_i)
    i2_len = y2[int(0.9 * len(review_len_i)) - 1]
    print ("u_len:", u_len)
    print ("i_len:", i_len)
    print ("u2_len:", u2_len)
    print ("i2_len:", i2_len)
    user_num = len(u_text)
    item_num = len(i_text)
    print ("user_num:", user_num)
    print ("item_num:", item_num)
    return [u_text, i_text, y_train, y_valid, u_len, i_len, u2_len, i2_len, uid_train,
            iid_train, uid_valid, iid_valid, user_num,
            item_num, reid_user_train, reid_item_train, reid_user_valid, reid_item_valid]


if __name__ == '__main__':
    TPS_DIR = '../imdb/data'
    FLAGS = tf.flags.FLAGS
    FLAGS.flag_values_dict()

    u_text, i_text, y_train, y_valid, vocabulary_user, vocabulary_inv_user, vocabulary_item, \
    vocabulary_inv_item, uid_train, iid_train, uid_valid, iid_valid, user_num, item_num, reid_user_train, reid_item_train, reid_user_valid, reid_item_valid = \
        load_data(FLAGS.train_data, FLAGS.valid_data, FLAGS.user_review, FLAGS.item_review, FLAGS.user_review_id,
                  FLAGS.item_review_id, FLAGS.stopwords)

    np.random.seed(2019)

    shuffle_indices = np.random.permutation(np.arange(len(y_train)))

    userid_train = uid_train[shuffle_indices]
    itemid_train = iid_train[shuffle_indices]
    y_train = y_train[shuffle_indices]
    reid_user_train = reid_user_train[shuffle_indices]
    reid_item_train = reid_item_train[shuffle_indices]

    y_train = y_train[:, np.newaxis]
    y_valid = y_valid[:, np.newaxis]

    userid_train = userid_train[:, np.newaxis]
    itemid_train = itemid_train[:, np.newaxis]
    userid_valid = uid_valid[:, np.newaxis]
    itemid_valid = iid_valid[:, np.newaxis]

    batches_train = list(
        zip(userid_train, itemid_train, reid_user_train, reid_item_train, y_train))
    batches_test = list(zip(userid_valid, itemid_valid, reid_user_valid, reid_item_valid, y_valid))
    print ('write begin')
    output = open(os.path.join(TPS_DIR, 'imdb.train'), 'wb')
    pickle.dump(batches_train, output)
    output = open(os.path.join(TPS_DIR, 'imdb.test'), 'wb')
    pickle.dump(batches_test, output)

    para = {}
    para['user_num'] = user_num
    para['item_num'] = item_num
    para['review_num_u'] = u_text[0].shape[0]
    para['review_num_i'] = i_text[0].shape[0]
    para['review_len_u'] = u_text[1].shape[1]
    para['review_len_i'] = i_text[1].shape[1]
    para['user_vocab'] = vocabulary_user #{1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6, 8: 7, 9: 8, 10: 9, 20: 10, 22: 11}
    para['item_vocab'] = vocabulary_item
    para['train_length'] = len(y_train)
    para['test_length'] = len(y_valid)
    para['u_text'] = u_text
    para['i_text'] = i_text
    output = open(os.path.join(TPS_DIR, 'imdb.para'), 'wb')

    # Pickle dictionary using protocol 0.
    pickle.dump(para, output)











