import pymongo
import connection
import hashlib
import os
import nltk
import json
import pymystem3
from nltk.stem import PorterStemmer
import porter_stemmer
import redis


def export_text():
    for item in col.find({}):
        print(item["doc"])


def load_stopwords():
    with open("stopwords.json", "r", encoding="utf-8") as read_file:
        stopwords = json.load(read_file)
    return stopwords


def check_text_for_stopwords(words, stopwords):
    words_without_stopwords = []
    words = [element.lower() for element in words]  # преобразование списка в нижний регистр

    count = 0
    for text_word in words:
        for stopword in stopwords:
            if text_word == stopword:
                count += 1
                # words.remove(text_word)

        if count == 0:
            words_without_stopwords.append(text_word)
        count = 0

    return words_without_stopwords


def create_mongodb_map(id, words):
    col = connection.connection_mongodb_test_map()

    for word in words:
        stem = porter_stemmer.Porter.stem(word)
        c = 0
        try:
            if col.count() == 0:
                mydict = {"text_hash": id, "stem": stem, "count": 1}
                x = col.insert_one(mydict)
            else:
                for item in col.find({}):
                    if item["stem"] == stem and item["text_hash"] == id:
                        _count = item["count"] + 1
                        _id = item["_id"]
                        myquery = {"_id": _id}
                        newvalues = {"$set": {"count": _count}}

                        x = col.update_one(myquery, newvalues)
                        c += 1
                        #break

                if c == 0:
                    mydict = {"text_hash": id, "stem": stem, "count": 1}
                    x = col.insert_one(mydict)
                    c = 0

        except Exception as e:
            print(e)


def create_redis_map(id, words, r):
    for word in words:
        stem = porter_stemmer.Porter.stem(word)
        redis_key = id + ":" + stem

        try:
            if r.exists(redis_key):
                r.incr(redis_key)
            else:
                r.mset({redis_key: '1'})

        except Exception as e:
            print(e)

    #print_all(get_keys_list(id)) #вывести на экран все записи в redis


def get_keys_list(id):
    # кол-во записей для одного текста (один хэш)
    redis_key = id + ":" + "*"
    keys_list = []
    _keys_list = r.keys(redis_key)
    for item in _keys_list:
        key = item.decode("utf-8")
        keys_list.append(key)
    return keys_list


def get_stem_frequency(key, keys_list):
    total_stem_count = len(keys_list)
    stem_count = r.get(key).decode("utf-8")
    frequency = round(int(stem_count) / total_stem_count * 100, 2) # частот-ть в %, округление до 2х знаков после запятой
    return frequency


def get_stems_frequency(key, keys_list):
    total_stem_count = len(keys_list)
    for key in keys_list:
        stem_count = r.get(key).decode("utf-8")
        frequency = round(int(stem_count) / total_stem_count * 100, 2) # частот-ть в %, округление до 2х знаков после запятой
    return frequency


def add_stems_to_mongodb(keys_list):
    col = connection.connection_mongodb_test_stems()

    for item in keys_list:
        key = item
        count = int(r.get(key).decode("utf-8"))
        _keys = key.partition(':')
        text_hash = _keys[0]
        stem = _keys[2]
        frequency = get_stem_frequency(key, keys_list)

        try:
            mydict = {"text_hash": text_hash, "stem": stem, "count": count, "frequency": frequency}
            x = col.insert_one(mydict)

        except Exception as e:
            print(e)


def sorting_text(id):
    #col = connection.connection_mongodb_test_stems()
    col = connection.connection_mongodb_test_map()

    print(col.find({"text_hash": id}).count())
    for i in col.find({"text_hash": id}).sort([("count", -1)]).limit(10):
        print(i)


def print_all(list_keys):
    for redis_key in list_keys:
        print(redis_key + " -- " + r.get(redis_key).decode("utf-8"))





r = redis.Redis()

punctuation_mark = ['.', ',', ':', ';', '?', '!', '...', '—', '"', '(', ')', '/', '№', '$', '%', '*', '&', '`', '~', '#', '@', '+', '»', '«']

stopwords = load_stopwords()

stopwords.extend(punctuation_mark)

col = connection.connection_mongodb_test()

# ЭКСПОРТ ТЕКСТА
for item in col.find({}):
    text = item["doc"]
    id = item["_id"]

    words = nltk.word_tokenize(text)

    words_without_stopwords = check_text_for_stopwords(words, stopwords)



    # create_redis_map(id, words_without_stopwords, r)
    # keys_list = get_keys_list(id)
    # add_stems_to_mongodb(keys_list)

    sorting_text(id)


    # create_mongodb_map(id, words_without_stopwords)




