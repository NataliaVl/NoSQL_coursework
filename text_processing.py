import pymongo
import connection
import hashlib
import os
import nltk
import json
import pymystem3
from nltk.stem import PorterStemmer
import porter_stemmer


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


def map_stem(id, words):
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



def sorting_text(id):
    col = connection.connection_mongodb_test_map()

    col.find({"text_hash": id}).sort([("count", -1)])

    print(col.find({"text_hash": id}).count())
    for i in col.find({"text_hash": id}).limit(10):
        print(i)



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

    map_stem(id, words_without_stopwords)


    #print(id)
    #print(words_without_stopwords)
    #wo = ["кроссовк", "новый", "кроссовок", "новый"]
    #map_stem(wo)
