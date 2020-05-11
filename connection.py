import os
import pymongo
from neo4j import GraphDatabase

mongodb = pymongo.MongoClient("mongodb://localhost:27017/")

# запуск серверов
def start_mongodb_server():
    os.system(r'C:\Users\Natasha\mongodb\bin\mongod.exe')

def start_redis_server():
    os.system(r'C:\DB\Redis\redis-server.exe')

def start_neo4j_server():
    os.system(r'C:\DB\Neo4j\bin\neo4j.bat console')



def connection_mongodb_test_map():
    db = mongodb["test"]
    col = db["map"]
    return col

def connection_mongodb_test_stems():
    db = mongodb["test"]
    col = db["stems"]
    return col

def connection_mongodb_test():
    db = mongodb["test"]
    col = db["texts"]
    return col

