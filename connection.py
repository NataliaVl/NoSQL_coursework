import os
import pymongo

mongodb = pymongo.MongoClient("mongodb://localhost:27017/")

def connection_mongodb_test_map():
    db = mongodb["test"]
    col = db["map"]
    return col

def connection_mongodb_test():
    db = mongodb["test"]
    col = db["texts"]
    return col

# запуск сервера mondodb
def start_mongodb_server():
    os.system(r'C:\Users\Natasha\mongodb\bin\mongod.exe')

def stert_neo4j_server():   #!!
    os.system(r'C:\Program Files\Neo4j\bin\neo4j.bat')