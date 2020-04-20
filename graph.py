import connection
from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "lyoha123"), encrypted=False)

def add_text_to_neo4j(tx, text_name, hash):
    tx.run("MERGE (t:Text {name: $text_name, hash: $hash})",
           text_name=text_name, hash=hash)

def add_stem_to_neo4j(tx, stem, count, hash):
    tx.run("MATCH (t:Text) WHERE t.hash = $hash "
           "MERGE (s:Stem {stem: $stem, count: $count}) "
           "MERGE (t)-[:INCLUDES]->(s)",
           stem=stem, count=count, hash=hash)

def get_title_from_mongodb(text_hash):
    col = connection.connection_mongodb_test()
    for item in col.find({"_id": text_hash}):
        text_name = item["title"]
        break
    return text_name

def data_transfer(id):
    # получение данных из MongoDB
    col = connection.connection_mongodb_test_map()

    # сортировка записей по убыванию кол-ва стемм в тексте
    # limit 10 стемм у каждого текста
    for item in col.find({"text_hash": id}).sort([("count", -1)]).limit(10):
        text_hash = item["text_hash"]
        stem = item["stem"]
        count = item["count"]
        text_name = get_title_from_mongodb(text_hash)

        # запись данных в Neo4j
        with driver.session() as session:
            session.write_transaction(add_text_to_neo4j, text_name, text_hash)
            session.write_transaction(add_stem_to_neo4j, stem, count, text_hash)



def print_stems(tx):
    for record in tx.run("MATCH (s:Stem) "
                         "RETURN s.stem"):
        print(record["s.stem"])

def print_stems1(tx, stem, count):
    rel = "INCLUDES_" + count + "_TIMES"
    print(rel)
    for record in tx.run("MATCH (s:Stem) "
                         "RETURN s.stem"):
        print(record["s.stem"])

def print_all(tx):
    for record in tx.run("MATCH (s) "
                         "RETURN s"):
        print(record["s"])





col = connection.connection_mongodb_test()

hash_list = []
for item in col.find({}):
    hash_list.append(item["_id"])

index = 0
c = len(hash_list)
while index < c:
    data_transfer(hash_list[index])
    #print(hash_list[index])
    index = index + 1


with driver.session() as session:
    session.read_transaction(print_all)

session.close()
driver.close()




