import pymongo
import hashlib
import os
import connection

def import_doc(path):
    f = open(path, 'r', encoding="utf-8")
    title = f.readline()

    doc = title + f.read()
    _line = doc.partition('Содержание статьи:\n')
    _l = _line[2].partition('\n\n')
    description = _l[0]

    hash_object = hashlib.md5(doc.encode())
    hash = hash_object.hexdigest()

    try:
        mydict = {"_id": hash, "doc": doc, "title": title, "description": description}
        x = col.insert_one(mydict)
        export_md5_data(hash, doc)
    except Exception as e:
        print(e)

    f.close()


def export_md5_data(hash, doc):
    path = "C:\hash_archive_storage\ " + hash + ".txt"
    f = open(path.replace(' ', ""), 'w')
    try:
        f.write(doc)
    except Exception as e:
        print(e)
    finally:
        f.close()



col = connection.connection_mongodb_test()

directory = 'C://archive_storage'
files = os.listdir(directory)

for item in files:
    way = directory + "/" + item
    import_doc(way)
