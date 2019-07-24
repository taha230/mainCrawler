#!/usr/bin/python
import MySQLdb
import random, string
from pymongo import MongoClient

##############################################3
def get_unique_key():
    try:
        # db = MySQLdb.connect(host="192.168.1.117",
        #                      port=3306,
        #                      user="root",
        #                      passwd="seyyed158651367",
        #                      db="string_key")
        # cur = db.cursor()
        # while True:
        #     generated_key = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(40))
        #
        #     if not cur.execute("SELECT (1) FROM key_table WHERE key_value = %s", (str(generated_key),)):
        #         try:
        #             cur.execute("""INSERT INTO key_table VALUES (%s)""", (generated_key,))
        #             db.commit()
        #             return generated_key
        #         except:
        #             db.rollback()
        #         db.close()

        clientMongo = MongoClient('192.168.1.117', 27017, connect=False)
        db = clientMongo['string_key']
        collection_key = db['key']

        while True :

            generated_key = ''.join(random.choice(string.ascii_uppercase + string.ascii_lowercase + string.digits) for _ in range(40))

            if not db.collection_key.find_one({ "id": generated_key }):
                collection_key.insert({'id': generated_key}, check_keys=False)
                break


        return generated_key
    except Exception as e:
        print(e)
        print('Error in Accessing DataBase...')
        return None



