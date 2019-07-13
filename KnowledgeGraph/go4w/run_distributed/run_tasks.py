from .tasks import write_db_KG
from pymongo import MongoClient
from grakn.client import GraknClient
import time
from celery.result import ResultBase
##########################################
##########################################
if __name__ == '__main__':

    clientMongo = MongoClient('192.168.1.117', 27017, connect=False)
    db = clientMongo['CrawlingData']
    collection_go4w_data = db['go4w_data']
    DBTotalCount = collection_go4w_data.count()
    clientMongo.close()

    client = GraknClient(uri="localhost:48555")
    session = client.session(keyspace="mineral")
    transaction = session.transaction().write()

    graql_delete_query = "match $x isa company; delete  $x;"
    transaction.query(graql_delete_query)
    transaction.commit()

    # write_result_db()
    write_db_KG(1,DBTotalCount,1)
    client.close()





    #ff.close()