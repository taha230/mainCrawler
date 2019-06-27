
# -*- coding: utf-8 -*-
import json
import urllib
import re
from bs4 import BeautifulSoup
from jsonmerge import merge
from twisted.internet import defer
from twisted.internet.error import TimeoutError, DNSLookupError, \
        ConnectionRefusedError, ConnectionDone, ConnectError, \
        ConnectionLost, TCPTimedOutError
from twisted.web.client import ResponseFailed
import multiprocessing
from scrapy.core.downloader.handlers.http11 import TunnelError
import requests
import requests.exceptions
import time
from pymongo import MongoClient
from grakn.client import GraknClient
import grakn

##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

############################################################

def write_result_db():
    '''
    function_name: write_result_db
    input: none
    output: none
    description: write crawled data from go4w_result.json to CrawlingData mongodb database
    '''

    #Connect to MongoDB
    client = MongoClient('localhost', 27017)
    db = client['CrawlingData']
    collection_go4w_data = db['go4w_data']
    collection_go4w_data.delete_many({})  # Delete all documents in collection


    with open('go4w_result.json') as f:
        file_lines = f.read()
    #print(products[0])
    lines = file_lines.replace('}{','}\n{').split('\n')
    #results = [json.loads(line.strip()) for line in lines]


    chunk_num = 30
    linesToRun = chunkIt(lines, chunk_num)

    cnt_ = 0
    for index_to_write in range(chunk_num):
        results = []
        #index_to_write = 9

        for i in range(len(linesToRun[index_to_write])):
            try:
                # print(i)
                results.append(json.loads(linesToRun[index_to_write][i].strip()))
            except :
                cnt_ +=1
                continue

        for r in list(results):
            collection_go4w_data.insert(r, check_keys=False)

        print (str(index_to_write) + '/' + str(chunk_num) + ' write to database !' )

        #break

    print('all data print to mongodb !')

def write_db_KG():
    '''
    function_name: write_db_KG
    input: none
    output: none
    description: write crawled data from mongodb to grakn.Ai knowledge Graph
    '''

    #Connect to MongoDB
    client = MongoClient('localhost', 27017)
    db = client['CrawlingData']
    collection_go4w_data = db['go4w_data']

    requests = collection_go4w_data.find()


    # Insert to Grakn.Ai Knowledge Graph

    client = GraknClient(uri="localhost:48555")
    session = client.session(keyspace="mineral")
    transaction = session.transaction().write()

    graql_delete_query = "match $x isa company; delete  $x;"
    transaction.query(graql_delete_query)
    transaction.commit()

    company_id =0
    for request in requests:
        if ('buyerCompanyName' in request and request['buyerCompanyName'] != "" ):

            client = GraknClient(uri="localhost:48555")
            session = client.session(keyspace="mineral")
            transaction = session.transaction().write()

            graql_insert_query = "insert $company isa company, has company_id '" + str(company_id) + "'"
            graql_insert_query += ", has country 'a'" # + request["buyerCountry"] + "'"
            # graql_insert_query += ", has address 'b'" # + request["Contact Details: "] + "'"
            graql_insert_query += ";"

            transaction.query(graql_insert_query)
            transaction.commit()
            print("Executed Graql Query: " + str(company_id))


            company_id = company_id+1


    a = 3
    print('all data added to company !')

##################################################

def chunkIt(seq, num):
    '''
    function_name: chunkIt
    input: list, number of parts
    output: list of lists
    description: split list with approximate num of elements in each parts
    '''
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out

##################################################



EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
                           ConnectionRefusedError, ConnectionDone, ConnectError,
                           ConnectionLost, TCPTimedOutError, ResponseFailed,
                           IOError, TunnelError)


#write_result_db()
write_db_KG()

