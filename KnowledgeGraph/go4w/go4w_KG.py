
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
import sys
sys.path.append("..")  # Adds higher directory to python modules path.
from unique_key import get_unique_key
import pymongo
import spacy
import time
import os.path



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
    client = MongoClient('192.168.1.117', 27017)
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

######################################################################################################

def clean_text(input):

    '''
    function_name: clean_text
    input: input string
    output: none
    description: clean string to write in grakn knowledge graph
    '''

    res = input.replace(':', '')\
        .replace('&', ' and ')\
        .replace('/', ' ')\
        .replace('\\', ' ')\
        .replace('\'', '') \
        .replace('.', ' ') \
        .replace('%', ' ') \
        .replace('*', ' ') \
        .replace('\r', ' ') \
        .strip()
    return res

######################################################################################################

def insertCompanyToKG_Buyer(id , request):

    '''
    function_name: insertCompanyToKG_Buyer
    input: company_id and request
    output: none
    description: insert new request for buyer company in the grakn knowledge graph (company table)
    '''

    graql_insert_query = "insert $company isa company, has company_id '" + id + "'"
    graql_insert_query += ", has country '" + clean_text(request["buyerCountry"]) + "'"
    graql_insert_query += ", has name '" + clean_text(request["buyerCompanyName"]) + "'"

    if ('Contact_Details' in request and requests['Contact Details'] != ""):
        graql_insert_query += ", has address '" + clean_text(requests['Contact_Details']) + "'"

    for item in request:
        if ('_id' in item or 'buyerCompanyName' in item or 'date' in item
                or 'buyerProductName' in item or 'buyerCountry' in item
                or 'buyerText' in item or 'buyerCompanyLink' in item
                or 'Key' in item or 'searchCategory' in item or 'Contact Details' in item
                or request[item] == "" or item[0].isdigit() or 'productList' in item
        ):
            continue

        else:

            try:

                item_clean = clean_text(str(item)).replace(',', ' ').replace(')', ' ').replace('(', ' ').replace(' ','_')
                value_clean = clean_text(str(request[item]))

                # print(item_clean)
                # print('value: ' + value_clean)

                client = GraknClient(uri="localhost:48555")
                session = client.session(keyspace="mineral")
                transaction = session.transaction().write()

                transaction.query(
                    'define ' + item_clean + ' sub attribute, datatype string; company sub entity, has ' + item_clean + ';');
                transaction.commit();
                client.close()


                graql_insert_query += ", has " + item_clean + " '" + value_clean + "'"

            except:
                print("exception in adding buyer company new attribute and handled")

    graql_insert_query += ";"

    try:

        client = GraknClient(uri="localhost:48555")
        session = client.session(keyspace="mineral")
        transaction = session.transaction().write()

        transaction.query(graql_insert_query)
        transaction.commit()
        client.close()

        # print("Executed company Buyer insert Query: " + str(request["date"]))
    except:
        print("exception in adding new company query buyer attribute and handled")

######################################################################################################

def insertCompanyToKG_Supplier(id , request):

    '''
    function_name: insertCompanyToKG_Supplier
    input: company_id and request
    output: none
    description: insert new request for supplier company in the grakn knowledge graph (company table)
    '''

    graql_insert_query = "insert $company isa company, has company_id '" + id + "'"
    graql_insert_query += ", has country '" + clean_text(request["supplierCountry"]) + "'"
    graql_insert_query += ", has name '" + clean_text(request["supplierCompanyName"]) + "'"

    if ('Contact_Details' in request and requests['Contact Details'] != ""):
        graql_insert_query += ", has address '" + clean_text(requests['Contact_Details']) + "'"

    for item in request:
        if ('_id' in item or 'supplierCompanyName' in item or 'date' in item
                or 'supplierCountry' in item
                or 'supplierText' in item or 'supplierCompanyLink' in item
                or 'Key' in item or 'searchCategory' in item or 'Contact Details' in item
                or request[item] == "" or item[0].isdigit() or 'productList' in item
        ):
            continue

        else:

            try:

                item_clean = clean_text(str(item)).replace(',', ' ').replace(')', ' ').replace('(', ' ').replace(' ',
                                                                                                                 '_')
                value_clean = clean_text(str(request[item]))
                # print(item_clean)
                # print('value: ' + value_clean)

                client = GraknClient(uri="localhost:48555")
                session = client.session(keyspace="mineral")
                transaction = session.transaction().write()

                transaction.query(
                    'define ' + item_clean + ' sub attribute, datatype string; company sub entity, has ' + item_clean + ';');
                transaction.commit();

                graql_insert_query += ", has " + item_clean + " '" + value_clean + "'"


            except:
                print("exception in adding new company supplier attribute and handled")

    graql_insert_query += ";"

    try:

        client = GraknClient(uri="localhost:48555")
        session = client.session(keyspace="mineral")
        transaction = session.transaction().write()

        transaction.query(graql_insert_query)
        transaction.commit()
      # print("Executed company Supplier insert Query: " + str(request["date"]))
    except:
        print("exception in adding new company query supplier attribute and handled")

######################################################################################################

def insertPersonToKG (id , request):

    '''
    function_name: insertPersonToKG
    input: person_id and request
    output: none
    description: insert new request for person in the grakn knowledge graph (person table)
    '''



    if ('Contact Person:' in request and  request['Contact Person:'] != ""):

        graql_insert_query = "insert $person isa person, has person_id '" + id + "'"
        nameString = request['Contact Person:']
        firstName, lastName = "",""



        firstName = nameString.split(' ')[0]
        if (len(nameString.split(' ')) > 1):
            lastName = nameString.split(' ')[1]

        # print(firstName)
        # print(lastName)

        #print ("Full information : " + str(request['Contact Person:']))

        graql_insert_query += ", has first_name '" +  firstName + "'"

        if (lastName != ""):
            graql_insert_query += ", has last_name '" +  lastName + "'"

        graql_insert_query += ";"

        client = GraknClient(uri="localhost:48555")
        session = client.session(keyspace="mineral")
        transaction = session.transaction().write()

        transaction.query(graql_insert_query)
        transaction.commit()
        # print("Executed person insert Query: " + str(request["date"]))

    if ('Contact' in request and  request['Contact'] != ""):

        graql_insert_query = "insert $person isa person, has person_id '" + id + "'"
        nameString = request['Contact']
        firstName, lastName = "", ""

        firstName = nameString.split(' ')[0]
        if (len(nameString.split(' ')) > 1):
            lastName = nameString.split(' ')[1]

        # print(firstName)
        # print(lastName)

        graql_insert_query += ", has first_name '" + firstName + "'"

        if (lastName != ""):
            graql_insert_query += ", has last_name '" + lastName + "'"

        graql_insert_query += ";"

        client = GraknClient(uri="localhost:48555")
        session = client.session(keyspace="mineral")
        transaction = session.transaction().write()

        transaction.query(graql_insert_query)
        transaction.commit()
        # print("Executed person insert Query: " + str(request["date"]))

######################################################################################################

def insert_product_company_relation (company_id , product_id, type , productInstance):

    '''
    function_name: insert_product_company_relation
    input: company_id , product_id
    output: none
    description: insert new relation for company_product in the grakn knowledge graph (product_company_relation table)
    '''

    graql_insert_query = 'match $company isa company, has company_id "' + company_id + '";'
    # match company
    graql_insert_query += ' $product isa product, has product_id "' + product_id + '";'
    # match product
    graql_insert_query += 'insert $company_product (company_product_owner: $company, company_product: $product) isa product_company_relation'
    graql_insert_query += ', has seller_type "' + type + '"'
    graql_insert_query += ";"

    client = GraknClient(uri="localhost:48555")
    session = client.session(keyspace="mineral")
    transaction = session.transaction().write()

    transaction.query(graql_insert_query)
    transaction.commit()
    # print("Executed person insert Query to product_company_relation with Company_id : " + company_id + " Product_id :" + product_id)

######################################################################################################

def insert_product_broker_relation (person_id , product_id ,type, productInstance):

    '''
    function_name: insert_product_broker_relation
    input: person_id , product_id
    output: none
    description: insert new relation for broker_product in the grakn knowledge graph (product_broker_relation table)
    '''

    graql_insert_query = 'match $person isa person, has person_id "' + person_id + '";'
    # match person
    graql_insert_query += ' $product isa product, has product_id "' + product_id + '";'
    # match product
    graql_insert_query += 'insert $broker_product (broker_product_owner: $person, broker_product: $product) isa product_broker_relation'
    graql_insert_query += ', has seller_type "' + type + '"'
    graql_insert_query += ";"

    client = GraknClient(uri="localhost:48555")
    session = client.session(keyspace="mineral")
    transaction = session.transaction().write()

    transaction.query(graql_insert_query)
    transaction.commit()
    client.close()

    # print("Executed broker insert Query to product_broker_relation with person_id : " + person_id + " Product_id :" + product_id)

######################################################################################################

def insertEmployToKG (company_id , person_id):

    '''
    function_name: insertEmployToKG
    input: company_id , person_id
    output: none
    description: insert new relation for employee in the grakn knowledge graph (employ table)
    '''

    graql_insert_query = 'match $company isa company, has company_id "' + company_id + '";'
    # match company
    graql_insert_query += ' $person isa person, has person_id "' + person_id + '";'
    # match person
    graql_insert_query += 'insert $employ (employer: $company, employee: $person) isa employ'
    graql_insert_query += ";"

    client = GraknClient(uri="localhost:48555")
    session = client.session(keyspace="mineral")
    transaction = session.transaction().write()

    transaction.query(graql_insert_query)
    transaction.commit()
    client.close()

    # print("Executed employ insert Query to employ with person_id : " + person_id + " company_id :" + company_id)

######################################################################################################

def getSimilarCategory (productName):

    '''
    function_name: getSimilarCategory
    input: productName
    output: Product Category
    description: compare the input product name with pre-defined categories and returns the most similar
    category according to the learned model
    '''

    mostSimilarIndex = 0
    mostSimilarityValue =0
    for i in range(len(categories)):
        categoryText = categories[i].replace('_', ' ')
        if (categoryText == ""): continue

        doc1 = nlp(categoryText)
        doc2 = nlp(productName)
        similarity = doc1.similarity(doc2)
        if (similarity > mostSimilarityValue):
            mostSimilarityValue = similarity
            mostSimilarIndex = i


    # print ("Product Name :" + productName + " similar Category: " + categories[mostSimilarIndex])

    return categories[mostSimilarIndex], mostSimilarityValue

######################################################################################################

def insertProductToKG (request, company_id, person_id, company_selected, broker_selected, type):

    '''
    function_name: insertProductToKG
    input:request
    output: none
    description: insert new request for prodcutList in the grakn knowledge graph (product table)
    '''

    for product in request['productList']:

        if ('productName' not in product or product['productName'] == ""): continue

        productInstance , similarity = getSimilarCategory(product['productName'])

        if similarity == 0:
            missed_productCategoryJson = {
                'missedProductName' : product['productName'],
                'request':  str(request)
            }
            f_missed.write(json.dumps(missed_productCategoryJson))
            f_missed.write('\n')

        else :
            id = get_unique_key()

            graql_insert_query = "insert $product isa "+productInstance+", has product_id '" + id + "'"
            if ('productName' in product and product['productName'] != ""):
                graql_insert_query += ", has name '" + clean_text(product['productName']) + "'"
            if ('productText' in product and product['productText'] != ""):
                graql_insert_query += ", has text '" + clean_text(product['productText']) + "'"
            if ('ProductImageSrc' in product and product['ProductImageSrc'] != ""):
                graql_insert_query += ", has imageSrc '" + product['ProductImageSrc'] + "'"

            graql_insert_query += ";"

            client = GraknClient(uri="localhost:48555")
            session = client.session(keyspace="mineral")
            transaction = session.transaction().write()

            transaction.query(graql_insert_query)
            transaction.commit()
            client.close()

            # print("Executed person insert Query: " + str(request["date"]))

            if (company_selected):
                # insert to product_company_relation table
                insert_product_company_relation(company_id , id , type , productInstance)

            elif (broker_selected):
                # insert to product_company_relation table
                insert_product_broker_relation(person_id , id, type , productInstance)


######################################################################################################

def write_db_KG(startIndex, endIndex , prrocessNum):
    '''
    function_name: write_db_KG
    input: none
    output: none
    description: write crawled data from mongodb to grakn.Ai knowledge Graph
    '''

    #Connect to MongoDB

    processed = 1

    clientMongo = MongoClient('192.168.1.117', 27017, connect=False)
    db = clientMongo['CrawlingData']
    collection_go4w_data = db['go4w_data']


    while True:

        requests = collection_go4w_data.find()[startIndex:endIndex]
        # Insert to Grakn.ai Knowledge Graph

        try:
            for request in requests:


                # print(request['date'])
                if (request['date'] == "" ): continue

                company_id = get_unique_key()
                person_id = get_unique_key()
                company_selected = False
                hasPerson = False
                broker_selected = False
                type = ""

                if ('buyerCompanyName' in request and request['buyerCompanyName'] != ""):
                    insertCompanyToKG_Buyer(company_id, request)
                    company_selected = True
                    type = "Buyer"

                if ('supplierCompanyName' in request and request['supplierCompanyName'] != ""):
                    insertCompanyToKG_Supplier(company_id, request)
                    company_selected = True
                    type = "Supplier"

                if (('Contact Person:' in request and request['Contact Person:'] != "") or ('Contact' in request and request['Contact'] != "")):
                    insertPersonToKG(person_id, request)
                    hasPerson = True


                if ('buyerProductName' in request and request['buyerProductName'] != "" and hasPerson and company_selected == False):
                    broker_selected = True
                    type = "Buyer"

                if ('productList' in request and request['productList'] != ''):
                    insertProductToKG(request, company_id, person_id, company_selected, broker_selected, type)

                if (company_selected and hasPerson):
                    insertEmployToKG(company_id, person_id)


                print ('>>>>>>>>>>>>>>> process ' + str(prrocessNum) +' : ' + str(processed) + ' / '+ str (endIndex - startIndex) +' <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<')
                processed += 1



            requests.close()



            break

        except pymongo.errors.CursorNotFound:
            print("Lost cursor. Retry with skip")

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


#nlp = spacy.load("crawl-300d-2M.vec_wiki_lg")
nlp = spacy.load("crawl-300d-2M-subword_wiki_lg")

categories = []

f = open(os.path.dirname(__file__) + '/../Product_Categories.txt', "r") # read from parent directory
categories = f.read().split('\n')

f_missed = open('go4w_missedProduct.json','w')
f_missed.close() # to erase the previous result
f_missed = open('go4w_missedProduct.json','a')


clientMongo = MongoClient('192.168.1.117', 27017, connect=False)
#client = MongoClient('192.168.1.117', 27017, connect=False, username='taha', password='6141', authSource='CrawlingData')
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

#write_result_db()
#write_db_KG(1,DBTotalCount)
client.close()

number_processes = 5
processes = []
countEachProcess = DBTotalCount / number_processes

for i in range(number_processes):
    processes.append(multiprocessing.Process(target=write_db_KG, args=[round(i * countEachProcess), round((i+1)* countEachProcess) -1 , i+1]))
    # break

for p in processes:
    p.start()
    # break

for p in processes:
    p.join()
    # break


f.close()
f_missed.close()

