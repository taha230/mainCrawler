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

##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}
EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
                           ConnectionRefusedError, ConnectionDone, ConnectError,
                           ConnectionLost, TCPTimedOutError, ResponseFailed,
                           IOError, TunnelError)

###########################################################
def create_category_url(proxy):
    '''
    function_name: create_category_url
    input: none
    output: first phase of crawler run in clery
    description: add categories urls to categories_importer_tradekey.json file
    '''
    categories = []

    f_categories = open('categories_importer_tradekey.json', 'w')

    url = 'https://importer.tradekey.com'

    while True:
        try:
            html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
            soup = BeautifulSoup(html, 'html.parser')

            if (soup.find_all('a', class_= "smalllinkb")):
                for a in soup.find_all('a', class_= "smalllinkb"):
                    f_categories.write(a['href'])
                    f_categories.write('\n')

        except requests.exceptions.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                time.sleep(30)
                continue
        except EXCEPTIONS_TO_RETRY as e:
            print(e)
            proxy, useragent = change_proxy()
            headers['User-Agent'] = useragent
            print('Error Occurred in creating categories url function and try again')
            time.sleep(30)
            continue

        except Exception as e:
            print('Exception ' + str(e) + ' occured in creating categories urls in url:' + url)
            return {}

        else:
            break


    f_categories.close()


##################################################

def create_all_pages_category_url(proxy):
    '''
    function_name: create_all_pages_category_url
    input: proxy
    output: second phase of crawler run in clery
    description: add all pages of category files
    '''
    categories = []

    f_all_pages_categories_url = open('categories_all_pagesimporter_tradekey.json', 'w')


    with open('categories_importer_tradekey.json') as f:
        categories = json.load(f)

    for url in list(categories):


        while True:
            try:
                html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find_all('a', class_= "smalllinkb")):


            except requests.exceptions.HTTPError as e:
                if (e.code == 403):
                    proxy, useragent = change_proxy()
                    headers['User-Agent'] = useragent
                    time.sleep(30)
                    continue
            except EXCEPTIONS_TO_RETRY as e:
                print(e)
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                print('Error Occurred in creating all categories url function and try again')
                time.sleep(30)
                continue

            except Exception as e:
                print('Exception ' + str(e) + ' occured in creating all categories urls in url:' + url)
                return {}

            else:
                break


        f_all_pages_categories_url.close()

def isListEmpty(inList):
    '''
    function_name: isListEmpty
    input: list
    output: boolean
    description: check if nested list is empty or not
    '''
    if isinstance(inList, list): # Is a list
        return all( map(isListEmpty, inList))
    return False # Not a list

##################################################
def change_proxy():
    '''
    function_name: change_proxy
    input: none
    output: none
    description: change proxy with proxyrotator api
    '''
    url = 'http://falcon.proxyrotator.com:51337/'

    params = dict(
        apiKey='YEXDtBuyrKq3obRLwC4PUQmTZN2SjcxV'
    )

    print('********************************************')
    data = ''

    while True:
        try:
            resp = requests.get(url=url, params=params, timeout=5)
            data = json.loads(resp.text)
            print('Changing Proxy ... ' + data['proxy'])
            print('********************************************')
            return data['proxy'], data['randomUserAgent']

        except:
            print('Exception occured in changeproxy')
            time.sleep(10)
            continue
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
def clean_html(text):
    '''
    function_name: clean_html
    input: text
    output: clean text from html tags
    description: remove html tags from text
    '''
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', text)
    return cleantext

###################################################
def clean_text(text):
    '''
    function_name: clean_text
    input: json
    output: json
    description: Clean Text from non valid characters for add to database
    '''
    json_text = json.loads(json.dumps(text))
    for att, value in json_text.items():
        if (not (type(value) == bool)):
            if (type(value) == list):
                text_1 = clean_html(str(value)[2:-2])  # remove html tags from text
                text_2 = text_1.replace("\']", "").replace("[\'", "")
                text_3 = text_2.lstrip().rstrip()  # remove whitespaces from begin and end of string
                text_4 = text_3.replace('\\n', '').strip()  # remove new line chracters from string
                json_text[att] = re.sub(' +', ' ', text_4)

    return json_text

###################################################
def clean_text_(text):
    '''
    function_name: clean_text_
    input: text
    output: text
    description: Clean Text from non valid characters for add to database
    '''
    text_1 = clean_html(str(text.replace('\n', '')))  # remove html tags from text
    text_2 = text_1.lstrip().rstrip()  # remove whitespaces from begin and end of string
    text_3 = text_2.replace('\\n', '').strip()  # remove new line chracters from string
    text_4 = re.sub(' +', ' ', text_3)
    text_5 = text_4.replace('[]', '').strip()  # remove  [] for empty list
    text_6 = text_5.replace('\']', '').replace('[\'', '')  # remove [' and '] for start and end list
    text_7 = text_6.replace('\\r', '')  # remove \r from text
    text_8 = text_7.replace('\r', '')  # remove \r from text

    return text_8

###################################################
def clean_backslashN_array(inputArray):
    '''
    function_name: clean_backslashN_array
    input: array
    output: cleaned array
    description: Clean all elements of array have \n
    '''
    outArray = []
    for item in inputArray:
        if re.match('.*[a-zA-Z0-9].*', str(item)):
            outArray.append(item)

    return outArray

############################################################
def tokenize_text(txt):
    '''
    function_name: tokenize_text
    input: json
    output: json
    description: tokenize the text according to ":"
    '''

    newResult = {}

    try:
        temp = str(txt)
        temp = re.sub(' +', ' ', temp)
        tokens_ = temp.split("\n")
        for t in tokens_:
            clean_t_ = t.split(":")
            if (len(clean_t_) == 2 and not ('Contact Details:' in t)): # except Contact Details: to add in result
                newResult[clean_text_(clean_t_[0])] = clean_text_(clean_t_[1].split("\n")[0])
    except:
        pass

    return newResult

############################################################
def tokenize_buyer_or_supplier_text(result):
    '''
    function_name: tokenize_buyer_or_supplier_text
    input: json
    output: json
    description: tokenize the supplierText or buyerText
    '''

    newResult = {}  # json to add MongoDB

    if (result['isSupplier']):

        try:

            #####Extract information from 'supplierText' string
            temp = str(result['supplierText'])
            temp = re.sub(' +', ' ', temp)
            tokens_ = temp.split("\n")
            for t in tokens_:
                clean_t_ = t.split(":")
                if (len(clean_t_) == 2 and len(clean_t_[0].replace('\n', '').replace(' ', '').strip()) > 0 and len(
                        clean_t_[1].replace(' ', '').replace('\n', '').strip()) > 0):
                    newResult[str(clean_text_(clean_t_[0]))] = clean_t_[
                        1].replace(' ', '').replace('\n', '').strip()

        except:
            pass

    elif (result['isBuyer']):

        #####Extract information from 'buyerText' string
        temp = str(result['buyerText'])
        temp = re.sub(' +', ' ', temp)
        tokens_ = temp.split("\n")
        for t in tokens_:
            clean_t_ = t.split(":")
        if (len(clean_t_) == 2 and len(clean_t_[0].replace('\n', '').replace(' ', '').strip()) > 0 and len(
                clean_t_[1].replace('\n', '').replace(' ', '').strip()) > 0):
            newResult[str(clean_t_[0].replace('\n', '').replace(' ', '').strip())] = clean_t_[1].replace('\n',
                                                                                                          '').replace(
                ' ', '').strip()


    return newResult

    # update result document in MongoDB
    # if (newResult != {}):
    # self.collection_importer_tradekey_data.update({'Key': result['Key']},{'$set': newResult})

########################################################################################



############################################################



# f = open('importer_tradekey_result.json','w')
#
# f.close() # to erase the previous result
#
# f = open('importer_tradekey_result.json','a')

proxy, useragent = change_proxy()
headers['User-Agent'] = useragent

# create_category_url(proxy)
create_all_pages_category_url(proxy)


# UrlListToRun = chunkIt(total_urls, 5)
#
#
# number_processes = 5
# parts = chunkIt(UrlListToRun[0], number_processes)
#
# processes = []
#
# for i in range(number_processes):
#     processes.append(multiprocessing.Process(target=main_parse, args=[i,parts[i]]))
#
#
# for p in processes:
#     p.start()
#
# for p in processes:
#     p.join()
#
#
# main_parse(1 ,total_urls)
#

# f.close()



