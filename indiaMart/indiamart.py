# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerProcess
import json
import urllib
from urllib import request as urlrequest
from socket import timeout
import multiprocessing
import re
import requests
from requests.adapters import HTTPAdapter
from bs4 import BeautifulSoup
from pymongo import MongoClient
from jsonmerge import merge
from logging import exception
from scrapy import signals
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from twisted.internet.error import TCPTimedOutError, TimeoutError
import logging
from twisted.internet import defer
from twisted.internet.error import TimeoutError, DNSLookupError, \
        ConnectionRefusedError, ConnectionDone, ConnectError, \
        ConnectionLost, TCPTimedOutError
from twisted.web.client import ResponseFailed
import multiprocessing
from scrapy.exceptions import NotConfigured
from scrapy.utils.response import response_status_message
from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.utils.python import global_object_name
import requests
import mechanicalsoup
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

###########################################################
def create_category_url(s):
    '''
    function_name: create_category_url
    input: none
    output: start_urls for scrapy
    description: add products to urls from products_indiamart.json file
    '''
    url = "https://dir.indiamart.com/"
    global proxy

    while True:
        try:

            # browser = mechanicalsoup.StatefulBrowser()
            # browser.open(url)
            # viewAllList = browser.get_current_page().find_all('a')

            proxy, useragent = change_proxy()
            headers['User-Agent'] = useragent

            html = requests.get(url, proxies={'http': proxy}, headers = headers).content
            soup = BeautifulSoup(html, 'html.parser')
            viewAllHrefList = []
            aList = soup.find_all('a')

            f = open('indiamart_categories.txt','w')


            for a in aList:
                if 'view all' in a.text:
                    viewAllHrefList.append('https:' + a['href'])


            if (len(viewAllHrefList) < 1):
                continue


            for urlViewAll in viewAllHrefList:
                htmlViewAll = requests.get(urlViewAll, proxies={'http': proxy}, headers=headers).content
                soupViewAll = BeautifulSoup(htmlViewAll, 'html.parser')

                blockList = soupViewAll.find_all('li', class_= "q_cb")
                for block in blockList:
                    for a in block.find_all('a'):
                        f.write("https://dir.indiamart.com" + a['href'])
                        f.write('\n')

            f.close()  # to erase the previous result

            break



        except urllib.error.HTTPError as e:
            print(e)
            if (e.code == 403):

                continue
        except EXCEPTIONS_TO_RETRY as e:
            print(e)

            print('Error Occurred in main_parse function and try again')
            continue

        except Exception as e:
            print(e)

        else:
            break

        html = s.get(url, proxies={'http': proxy}).content
        soup = BeautifulSoup(html, 'html.parser')
        viewAllList = soup.find_all('a')


##########################################################
def read_category_url():
    '''
    function_name: read_category_url
    input: none
    output: start_urls for Beautifulsoup
    description: add categories to urls from CategoriesLinks_globalresources.txt file
    '''

    file = open('CategoriesLinks_indiamart.txt', 'r')
    categories = file.read().split('\n')


    urls = []
    # categories = categories[0:10]
    for c in list(categories):
        urls.append(str(c))
        # return urls

    return urls

##########################################################
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
    resp = requests.get(url=url, params=params)
    data = json.loads(resp.text)

    print('Changing Proxy ... ' + data['proxy'])
    print('********************************************')
    return data['proxy'], data['randomUserAgent']

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

    return text_7

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
                    newResult[str(clean_t_[0].replace('\n', '').replace(' ', '').replace('.', '').strip())] = clean_t_[
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
    # self.collection_go4w_data.update({'Key': result['Key']},{'$set': newResult})
########################################################################################

############################################################

def main_parse(urls):
    '''
    function_name: main_parse
    input: list
    output: none
    description: first level of crawling
    '''

    ########################################################
    for url in urls:
        # categories
        # taha
        proxy, useragent = change_proxy()
        s = requests.session()
        s.headers.update({'User-Agent': useragent})

        while True:
            try:


                # try:
                #     response = urlrequest.urlopen(req, timeout=5)
                # except SocketError as e:
                #     proxy, useragent = change_proxy()
                #     headers['User-Agent'] = useragent
                #     continue
                #
                #
                # soup = BeautifulSoup(response.read().decode('utf8'), 'html.parser')
                html = s.get(str(url), proxies={'http': proxy}).content
                soup = BeautifulSoup(html, 'html.parser')

                #u1 = urllib.urlretrieve(str(url))
                #soup = BeautifulSoup(requests.get(str(url), proxies={'http': proxy}, headers=headers).content, 'html.parser')
                items = soup.find_all('div', recursive=True)
                #items_urls = [i.find('a').attrs['href'] for i in items]
                ls = soup.select('div.image_tit')



            except urllib.error.HTTPError as e:
                if (e.code == 403):
                    proxy, useragent = change_proxy()
                    s.headers.update({'User-Agent': useragent})
                    continue

            # except:
            #     proxy, useragent = change_proxy()
            #     headers['User-Agent'] = useragent
            #     print('Error Occurred in function and try again')
            #     continue
            else:
                break

############################################################
############################################################
EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
                           ConnectionRefusedError, ConnectionDone, ConnectError,
                           ConnectionLost, TCPTimedOutError, ResponseFailed,
                           IOError, TunnelError)

proxy, useragent = change_proxy()
s = requests.session()
s.headers.update({'User-Agent': useragent})

create_category_url(s)
#urls = read_category_url()
#main_parse(urls)