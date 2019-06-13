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
from tabletojson import table_to_json, table_to_json_complex
from bs4 import BeautifulSoup
from pymongo import MongoClient
##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}
##################################################
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
    resp = requests.get(url=url, params=params)
    data = json.loads(resp.text)

    print('Changing Proxy ... ' + data['proxy'])
    print('********************************************')
    return data['proxy'], data['randomUserAgent']

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
    text_1 = clean_html(str(text)[2:-2])  # remove html tags from text
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

###########################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: start_urls for scrapy
    description: add products to urls from products_alibaba.json file
    '''
    with open('products_go4w.json') as f:
        products = json.load(f)
    #print(products[0])
    urls = []
    #products = products[0:10]
    for p in list(products):
        urls.append("https://www.go4worldbusiness.com/suppliers/" + str(p['name']).replace(',','').replace(' & ',' ').replace(' ',"-"))
        urls.append("https://www.go4worldbusiness.com/buyers/" + str(p['name']).replace(',','').replace(' & ',' ').replace(' ',"-"))
        #return urls

    return urls
############################################################
############################################################
def buyerCrawler(url, s):
    '''
    function_name: buyerCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from buyer in www.go4worldbusiness.com
    '''
    proxy, useragent = '',''

    ########################################################
    while True:
        try:
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')
            buyerList = soup.find_all('div', class_ ="col-xs-12 nopadding search-results")

            for searchResultSet in buyerList:
                result = {
                    'buyerCompanyName': searchResultSet.find('h2',class_="entity-row-title h2-item-title").find('span').text.strip(),
                    'date': searchResultSet.find('div',class_="col-xs-3 col-sm-2 xs-padd-lr-2 nopadding").find('small').text.strip(),
                    'buyerProductName': clean_text_(str(searchResultSet.find('h2',class_="text-capitalize entity-row-title h2-item-title").find('span').text.strip())),
                    'buyerCountry': clean_text_(str(searchResultSet.find('span',class_="pull-left subtitle text-capitalize").text.strip().replace('Buyer From', ''))),
                    'buyerText': searchResultSet.find('div',class_="col-xs-12 entity-row-description-search xs-padd-lr-5").find('p').text.strip(),
                    'buyerBuyerOF': clean_text_(str(searchResultSet.find('div', class_="col-xs-12 xs-padd-lr-5").find('div').find('a').text.strip()).replace('Buyer Of', '')),
                    'buyerCompanyLink': searchResultSet.find('span',class_="pull-left").find('a')['href'],
                    'isSupplier': False,
                    'isBuyer': True,
                    'Key': str(clean_text_(searchResultSet.find('div',class_="col-xs-3 col-sm-2 xs-padd-lr-2 nopadding").find('small').text.strip()) + ' , ' + clean_text_(str(searchResultSet.find('span',class_="pull-left").find('a')['href']))).replace(' ', ''),
                    'searchCategory': url.replace('https://www.go4worldbusiness.com/find?searchText=', '').replace('&FindBuyers', '')

                }



        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy()
                s.headers.update({'User-Agent': useragent})
                continue
        except:
            proxy, useragent = change_proxy()
            s.headers.update({'User-Agent': useragent})
            print('Error Occurred in buyerCrawler function and try again')
            continue

        else:
            break

###########################################################
###########################################################
def supplierCrawler(url, s):
    '''
    function_name: supplierCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from supplier in www.go4worldbusiness.com
    '''
    proxy, useragent = '', ''

    ########################################################
    while True:
        try:
            a=3

        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy()
                s.headers.update({'User-Agent': useragent})
                continue
        except:
            proxy, useragent = change_proxy()
            s.headers.update({'User-Agent': useragent})
            print('Error Occurred in supplierCrawler function and try again')
            continue

        else:
            break
###########################################################
###########################################################
def main_parse(urls):

    '''
    function_name: main_parse
    input: list
    output: none
    description: first level of crawling
    '''

    proxy, useragent = change_proxy()
    s = requests.session()
    s.headers.update({'User-Agent': useragent})

    ########################################################
    for url in urls:
        # categories

        isBuyerSelected, isSupplierSelected = False, False
        if ("/suppliers/" in str(url)):
            isSupplierSelected = True
        if ("/buyers/" in str(url)):
            isBuyerSelected = True
        TotalPageNum = 1

        while True:
            try:
                html = s.get(str(url), proxies={'http': proxy}).content
                soup = BeautifulSoup(html, 'html.parser')


                ########################################################################
                if (isBuyerSelected):
                    lastPagelist = soup.find('ul', class_ ="pagination").find_all('li')
                    lastPageBuyerhref = lastPagelist[len(lastPagelist) - 1].find('a')['href'].strip()
                    if ("pg_buyers" not in lastPageBuyerhref):  # category has only one page of buyer
                        TotalPageNum = 1
                    else:
                        TotalPageNum = int(str(lastPageBuyerhref).split('pg_buyers')[1].split('=')[1])  # parse the buyer total page number

                    for i in range(TotalPageNum):
                        nextPageURL = url+"?region=worldwide&pg_buyers=" + str(i+1) # +1 to start from 1 to buyerPageNum
                        buyerCrawler(nextPageURL, s)


                ###############################################################################
                elif (isSupplierSelected):
                    lastPagelist = soup.find('ul', class_ ="pagination").find_all('li')
                    lastPageSupplierhref = lastPagelist[len(lastPagelist) - 2].find('a')['href'].strip()
                    if ("pg_suppliers" not in lastPageSupplierhref):  # category has only one page of buyer
                        TotalPageNum = 1
                    else:
                        TotalPageNum = int(
                            str(lastPageSupplierhref).split('pg_suppliers')[1].split('=')[1])  # parse the supplier total page number

                    for i in range(TotalPageNum):
                        nextPageURL = url+"?region=worldwide&pg_suppliers=" + str(i+1) # +1 to start from 1 to supplierPageNum
                        supplierCrawler(nextPageURL, s)

            except urllib.error.HTTPError as e:
                print(e)
                if (e.code == 403):
                    proxy, useragent = change_proxy()
                    s.headers.update({'User-Agent': useragent})
                    continue
            except:
                proxy, useragent = change_proxy()
                s.headers.update({'User-Agent': useragent})
                print('Error Occurred in main_parse function and try again')
                continue

            else:
                break

############################################################
urls = create_category_url()
main_parse(urls)