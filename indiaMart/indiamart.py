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
from tabletojson import table_to_json, table_to_json_complex
import random
import time


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
                        if ('impcat' in a['href']):
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



##########################################################
def read_category_url():
    '''
    function_name: read_category_url
    input: none
    output: start_urls for Beautifulsoup
    description: add categories to urls from CategoriesLinks_globalresources.txt file
    '''

    file = open('indiamart_categories.txt', 'r')
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
        apiKey='YEXDtBuyrKq3obRLwC4PUQmTZN2SjcxV',
        #referer="true",
        connectionType="Datacenter"
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

            # if('Android' in data['randomUserAgent']):
            #     continue

        except:
            print('Exception occured in changeproxy')
            time.sleep(60)
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

def cleanhtmlFromTag(html , tag):
    htmlText = str(html)
    tablestr = re.findall("<"+tag+">(.*?)</"+tag+">",htmlText)
    for table in tablestr:
        htmlText = htmlText.replace(table, '')
    return bytes(htmlText, 'utf-8')


########################################################################################

def cleanhtml(html):

  htmlText = str(html)
  htmlText = htmlText.replace('\\\\','').replace('\\\'','\'')
  return bytes(htmlText, 'utf-8')

############################################################

def productDetailCrawler(url, proxy):
    '''
    function_name: productDetailCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from productDetailCrawler
    '''

    url = 'https://www.indiamart.com/proddetail/detergent-cake-making-machine-11246772962.html'

    while True:
        try:

            newResult = {}
            html = requests.get(url, proxies={'http': proxy}, headers = headers, timeout=5).content
            soup = BeautifulSoup(html, 'html.parser')

            productDescriptionDiv = soup.find('div', id="pdpD")

            if (productDescriptionDiv):

                tableList = productDescriptionDiv.find_all('table')

                for table in tableList:
                    dataTable = table_to_json(str(table))
                    tableName = table.parent.find('h3').text.strip()
                    newResult[tableName] = dataTable

                cleanhtml = cleanhtmlFromTag(html , "table")
                soupcleanhtml = BeautifulSoup(cleanhtml, 'html.parser')

                if (soupcleanhtml.find('div', id="pdpD")):
                    h3List = soupcleanhtml.find('div', id="pdpD").find_all('h3')

                    for h3 in h3List:
                        nextDiv = h3.findNext('div')
                        if (nextDiv.text != "" and 'Product Image' not in str(nextDiv)):
                            newResult[h3.text] = clean_text_(nextDiv.text)

            if (soup.find('div' , class_="pdppro-img") and soup.find('div' , class_="pdppro-img").find('img') and soup.find('div' , class_="pdppro-img").find('img')['data-src']):
                newResult['Product Image'] = soup.find('div' , class_="pdppro-img").find('img')['data-src']


            return newResult

        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                continue
        except EXCEPTIONS_TO_RETRY as e:
            print (e)
            proxy, useragent = change_proxy()
            headers['User-Agent'] = useragent
            print('Error Occurred in productDetailCrawler function and try again')
            continue

        except:
            print('Exception occured in productDetailCrawler  with url : ' + url)
            return {}

        else:
            break

########################################################################################

def companyDetailCrawler(url, proxy):
    '''
    function_name: companyDetailCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from companyDetailCrawler
    '''

    url = 'https://www.indiamart.com/proddetail/detergent-cake-making-machine-11246772962.html'


    while True:
        try:
            newResult = {}
            html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
            soup = BeautifulSoup(html, 'html.parser')

            companyDescriptionDiv = soup.find('div', id="aboutUs")

            if (companyDescriptionDiv):
                divWid3List = companyDescriptionDiv.find_all('div' , class_="wid3")

                for divWid3 in divWid3List:
                    if (divWid3.find_all('span') and len(divWid3.find_all('span')) == 2):
                        divWid3Title = divWid3.find_all('span')[0].text.strip()
                        divWid3Value = divWid3.find_all('span')[1].text.strip()
                        newResult[divWid3Title] = divWid3Value


                tableList = companyDescriptionDiv.find_all('table')

                for table in tableList:
                    dataTable = table_to_json(str(table))
                    tableName = table.parent.find('h3').text.strip()
                    newResult[tableName] = dataTable

                aboutusText = str(companyDescriptionDiv)
                tag = 'div'
                strRE = "<(?:div|h3|input)(.*?)</(?:div|h3|input)"
                divSections = re.findall(strRE, str(companyDescriptionDiv))
                for div in divSections:
                    aboutusText = aboutusText.replace(div , '')


                newResult['about us text'] = clean_text_(aboutusText)


            return newResult

        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                continue
        except EXCEPTIONS_TO_RETRY as e:
            print (e)
            proxy, useragent = change_proxy()
            headers['User-Agent'] = useragent
            print('Error Occurred in companyDetailCrawler function and try again')
            continue

        except:
            print('Exception occured in companyDetailCrawler with url : ' + url)
            return {}

        else:
            break


########################################################################################
def nestedGeneralProductCrawler(url, result, proxy):
    '''
    function_name: nestedURLGeneralCompanyCrawler
    input: response
    output: json
    description: crawl all information from company nested url
    '''


    while True:
        try:
            nestedResult = {}

            html = requests.get(url, proxies={'http': proxy}, headers = headers, timeout=5).content
            soup = BeautifulSoup(html, 'html.parser')



            companyMode, companyResponseRate, companyContactPhone, companyContactPerson= "","","",""


            listDiv = soup.find_all('div' , class_="sbox")

            for div in listDiv:
                if (div.find('i' , class_="pmic psimg") and div.find('span')):
                    companyMode =  div.find('span').text.strip()
                if (div.find('i', class_="preic psimg") and div.find('span')):
                    companyResponseRate = div.find('span').text.replace('Response Rate' , '').strip()

            if (soup.find('span', id = "pns_no2")):
                companyContactPhone = soup.find('span', id = "pns_no2").text.replace('Call' ,'').strip()

            if(soup.find('div', id = "supp_nm")):
                companyContactPerson = soup.find('div', id = "supp_nm").text.strip()

            nestedResult = {
                'companyMode': companyMode,
                'companyResponseRate': companyResponseRate,
                'companyContactPhone': companyContactPhone,
                'companyContactPerson': companyContactPerson,
                'nestedURL' : url
            }


            # Product Detail Crawler
            if (soup.find('div', id= "pdpD")):
                productDetailResult = productDetailCrawler(url,proxy)
                nestedResult = merge(nestedResult, productDetailResult)

            # Company Detail Crawler
            if (soup.find('div', id= "pdpD")):
                companyDetailResult = companyDetailCrawler(url, proxy)
                nestedResult = merge(nestedResult, companyDetailResult)


            return nestedResult

        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                continue
        except EXCEPTIONS_TO_RETRY as e:
            print (e)
            proxy, useragent = change_proxy()
            headers['User-Agent'] = useragent
            print('Error Occurred in nestedGeneralProductCrawler function and try again')
            continue
        except:
            print('Exception occured in nestedGeneralProductCrawler in url:' + url)
            return {}

        else:
            break


def main_parse(p, urls):
    '''
    function_name: main_parse
    input: list
    output: none
    description: first level of crawling
    '''

    proxy, useragent = change_proxy()

    # headers['path'] = '/impcat/next?mcatId=30693&prod_serv=P&mcatName=industrial-machinery&srt=57&end=76&ims_flag=&cityID=&prc_cnt_flg=1&fcilp=0&pr=0&pg=3&frsc=28&video='
    # headers['cookie'] = 'site-entry-page=https://dir.indiamart.com/impcat/metal-mesh.html; _ga=GA1.2.1901784363.1560320000; __gads=ID=7b29b5a9d6edf2a1:T=1560320007:S=ALNI_MbGAavgYJly0Pg7binsNn8IqPfDtQ; _ym_uid=1560320011246585078; _ym_d=1560320011; blformopen=1; _gaexp=GAX1.2.jKa9WrmiRQyPjEYmZtMRlw.18136.0; GeoLoc=lt%3D%7Clg%3D%7Caccu%3D%7Clg_ct%3D%7Clg_ctid%3D; _gid=GA1.2.599650647.1560781508; __sonar=16503240949616253344; G_ENABLED_IDPS=google; iploc=gcniso=DE|gcnnm=Germany|gctnm=|gctid=|gacrcy=200|gip=148.251.243.103|gstnm=null; _ym_isad=2; sessid=spv=1; xnHist=pv%3D24%7Cipv%3D0%7Cfpv%3D0%7Ccity%3D%7Ccvstate%3D%7Cpopupshown%3Dundefined%7Cinstall%3Dundefined%7Css%3Dundefined%7Cmb%3Dundefined%7Ctm%3Dundefined%7Cage%3Dundefined%7Ccount%3D%7Ctime%3D%7Cglid%3D; r=g; _ym_visorc_51115208=w; _gat_UA-10312824-1=1'
    # headers['referer'] = 'https://dir.indiamart.com/impcat/industrial-machinery.html'
    # headers['user-agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'

    headers['authority'] = 'dir.indiamart.com'
    headers['method'] = 'GET'
    headers['scheme'] = 'https'
    headers['accept'] = '*/*'
    headers['accept-encoding'] = 'gzip, deflate, br'
    headers['accept-language'] = 'en-US,en;q=0.9,fa;q=0.8'
    headers['x-requested-with'] = 'XMLHttpRequest'
    #headers['User-Agent'] = useragent
    #headers['User-Agent'] = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])

    ########################################################
    for url in urls:
        # categories
        #proxy, useragent = change_proxy()
        #s = requests.session()
        #headers['User-Agent'] = useragent

        productResultSet = [] # initial set for each url

        # while len(productResultSet) < 1000:
        #     previousProductResultLength = len(productResultSet)

        while True:
            try:

                #url = "https://dir.indiamart.com/impcat/spm-machine.html"
                html = requests.get(str(url), proxies = {'http': proxy}, headers=headers).content
                soup = BeautifulSoup(html, 'html.parser')


                productList = []



                if (soup.find('div', id= "page_variables") and soup.find('div',{"id": "page_variables"}).script):

                    scriptPageVariable = soup.find('div',{"id": "page_variables"}).script

                    urlFirst = url.split('/impcat')[0] + '/impcat/next?mcatId='
                    mcatId = str(scriptPageVariable).split(',mcatID:"')[1].split("\"")[0]
                    prod_serv = str (scriptPageVariable).split('var prod_serv = \'')[1].split("\'")[0]
                    pageDisplay = str(scriptPageVariable).split('firstResultCount = ')[1].split(';')[0]
                    mcatName = str(scriptPageVariable) .split('mcatName:\"')[1].split('\"')[0]
                    firstResultCount = str(scriptPageVariable) .split('firstResultCount =')[1].split(';')[0]
                    fcilp = str(scriptPageVariable) .split("fcilp = \'")[1].split('\'')[0]


                    # # Page 1
                    firstResultCount = 0
                    page =0

                    while True:

                        soupPage = None

                        while True:
                            try:
                                endResultCount = int(firstResultCount) + int (pageDisplay)
                                pg =page +1
                                urlPage = urlFirst + str(mcatId) + "&prod_serv=" + str(prod_serv) + "&mcatName=" + str(mcatName) + "&srt=" + str(int(firstResultCount)+1) + "&end=" + str(endResultCount) + "&ims_flag=&cityID=&prc_cnt_flg=1&fcilp="+ fcilp +"&pr=0&pg=" + str(pg) + "&frsc=" + str(pageDisplay) + "&video="
                                htmlPage = requests.get(str(urlPage), proxies={'http': proxy}, headers = headers, timeout=5).content
                                htmlPage = cleanhtml(htmlPage)
                                soupPage = BeautifulSoup(htmlPage, 'html.parser')

                                if ('Error 403 (Forbidden)' in str(soupPage)):
                                    print('Error 403 (Forbidden) occured and retry')
                                    continue

                            except urllib.error.HTTPError as e:
                                #if (e.code == 403):
                                proxy, useragent = change_proxy()
                                headers['User-Agent'] = useragent
                                continue

                            except EXCEPTIONS_TO_RETRY as e:
                                print(e)
                                continue
                                # proxy, useragent = change_proxy()
                                # headers['User-Agent'] = useragent
                                # print('Error Occurred in function mainparser and try again')
                                # continue
                            except exception as e:
                                print(e)
                                continue

                            else:
                                break


                        if (soupPage.find_all('li' , class_="lst_cl")):
                            productList.extend(soupPage.find_all('li' , class_="lst_cl"))
                        else:
                            break

                        firstResultCount = firstResultCount + int(pageDisplay)
                        print(page)
                        page = page + 1



                    for product in productList:

                        companyName, companyURL, companyAddress,companyLocation,companyType, companyTrustType, companyProductName,companyRate,productURL = "","","","","","","","",""

                        if (product.find('h4', class_="lcname")):
                            companyName = product.find('h4', class_="lcname").text.strip()
                        if (product.find('div' , class_= "r-cl") and product.find('div' , class_= "r-cl").find('a') and product.find('div' , class_= "r-cl").find('a')['href']):
                            companyURL = product.find('div' , class_= "r-cl").find('a')['href']
                        if (product.find('span' , class_= "to-txt")):
                            companyAddress = product.find('span' , class_= "to-txt").text
                        if (product.find('p', class_="sm clg")):
                            companyLocation = product.find('p', class_="sm clg").text.replace(companyAddress, '').strip()
                        if (product.find('p', class_="ig mrg") and product.find('p', class_="ig mrg").find('span')):
                            companyType = product.find('p', class_="ig mrg").find('span').text.strip()
                        if (product.find('span' , class_="bg t_se")):
                            companyTrustType = product.find('span' , class_="bg t_se").text.strip()
                        if (product.find('h3', class_="lg")):
                            companyProductName = product.find('h3', class_="lg").text.strip()
                        if (product.find('span' , class_="prc cur")):
                            companyRate = product.find('span' , class_="prc cur").text.replace('Get Latest Price','').replace(' ', '').strip()
                        if (product.find('a', class_="pnm ldf cur") and product.find('a', class_="pnm ldf cur")['href']):
                            productURL = product.find('a', class_="pnm ldf cur")['href']


                        result = {
                            'companyName': companyName,
                            'companyURL': companyURL,
                            'companyAddress': companyAddress,
                            'companyLocation': companyLocation,
                            'companyType': companyType,
                            'companyTrustType': companyTrustType,
                            'companyProductName': companyProductName,
                            'companyRate': companyRate,
                            'productURL': productURL,
                            'Key': companyURL + productURL,
                            'searchURL': url
                        }
                        nestedResult = {}

                        if (productURL != ""):
                            nestedResult = nestedGeneralProductCrawler(productURL ,result, proxy)

                            result = merge(result, nestedResult)
                            #productResultSet.append(json.dumps(result))
                            f.write(json.dumps(result))
                            f.write('\n')

                            print(json.dumps(result))



                else :
                    proxy, useragent = change_proxy()
                    headers['User-Agent'] = useragent
                    continue



            except urllib.error.HTTPError as e:
                if (e.code == 403):
                    proxy, useragent = change_proxy()
                    headers['User-Agent'] = useragent
                    continue

            except:
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                print('Error Occurred in function mainparser and try again')
                continue
            else:
                break

            # productResultSet = list(set(productResultSet))
            #
            # if (previousProductResultLength == len(productResultSet)):
            #     break


        ####################
        # for result in productResultSet:
        #     f.write(result)
        #     f.write('\n')
        # print( "<<<<<<<<<<<<<<<<<<<<<<<<<< category " + url + "write to file >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> ")



############################################################
############################################################
EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
                           ConnectionRefusedError, ConnectionDone, ConnectError,
                           ConnectionLost, TCPTimedOutError, ResponseFailed,
                           IOError, TunnelError)

#proxy, useragent = change_proxy()
#s = requests.session()
#headers['User-Agent'] = useragent


f = open('indiamart_result.json','w')

f.close() # to erase the previous result

f = open('indiamart_result.json','a')

#create_category_url(s)
total_urls = read_category_url()


UrlListToRun = chunkIt(total_urls, 5)


number_processes = 5
parts = chunkIt(UrlListToRun[0], number_processes)

processes = []

for i in range(number_processes):
    processes.append(multiprocessing.Process(target=main_parse, args=[i,parts[i]]))


for p in processes:
    p.start()

for p in processes:
    p.join()

#main_parse(1 ,total_urls)

f.close()