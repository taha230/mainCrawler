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
from jsonmerge import merge
##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

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
        #urls.append("https://www.go4worldbusiness.com/suppliers/" + str(p['name']).replace(',','').replace(' & ',' ').replace(' ',"-"))
        urls.append("https://www.go4worldbusiness.com/buyers/" + str(p['name']).replace(',','').replace(' & ',' ').replace(' ',"-"))
        #return urls

    return urls

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

def nestedURLOurCompanyCrawler(url, s, proxy):
    '''
    function_name: nestedURLOurCompanyCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from nestedURLOurCompanyCrawler
    '''

    nestedResultOurCompany = {}

    while True:
        try:
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')

            companyLogo = ""
            if (soup.find('div', class_="mar-top-10 col-xs-offset-0 col-xs-11 gold-menu text-center")):
                companyLogo = \
                soup.find('div', class_="mar-top-10 col-xs-offset-0 col-xs-11 gold-menu text-center").find('img')['src']

            tokenizedResult = {}

            if (soup.find('div', class_="padd-lr-10 mar-top-10")):
                txt = soup.find_all('div', class_="padd-lr-10 mar-top-10")[0].text
                tokenizedResult = tokenize_text(txt)

            contactDetailText = ""
            if (soup.find('div', class_="padd-lr-10 mar-top-10")):
                for div in soup.find_all('div', class_="padd-lr-10 mar-top-10"):
                    if (div.find('address')):
                        contactDetailText += div.find('address').text.replace('<address>', '').replace(
                            '</address>', '').replace('<br>', ',').replace('<b>', ',').replace('</b>', ',')

            newResult = {}
            newResult['logoSrc'] = clean_text_(str(companyLogo))
            newResult['contactDetail'] = clean_text_(contactDetailText)
            nestedResultOurCompany = merge(newResult, tokenizedResult)


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


    return nestedResultOurCompany


########################################################################################

def nestedURLProductsCompanyCrawler(url, s, proxy):
    '''
    function_name: nestedURLProductsCompanyCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from nestedURLProductsCompanyCrawler
    '''

    while True:
        try:
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')
            productList = soup.find_all('div', class_="xs-padd-lr-5")

            productTextList =[]
            productNameList =[]
            ProductImageSrcList =[]

            for product in productList:
                if (product.find('p')):
                    productTextList.append(product.find('p').text)
                if (product.find('img')):
                    ProductImageSrcList.append(product.find('img')['src'])

            productNameListSoup = soup.find_all('div' , class_= "mar-top-5")

            for productName in productNameListSoup:
                if (productName.find('h5', class_= "entity-row-title")):
                    if (productName.find('h5', class_= "entity-row-title").find('span')):
                        productNameList.append(productName.find('h5', class_= "entity-row-title").find('span').text)

            productList = []

            for i in range(len(productNameList)):
                productTex = ""
                ProductImageSrc = ""

                if (i < len(productTextList)): productTex = productTextList[i]
                if (i < len(ProductImageSrcList)): ProductImageSrc = ProductImageSrcList[i]

                product = {
                    'productName': clean_text_(productNameList[i]),
                    'productText': clean_text_(productTex),
                    'ProductImageSrc': clean_text_(ProductImageSrc),
                }
                productList.append(product)

            newResult = {}

            newResult["productList"] = productList

            return newResult

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


########################################################################################

def nestedURLManagementCompanyCrawler(url, s, proxy):
    '''
    function_name: nestedURLManagementCompanyCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from nestedURLManagementCompanyCrawler
    '''

    while True:
        try:
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')

            managementTextDiv = str(soup.find('div',class_="col-xs-12"))
            managementText =""

            if (managementTextDiv.find('p').text):
                managementText = managementTextDiv.find('p').text

            newResult = {}
            newResult['managementText'] = clean_text_(managementText)

            return newResult

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


########################################################################################

def nestedURLFacilitiesCompanyCrawler(url, s, proxy):
    '''
    function_name: nestedURLFacilitiesCompanyCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from nestedURLFacilitiesCompanyCrawler
    '''

    while True:
        try:
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')

            facilitiesTextDiv = soup.find_all('div', class_="padd-lr-10 mar-top-10")
            facilitiesText = ""

            for div in facilitiesTextDiv:
                for divrow in div.find_all('div' , class_="row"):
                    if (divrow.find('div', class_="col-xs-12").text):
                        facilitiesText = divrow.find('div', class_="col-xs-12").text

            newResult = {}
            newResult['facilitiesText'] = clean_text_(facilitiesText)

            return newResult

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


########################################################################################

def nestedURLNewsRoomCompanyCrawler(url, s, proxy):
    '''
    function_name: nestedURLNewsRoomCompanyCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from nestedURLNewsRoomCompanyCrawler
    '''

    while True:
        try:
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')

            newsRoomTextDiv = soup.find_all('div', class_="padd-lr-10 mar-top-10")
            newsRoomText = ""

            for div in newsRoomTextDiv:
                for divrow in div.find_all('div', class_="row"):
                    if (divrow.find('div', class_="col-xs-12").text):
                        newsRoomText = divrow.find('div', class_="col-xs-12").text

            newResult = {}
            newResult['newsRoomText'] = clean_text_(newsRoomText)

            return newResult

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
def nestedURLGeneralCompanyCrawler(url, result , s , proxy):
    '''
    function_name: nestedURLBuyerCompanyCrawler
    input: response
    output: json
    description: crawl all inforamtion from company nested url
    '''

    while True:
        try:
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')
            mainTabList = soup.find('ul', class_="center-pills").find_all('li')

            isOurCompanySelected, isProductsSelected, isManagementSelected, isFacilitiesSelector, isNewsRoomSelector = None, None, None, None, None
            nestedResult = {}

            for  i in range(len(mainTabList)):
                if ('Company' in mainTabList[i].find('a').text):
                    isOurCompanySelected = mainTabList[i].find('a')['href']
                elif ('Products' in mainTabList[i].find('a').text):
                    isProductsSelected = mainTabList[i].find('a')['href']
                elif ('Management' in mainTabList[i].find('a').text):
                    isManagementSelected = mainTabList[i].find('a')['href']
                elif ('Facilities' in mainTabList[i].find('a').text):
                    isFacilitiesSelector = mainTabList[i].find('a')['href']
                elif ('News' in mainTabList[i].find('a').text):
                    isNewsRoomSelector = mainTabList[i].find('a')['href']

            if ("html" in str(isOurCompanySelected) and (str(isOurCompanySelected) in url)):  # the second condition for nested page in company page

                companyLogo = ""
                if (soup.find('div', class_= "mar-top-10 col-xs-offset-0 col-xs-11 gold-menu text-center")):
                    companyLogo = soup.find('div', class_= "mar-top-10 col-xs-offset-0 col-xs-11 gold-menu text-center").find('img')['src']

                tokenizedResult = {}

                if (soup.find('div', class_="padd-lr-10 mar-top-10")):
                    txt = soup.find_all('div', class_="padd-lr-10 mar-top-10")[0].text
                    tokenizedResult = tokenize_text(txt)


                contactDetailText = ""
                if (soup.find('div', class_="padd-lr-10 mar-top-10")):
                    for div in soup.find_all('div', class_="padd-lr-10 mar-top-10"):
                        if (div.find('address')):
                            contactDetailText += div.find('address').text.replace('<address>','').replace(
                        '</address>', '').replace('<br>', ',').replace('<b>', ',').replace('</b>', ',')

                newResult = {}
                newResult['logoSrc'] = clean_text_(str(companyLogo))
                newResult['contactDetail'] = clean_text_(contactDetailText)
                nestedResultOurCompany = merge(newResult, tokenizedResult)
                nestedResult = merge (nestedResult , nestedResultOurCompany)

            #############################################################################################################
            if ((str(isOurCompanySelected) not in url) and ("html" in str(isOurCompanySelected))):
                nestedURLOurCompany = "https://www.go4worldbusiness.com" + str(isOurCompanySelected)
                nestedResultOurCompany = nestedURLOurCompanyCrawler(nestedURLOurCompany, s, proxy)
                nestedResult = merge (nestedResult , nestedResultOurCompany)

            if ("html" in str(isProductsSelected)):
                nestedURLProductsCompany = "https://www.go4worldbusiness.com" + str(isProductsSelected)
                nestedResultProducts = nestedURLProductsCompanyCrawler(nestedURLProductsCompany, s, proxy)
                nestedResult = merge (nestedResult , nestedResultProducts)

            if ("html" in str(isManagementSelected)):
                nestedURLManagementCompany = "https://www.go4worldbusiness.com" + str(isManagementSelected)
                nestedResultManagement = nestedURLManagementCompanyCrawler(nestedURLManagementCompany, s, proxy)
                nestedResult = merge (nestedResult , nestedResultManagement)

            if ("html" in str(isFacilitiesSelector)):
                nestedURLFacilitiesCompany = "https://www.go4worldbusiness.com" + str(isFacilitiesSelector)
                nestedResultFacilities = nestedURLFacilitiesCompanyCrawler(nestedURLFacilitiesCompany, s, proxy)
                nestedResult = merge (nestedResult , nestedResultFacilities)

            if ("html" in str(isNewsRoomSelector)):
                nestedURLNewsRoomCompany = "https://www.go4worldbusiness.com" + str(isNewsRoomSelector)
                nestedResultNewsRoom = nestedURLNewsRoomCompanyCrawler(nestedURLNewsRoomCompany, s, proxy)
                nestedResult = merge (nestedResult , nestedResultNewsRoom)

            return nestedResult

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





########################################################################
def buyerCrawler(url, s, proxy):
    '''
    function_name: buyerCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from buyer in www.go4worldbusiness.com
    '''

    ########################################################
    while True:
        try:
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')
            buyerList = soup.find_all('div', class_ ="search-results")

            for searchResultSet in buyerList:

                buyerCompanyName, buyerProductName, date, buyerCountry, buyerText, buyerBuyerOF, buyerCompanyLink= "","","","","","",""

                if (searchResultSet.find('h2',class_="entity-row-title") != None):
                    buyerCompanyName = searchResultSet.find('h2',class_="entity-row-title").find('span').text.strip()

                date =  searchResultSet.find('div',class_="col-xs-3 col-sm-2 xs-padd-lr-2 nopadding").find('small').text.strip()

                if (searchResultSet.find('h2',class_="text-capitalize entity-row-title h2-item-title") != None):
                    buyerProductName = clean_text_(str(searchResultSet.find('h2',class_="text-capitalize entity-row-title h2-item-title").find('span').text.strip()))

                buyerCountry = clean_text_(str(searchResultSet.find('span',class_="pull-left subtitle text-capitalize").text.strip().replace('Buyer From', '')))

                if (searchResultSet.find('div',class_="col-xs-12 entity-row-description-search xs-padd-lr-5") != None):
                    buyerText = searchResultSet.find('div',class_="col-xs-12 entity-row-description-search xs-padd-lr-5").find('p').text.strip()
                buyerBuyerOF = clean_text_(str(searchResultSet.find('div', class_="mar-top-10").find('a').text.strip()).replace('Buyer Of', ''))
                buyerCompanyLink = None
                if (len(searchResultSet.find_all('span',class_="pull-left")) >= 2 ):
                    buyerCompanyLink = searchResultSet.find_all('span',class_="pull-left")[1].find('a')['href']
                isSupplier = False
                isBuyer = True
                Key = str(clean_text_(date) + ' , ' + clean_text_(buyerCompanyLink).replace(' ', ''))
                searchCategory = url

                result = {
                    'buyerCompanyName' : buyerCompanyName,
                    'date' : date,
                    'buyerProductName' : buyerProductName,
                    'buyerCountry' : buyerCountry,
                    'buyerText' : buyerText,
                    'buyerBuyerOF' : buyerBuyerOF,
                    'buyerCompanyLink' : buyerCompanyLink,
                    'isSupplier' : isSupplier,
                    'isBuyer' : isBuyer,
                    'Key' : Key,
                    'searchCategory' : searchCategory
                }

                if (result['buyerCompanyName'] != []):  # crawl nested company link for cases have type buyerCompanyName (not person link and with star in front)
                    nestedURLCompany = "https://www.go4worldbusiness.com" + str(result['buyerCompanyLink'])
                    nestedResult = nestedURLGeneralCompanyCrawler(nestedURLCompany, result, s, proxy)
                else:
                    tokenizeResutl = tokenize_buyer_or_supplier_text(result)
                    result = merge(result, tokenizeResutl)

                result = merge(result, nestedResult)
                return result

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
def supplierCrawler(url, s, proxy):
    '''
    function_name: supplierCrawler
    input: response
    output: crawlerDataset
    description: crawl all information from supplier in www.go4worldbusiness.com
    '''

    ########################################################
    while True:
        try:
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')
            supplierList = soup.find_all('div', class_="search-results")

            for searchResultSet in supplierList:

                supplierCompanyName, date, supplierCountry, supplierText, supplierSupplierOF, supplierCompanyLink= "","","","","",""

                if (searchResultSet.find('h2', class_="entity-row-title") and searchResultSet.find('h2', class_="entity-row-title").find(
                        'span')):
                    supplierCompanyName = searchResultSet.find('h2', class_="h2-item-title").find(
                        'span').text.strip()

                if (searchResultSet.find('div', class_="borderJUNIOR") and searchResultSet.find('div', class_="borderJUNIOR").find('small')):
                    date = searchResultSet.find('div', class_="borderJUNIOR").find('small').text.strip()

                if (searchResultSet.find('span', class_="text-capitalize")):
                    supplierCountry = clean_text_(str(
                    searchResultSet.find('span', class_="text-capitalize").text.strip().replace(
                        'Supplier From', '')))

                if (searchResultSet.find('div', class_="entity-row-description-search") and searchResultSet.find('div', class_="entity-row-description-search").find('p')):
                    supplierText = searchResultSet.find('div',
                                                     class_="entity-row-description-search").find('p').text.strip()

                if (searchResultSet.find('div') and searchResultSet.find('div').find('a')):
                    aList = searchResultSet.find('div', class_="mar-top-10").find_all('a')
                    for a in aList:
                        supplierSupplierOF += clean_text_(str(a.text.strip()).replace('Supplier Of',''))

                if (searchResultSet.find('a')['href']):
                    supplierCompanyLink = searchResultSet.find('a')['href']


                isSupplier = True
                isBuyer = False
                Key = str(clean_text_(date) + ' , ' + clean_text_(supplierCompanyLink).replace(' ', ''))
                searchCategory = url

                result = {
                    'supplierCompanyName': supplierCompanyName,
                    'date': date,
                    'supplierCountry': supplierCountry,
                    'supplierText': supplierText,
                    'supplierSupplierOF': supplierSupplierOF,
                    'supplierCompanyLink': supplierCompanyLink,
                    'isSupplier': isSupplier,
                    'isBuyer': isBuyer,
                    'Key': Key,
                    'searchCategory': searchCategory
                }

                if (result['supplierCompanyLink'] != []):  # crawl nested company link for cases have supplier company link
                    tokenizeResutl = tokenize_buyer_or_supplier_text(result)
                    result = merge(result, tokenizeResutl)

                    nestedURLCompany = "https://www.go4worldbusiness.com" + str(result['supplierCompanyLink'])
                    nestedResult = nestedURLGeneralCompanyCrawler(nestedURLCompany, result, s, proxy)

                result = merge (result, nestedResult)

                return result

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
                        result = buyerCrawler(nextPageURL, s, proxy)
                        f.write(json.dumps(result))
                        f.write('\n')

                        print(json.dumps(result))


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
                        result = supplierCrawler(nextPageURL, s, proxy)
                        f.write(json.dumps(result))
                        f.write('\n')

                        print(json.dumps(result))

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

f = open('go4w_result.json','a')


urls = create_category_url()
main_parse(urls)

f.close()


