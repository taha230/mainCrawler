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
            temp = str(result['supplierText'])[2:-2]
            temp = re.sub(' +', ' ', temp)
            tokens_ = temp.split("\\r\\n")
            for t in tokens_:
                clean_t_ = t.split(":")
                if (len(clean_t_) == 2 and len(clean_t_[0].replace('\\n', '').replace(' ', '').strip()) > 0 and len(
                        clean_t_[1].replace(' ', '').replace('\\n', '').strip()) > 0):
                    newResult[str(clean_t_[0].replace('\\n', '').replace(' ', '').replace('.', '').strip())] = clean_t_[
                        1].replace(' ', '').replace('\\n', '').strip()

        except:
            pass

    elif (result['isBuyer']):

        #####Extract information from 'buyerText' string
        temp = str(result['buyerText'])[2:-2]
        temp = re.sub(' +', ' ', temp)
        tokens_ = temp.split("\\r\\n")
        for t in tokens_:
            clean_t_ = t.split(":")
        if (len(clean_t_) == 2 and len(clean_t_[0].replace('\\n', '').replace(' ', '').strip()) > 0 and len(
                clean_t_[1].replace('\\n', '').replace(' ', '').strip()) > 0):
            newResult[str(clean_t_[0].replace('\\n', '').replace(' ', '').strip())] = clean_t_[1].replace('\\n',
                                                                                                          '').replace(
                ' ', '').strip()


    return newResult

    # update result document in MongoDB
    # if (newResult != {}):
    # self.collection_go4w_data.update({'Key': result['Key']},{'$set': newResult})

############################################################
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
            mainTabList = soup.find('ul', class_="nav nav-pills center-pills").find_all('li')

            isOurCompanySelected, isProductsSelected, isManagementSelected, isFacilitiesSelector, isNewsRoomSelector = None, None, None, None, None

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

            if ("html" in str(isOurCompanySelected) and (str(isOurCompanySelected)[2:-2] in response.request.url)):  # the second condition for nested page in company page

                companyLogo = None
                if (soup.find('div', class_= "nopadding  mar-top-10 col-xs-offset-0 col-xs-11 gold-menu text-center")):
                    companyLogo = soup.find('div', class_= "nopadding  mar-top-10 col-xs-offset-0 col-xs-11 gold-menu text-center").find('img')['src']

                annualSalesBTagsList = None
                if (soup.find('div', class_="padd-lr-10 mar-top-10 ")):
                    annualSalesBTagsList = soup.find_all('div', class_="padd-lr-10 mar-top-10 ").find('b').text

                annualSalesBTagsValueList = None
                if (soup.find('div', class_="padd-lr-10 mar-top-10 ")):
                    annualSalesBTagsValueList = clean_backslashN_array(soup.find_all('div', class_="padd-lr-10 mar-top-10 ").text)

                ourCompanyText = None
                if (soup.find('div', class_="col-xs-12")):
                    ourCompanyText = soup.find('div', class_="col-xs-12").text

                contactDetailText = None
                if (soup.find('div', class_="padd-lr-10 mar-top-10 ")):
                    contactDetailText = soup.find_all('div', class_="padd-lr-10 mar-top-10 ").find('address').text.replace('<address>',
                                                                                                      '').replace(
                    '</address>', '').replace('<br>', ',').replace('<b>', ',').replace('</b>', ',')

                newResult = {}
                newResult['logoSrc'] = self.clean_text_(str(companyLogo))
                newResult['ourCompany'] = self.clean_text_(ourCompanyText)
                newResult['contactDetail'] = self.clean_text_(contactDetailText)

                for i in range(len(annualSalesBTagsList)):
                    if i < len(annualSalesBTagsValueList):
                        newResult[str(annualSalesBTagsList[i])] = annualSalesBTagsValueList[i].replace(' : ', '')

            #############################################################################################################
            if ((str(isOurCompanySelected)[2:-2] not in response.request.url) and ("html" in str(isOurCompanySelected))):
                nestedURLOurCompany = "https://www.go4worldbusiness.com" + str(isOurCompanySelected)[2:-2]
                # yield scrapy.Request(url=nestedURLOurCompany, callback=self.nestedURLOurCompanyCrawler,
                #                      meta={'inputJson': result})
            if ("html" in str(isProductsSelected)):
                nestedURLProductsCompany = "https://www.go4worldbusiness.com" + str(isProductsSelected)[2:-2]
                # yield scrapy.Request(url=nestedURLProductsCompany, callback=self.nestedURLProductsCompanyCrawler,
                #                      meta={'inputJson': result})
            if ("html" in str(isManagementSelected)):
                nestedURLManagementCompany = "https://www.go4worldbusiness.com" + str(isManagementSelected)[2:-2]
                # yield scrapy.Request(url=nestedURLManagementCompany,
                #                      callback=self.nestedURLManagementCompanyCrawler,
                #                      meta={'inputJson': result})
            if ("html" in str(isFacilitiesSelector)):
                nestedURLFaculitiesCompany = "https://www.go4worldbusiness.com" + str(isFacilitiesSelector)[2:-2]
                # yield scrapy.Request(url=nestedURLFaculitiesCompany,
                #                      callback=self.nestedURLFacilitiesCompanyCrawler,
                #                      meta={'inputJson': result})
            if ("html" in str(isNewsRoomSelector)):
                nestedURLNewsRoomCompany = "https://www.go4worldbusiness.com" + str(isNewsRoomSelector)[2:-2]
                # yield scrapy.Request(url=nestedURLNewsRoomCompany, callback=self.nestedURLNewsRoomCompanyCrawler,
                #                      meta={'inputJson': result})



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
            buyerList = soup.find_all('div', class_ ="col-xs-12 nopadding search-results")

            for searchResultSet in buyerList:

                buyerCompanyName = None
                if (searchResultSet.find('h2',class_="entity-row-title h2-item-title") != None):
                    buyerCompanyName = searchResultSet.find('h2',class_="entity-row-title h2-item-title").find('span').text.strip()
                date =  searchResultSet.find('div',class_="col-xs-3 col-sm-2 xs-padd-lr-2 nopadding").find('small').text.strip()
                buyerProductName = None

                if (searchResultSet.find('h2',class_="text-capitalize entity-row-title h2-item-title") != None):
                    buyerProductName = clean_text_(str(searchResultSet.find('h2',class_="text-capitalize entity-row-title h2-item-title").find('span').text.strip()))

                buyerCountry = clean_text_(str(searchResultSet.find('span',class_="pull-left subtitle text-capitalize").text.strip().replace('Buyer From', '')))
                buyerText = None
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
            supplierList = soup.find_all('div', class_="col-xs-12 nopadding search-results")

            for searchResultSet in supplierList:

                supplierCompanyName = None
                if (searchResultSet.find('h2', class_="entity-row-title h2-item-title") != None):
                    supplierCompanyName = searchResultSet.find('h2', class_="entity-row-title h2-item-title").find(
                        'span').text.strip()
                date = searchResultSet.find('div', class_="col-xs-3 col-sm-2 xs-padd-lr-2 nopadding").find(
                    'small').text.strip()

                supplierCountry = clean_text_(str(
                    searchResultSet.find('span', class_="pull-left subtitle text-capitalize").text.strip().replace(
                        'Supplier From', '')))
                supplierText = None
                if (searchResultSet.find('div', class_="col-xs-12 entity-row-description-search xs-padd-lr-5") != None):
                    supplierText = searchResultSet.find('div',
                                                     class_="col-xs-12 entity-row-description-search xs-padd-lr-5").find(
                        'p').text.strip()

                supplierSupplierOF = clean_text_(
                    str(searchResultSet.find('div', class_="mar-top-10").find('a').text.strip()).replace('Buyer Of',
                                                                                                         ''))
                supplierCompanyLink = None
                if (len(searchResultSet.find_all('span', class_="pull-left")) >= 2):
                    supplierCompanyLink = searchResultSet.find_all('span', class_="pull-left")[1].find('a')['href']
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
                    newResutl = tokenize_buyer_or_supplier_text(result)
                    nestedURLCompany = "https://www.go4worldbusiness.com" + str(result['supplierCompanyLink'])
                    nestedResult = nestedURLGeneralCompanyCrawler (nestedURLCompany, result, s, proxy)

                # outJson.append(result)



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
                        buyerCrawler(nextPageURL, s, proxy)


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
                        supplierCrawler(nextPageURL, s, proxy)

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