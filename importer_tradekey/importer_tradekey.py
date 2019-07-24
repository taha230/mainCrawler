# -*- coding: utf-8 -*-
import json
import urllib
import re
from bs4 import BeautifulSoup
from jsonmerge import merge
# from twisted.internet import defer
# from twisted.internet.error import TimeoutError, DNSLookupError, \
#         ConnectionRefusedError, ConnectionDone, ConnectError, \
#         ConnectionLost, TCPTimedOutError
# from twisted.web.client import ResponseFailed
import multiprocessing
# from scrapy.core.downloader.handlers.http11 import TunnelError
import requests
import requests.exceptions
import time
import warnings
import sys
from requests.auth import HTTPBasicAuth

sys.path.append("/file")
warnings.filterwarnings("ignore")


##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}
EXCEPTIONS_TO_RETRY = (IOError, TimeoutError, ConnectionRefusedError ) #(defer.TimeoutError, TunnelError, DNSLookupError, ConnectionDone, ConnectError, ConnectionLost, TCPTimedOutError, ResponseFailed,



###########################################################
def create_all_supplier_url(proxy, pages, process):

    '''
        function_name: create_all_buyer_url
        input: proxy
        output: third phase of crawler run in clery
        description: add all buyer of category files
        '''

    processed = 0
    for url in list(pages):
        # print(url)

        while True:
            try:
                s = requests.session()
                con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
                if (con.status_code == 404):
                    break
                else:
                    html = con.content
                    soup = BeautifulSoup(html, 'html.parser')

                    if (soup.find_all('div', class_="description_block")):

                        for product in soup.find_all('div', class_="description_block"):
                            if (product.find('a', class_="title") and product.find('a', class_="title")['href']):
                                file_all_product_categories_url.write(product.find('a', class_="title")['href'] + '\n')


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

        processed += 1
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(pages)))
        if processed > 5 : break

    file_all_product_categories_url.close()

###########################################################
def create_all_buyer_url(proxy, pages, process):

    '''
        function_name: create_all_buyer_url
        input: proxy
        output: third phase of crawler run in clery
        description: add all buyer of category files
        '''



    processed = 0
    for url in list(pages):
        # print(url)

        while True:
            try:
                s = requests.session()
                con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
                if (con.status_code == 404):
                    break
                else:
                    html = con.content
                    soup = BeautifulSoup(html, 'html.parser')

                    if (soup.find_all('div', class_="description")):

                        for product in soup.find_all('div', class_="description"):
                            if (product.find('a', class_="title") and product.find('a', class_="title")['href']):
                                file_all_product_categories_url.write(product.find('a', class_="title")['href'] + '\n')

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

        processed = processed + 1
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(pages)))
        if (processed > 5): break

    file_all_product_categories_url.close()

###########################################################
def create_all_product_url(proxy, pages, process):

    '''
        function_name: create_all_pages_category_url
        input: proxy
        output: third phase of crawler run in clery
        description: add all product of category files
        '''
    processed = 0
    for page in pages:
        outList=[]
        url = page

        while True:
            try:
                s = requests.session()
                con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify = False)
                if (con.status_code == 404): break
                else:
                    html = con.content
                    soup = BeautifulSoup(html, 'html.parser')

                    for product in soup.find_all('a', class_="product_title"):
                        if (product['href']):
                            #outList.append (product['href'] + '\n')
                            file_all_product_categories_url.write(product['href'] + '\n')
                            # print(product['href'])
                    break
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

        processed = processed + 1
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(pages)))
        if (processed > 5): break

###########################################################
def create_category_url_product(proxy):
    '''
    function_name: create_category_url
    input: none
    output: first phase of crawler run in clery
    description: add categories urls to categories_importer_tradekey.json file
    '''
    categories = []

    f_categories = open('categories_importer_tradekey_product.txt', 'w')

    url = 'https://www.tradekey.com/product_cat.htm'

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
            print('Error Occurred in creating categories url function product and try again')
            time.sleep(30)
            continue

        except Exception as e:
            print('Exception ' + str(e) + ' occured in creating categories urls product in url:' + url)
            return {}

        else:
            break


    f_categories.close()

###########################################################
def create_category_url_buyyer(proxy):
    '''
    function_name: create_category_url
    input: none
    output: first phase of crawler run in clery
    description: add categories urls to categories_importer_tradekey.json file
    '''
    categories = []

    f_categories = open('categories_importer_tradekey_buyer.txt', 'w')

    url = 'https://importer.tradekey.com/'

    while True:
        try:
            html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
            soup = BeautifulSoup(html, 'html.parser')

            if (soup.find_all('a', class_="smalllinkb")):
                for a in soup.find_all('a', class_="smalllinkb"):
                    ##############################################
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
            print('Error Occurred in creating categories url function product and try again')
            time.sleep(30)
            continue

        except Exception as e:
            print('Exception ' + str(e) + ' occured in creating categories urls product in url:' + url)
            return {}

        else:
            break

    f_categories.close()

###########################################################
def create_category_url_supplier(proxy):
    '''
    function_name: create_category_url
    input: none
    output: first phase of crawler run in clery
    description: add categories urls to categories_importer_tradekey.json file
    '''
    categories = []

    f_categories = open('categories_importer_tradekey_supplier.txt', 'w')

    url = 'https://www.tradekey.com/profile_cat.htm'

    while True:
        try:
            html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
            soup = BeautifulSoup(html, 'html.parser')

            if (soup.find_all('a', class_="smalllinkb")):
                for a in soup.find_all('a', class_="smalllinkb"):
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
            print('Error Occurred in creating categories url function supplier and try again')
            time.sleep(30)
            continue

        except Exception as e:
            print('Exception ' + str(e) + ' occured in creating categories urls supplier in url:' + url)
            return {}

        else:
            break

    f_categories.close()

############################################################
def create_category_url_product_layer2(proxy, urls, process):
    '''
    function_name: create_category_url
    input: none
    output: first phase of crawler run in clery
    description: add categories urls to categories_importer_tradekey.json file
    '''
    processed = 0

    for url in urls:

        while True:
            try:
                html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find_all('a', class_="main-category")):
                    for a in soup.find_all('a', class_="main-category"):
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
                print('Error Occurred in creating categories url function product and try again')
                time.sleep(30)
                continue

            except Exception as e:
                print('Exception ' + str(e) + ' occured in creating categories urls product in url:' + url)
                return {}

            else:
                break

        processed += 1
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(urls)))

###########################################################
def create_category_url_buyyer_layer2(proxy, urls, process):
    '''
    function_name: create_category_url
    input: none
    output: first phase of crawler run in clery
    description: add categories urls to categories_importer_tradekey.json file
    '''

    processed = 0
    for url in urls:

        while True:
            try:
                html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find_all('a', class_="main-category")):
                    for a in soup.find_all('a', class_="main-category"):
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
                print('Error Occurred in creating categories url function product and try again')
                time.sleep(30)
                continue

            except Exception as e:
                print('Exception ' + str(e) + ' occured in creating categories urls product in url:' + url)
                return {}

            else:
                break
        processed +=1
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(urls)) )

###########################################################
def create_category_url_supplier_layer2(proxy, urls, process):
    '''
    function_name: create_category_url
    input: none
    output: first phase of crawler run in clery
    description: add categories urls to categories_importer_tradekey.json file
    '''

    processed = 0
    for url in urls:

        while True:
            try:
                html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find_all('a', class_="main-category")):
                    for a in soup.find_all('a', class_="main-category"):
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
                print('Error Occurred in creating categories url function supplier and try again')
                time.sleep(30)
                continue

            except Exception as e:
                print('Exception ' + str(e) + ' occured in creating categories urls supplier in url:' + url)
                return {}

            else:
                break

        processed += 1
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(urls)))


############################################################
def create_all_pages_category_url_buyer(proxy, categories, process):
    '''
    function_name: create_all_pages_category_url
    input: proxy
    output: second phase of crawler run in clery
    description: add all pages of category files
    '''


    processed = 0
    for url in list(categories):
        print(url)

        while True:
            try:
                html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find('div' , class_="paging_showing") and len(soup.find('div' , class_="paging_showing").text.split('of')) > 1):
                    totalPages = int(soup.find('div' , class_="paging_showing").text.split('of')[1].strip())
                    for i in range(totalPages):
                        file_all_pages_categories_url.write(url.replace('.htm\n','') + '/page_no/' + str(i+1)+'.htm \n')

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

        processed += 1
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(categories)))

    file_all_pages_categories_url.close()

############################################################
def create_all_pages_category_url_product(proxy, categories, process):
    '''
    function_name: create_all_pages_category_url_product
    input: proxy
    output: second phase of crawler run in clery
    description: add all pages of category files
    '''

    processed = 0
    for url in list(categories):
        print(url)
        url = 'https://www.tradekey.com/Pharmaceutical-Machinery_pd4142.htm\n'

        while True:
            try:
                html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find('div', id="navcontainer") and soup.find('div', id="navcontainer").find_all('td')):
                    for td in soup.find('div', id="navcontainer").find_all('td'):
                        if 'of' not in td.text or 'Page' not in td.text or len(td.text.split('of')) <= 1: continue

                        totalPages = int(td.text.split('of')[1].strip())
                        for i in range(totalPages):
                            newURL = url.replace('.htm\n', '').replace('_pd','_pid') + '/page_no/' + str(i + 1) + '.htm \n'
                            file_all_pages_categories_url.write(newURL)

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

        processed += 1
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(categories)))


    file_all_pages_categories_url.close()

############################################################
def create_all_pages_category_url_supplier(proxy, categories, process):
    '''
    function_name: create_all_pages_category_url_supplier
    input: proxy
    output: second phase of crawler run in clery
    description: add all pages of category files for supplier
    '''


    processed= 0
    for url in list(categories):
        # print(url)

        while True:
            try:
                html = requests.get(url, proxies={'http': proxy}, headers=headers, timeout=5).content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find('div', id="navcontainer") and soup.find('div', id="navcontainer").find_all('div')):
                    for div in soup.find('div', id="navcontainer").find_all('div'):
                        if 'of' not in div.text or 'Page' not in div.text or len(div.text.split('of')) <= 1: continue

                        totalPages = int(div.text.split('of')[1].strip())
                        for i in range(totalPages):
                            string_page = 'page_no/' + str(i + 1)+ '/'
                            category_string = url.split('/')[len(url.split('/'))-1]
                            url_page = url.replace(category_string,'') + string_page + category_string
                            file_all_pages_categories_url.write(url_page)

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

        processed += 1
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(categories)))

    file_all_pages_categories_url.close()


#########################################################

def crawlSupplier_Products(proxy , url):

    '''
        function_name: crawlSupplier_Products
        input: proxy, url
        output: outjson
        description: crawl product information of supplier page
    '''

    outJson = {}

    while True:
        try:
            s = requests.session()
            con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
            if (con.status_code == 404):
                break
            else:
                html = con.content
                soup = BeautifulSoup(html, 'html.parser')
                productList = []
                if (soup.find('div' , id = 'product-first-section')):
                    for product in soup.find('div' , id = 'product-first-section').find_all('div', class_= 'big-rows'):
                        productJson = {}
                        if (product.find('div', class_= 'listing-big-title') and product.find('div', class_= 'listing-big-title').find('a')):
                            productJson['productTitle'] = product.find('div', class_= 'listing-big-title').find('a').text.strip()

                        if (product.find('div', class_='listing-big-img') and product.find('div', class_='listing-big-img').find('a') and
                            len(product.find('div', class_='listing-big-img').find('a').get('rel'))>0):
                            productJson['ProductImageURL'] = product.find('div', class_='listing-big-img').find('a').get('rel')[0].strip()

                        if (product.find('div', class_='listing-big-desc') and product.find('div', class_='listing-big-desc').find('p')):
                            productJson['productText'] = product.find('div', class_='listing-big-desc').find('p').text.strip()

                        productList.append(productJson)

                outJson['productList'] = productList

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

    return outJson

#########################################################

def crawlSupplier_SellOffers(proxy , url):

    '''
        function_name: crawlSupplier_Products
        input: proxy, url
        output: outjson
        description: crawl product information of supplier page
    '''

    outJson = {}

    while True:
        try:
            s = requests.session()
            con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
            if (con.status_code == 404):
                break
            else:
                html = con.content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find('div', id='product-first-section') and soup.find('div', id='product-first-section').find('h4')):
                    outJson['SellOffers'] = soup.find('div', id='product-first-section').find('h4').text.strip()


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

    return outJson
#########################################################

def crawlSupplier_ContactDetails(proxy , url):

    '''
        function_name: crawlSupplier_Products
        input: proxy, url
        output: outjson
        description: crawl product information of supplier page
    '''

    outJson = {}

    while True:
        try:
            s = requests.session()
            con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
            if (con.status_code == 404):
                break
            else:
                html = con.content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find('div', class_= 'contact-info')):
                    for div in soup.find('div', class_='contact-info').find_all('div', class_= 'ci-details'):
                        outJson[div.find('label').text.strip().replace(':', '')] = clean_text_(div.find('p').text.strip())



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

    return outJson
#########################################################

def crawlSupplier_TrustProfile(proxy , url):

    '''
        function_name: crawlSupplier_Products
        input: proxy, url
        output: outjson
        description: crawl product information of supplier page
    '''

    outJson = {}

    while True:
        try:
            s = requests.session()
            con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
            if (con.status_code == 404):
                break
            else:
                html = con.content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find('div', id= 'bi-body')):
                    for div in soup.find('div', id='bi-body').find_all('div', class_= 'ci-details'):
                        outJson[div.find('label').text.strip().replace(':', '')] = clean_text_(div.find('p').text.strip())

                if (soup.find('div', class_= 'tp-body') and soup.find('div', class_= 'tp-body').find('p')):
                    outJson['TrustProfile_text'] = clean_text_(soup.find('div', class_= 'tp-body').find('p').text.strip())

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

    return outJson

#########################################################

def crawlSupplier_Brochures(proxy , url):

    '''
        function_name: crawlSupplier_Products
        input: proxy, url
        output: outjson
        description: crawl product information of supplier page
    '''

    outJson = {}

    while True:
        try:
            s = requests.session()
            con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
            if (con.status_code == 404):
                break
            else:
                html = con.content
                soup = BeautifulSoup(html, 'html.parser')

                if (soup.find('div' , id = 'product-first-section') and soup.find('div' , id = 'product-first-section').find('h4')):
                    outJson['Brochures'] = soup.find('div' , id = 'product-first-section').find('h4').text.strip()


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

    return outJson

############################################################

def crawlSupplier(proxy , urls , process):

    '''
        function_name: crawlSupplier
        input: proxy, urls, process
        output: last phase of crawler run in clery
        description: create json output of supplier form product page
    '''


    processed = 0
    for url in list(urls):

        url = 'https://www.tradekey.com/company/Jiangyin-Daqiao-stainless-steel-tube-CoLtd-707169.html'

        outJson = {}
        while True:
            try:
                s = requests.session()
                con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
                if (con.status_code == 404):
                    break
                else:
                    html = con.content
                    soup = BeautifulSoup(html, 'html.parser')

                    if (soup.find('div', class_='company-name') and soup.find('div', class_='company-name').find('a') and soup.find('div', class_='company-name').find('a').find('span')):
                        outJson['companyName'] = soup.find('div', class_='company-name').find('a').find('span').text.strip()
                    if (soup.find('div', class_='company-name') and soup.find('div', class_='company-name').find('p') and soup.find('div', class_='company-name').find('p').find('span')):
                        outJson['companyAddress'] = soup.find('div', class_='company-name').find('p').find('span').text.strip()
                    if (soup.find('div' , id == 'product-body') and soup.find('div' , id == 'product-body').find('p')):
                        outJson['productText'] = soup.find('div' , id == 'product-body').find('p').text().strip()



                    if (soup.find('div', id='basic-info')):
                        for div in soup.find('div', id='basic-info').find_all('div', class_='ci-details'):
                            outJson[div.find('label').text.strip().replace(':', '')] = clean_text_(
                                div.find('p').text.strip())

                    if (soup.find('div', id='section-factory-info')):
                        for div in soup.find('div', id='section-factory-info').find_all('div', class_='ci-details'):
                            outJson[div.find('label').text.strip().replace(':', '')] = clean_text_(
                                div.find('p').text.strip())

                    if (soup.find('div', id='section-other-info')):
                        for div in soup.find('div', id='section-factory-info').find_all('div', class_='ci-details'):
                            outJson[div.find('label').text.strip().replace(':', '')] = clean_text_(
                                div.find('p').text.strip())



                    ####################################################################################################
                    productsJson, Sell_OffersJson, Contact_DetailsJson, Trust_ProfileJson, BrochuresJson= {}, {}, {}, {}, {}
                    for li in soup.find_all('li', class_='nav_btn'):
                        if (li.find('a') and li.find('a')['href']):

                            if ('Products' in li.find('a').get('title')):
                                productsJson = crawlSupplier_Products(proxy, li.find('a').get('href'))
                            if (False and 'Sell Offers' in li.find('a').get('title')):
                                Sell_OffersJson = crawlSupplier_SellOffers(proxy, li.find('a').get('href'))
                            if ('Contact Details' in li.find('a').get('title')):
                                Contact_DetailsJson = crawlSupplier_ContactDetails(proxy, li.find('a').get('href'))
                            if (False and 'Trust Profile' in li.find('a').get('title')):
                                Trust_ProfileJson = crawlSupplier_TrustProfile(proxy, li.find('a').get('href'))
                            if (False and 'Brochures' in li.find('a').get('title')):
                                BrochuresJson = crawlSupplier_Brochures(proxy, li.find('a').get('href'))



                    merge(outJson , productsJson)
                    merge(outJson, Sell_OffersJson)
                    merge(outJson, Contact_DetailsJson)
                    merge(outJson, Trust_ProfileJson)
                    merge(outJson, BrochuresJson)



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

        processed += 1
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(urls)))
        if processed > 5 : break

    f_supplier.close()

############################################################

def crawlBuyer(proxy , urls , process):

    '''
        function_name: crawlBuyer
        input: proxy, urls, process
        output: last phase of crawler run in clery
        description: create json output of buyer form buyer page
    '''


    processed = 0
    for url in list(urls):
        # url = 'https://importer.tradekey.com/buyoffer/Interested-In-Purchasing-Pet-Bottle-Scrap-1411588.html'
        outJson = {}

        while True:
            try:
                s = requests.session()
                con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
                if (con.status_code == 404):
                    break
                else:
                    html = con.content
                    soup = BeautifulSoup(html, 'html.parser')

                    if (soup.find('div', class_= 'bo-buyoffer-box') and soup.find('div', class_= 'bo-buyoffer-box').find('h1')):
                        outJson['buyerTitle'] = soup.find('div', class_= 'bo-buyoffer-box').find('h1').text.strip()
                    if (soup.find('div', class_= 'bo-buyoffer-box') and soup.find('div', class_= 'bo-buyoffer-box').find('ul')):
                        for li in soup.find('div', class_= 'bo-buyoffer-box').find('ul').find_all('li'):
                            if (li.find('span') and li.find('strong')):
                                if ('img' in str(li)):
                                    outJson['buyerCountry'] = li.find('span').find('img').get("title").strip()
                                else:
                                    outJson[li.find('span').text.strip().replace(':', '')] = li.find('strong').text.strip()


                    if (soup.find('div', class_= 'bo-desc') and soup.find('div', class_= 'bo-desc').find('h4') and soup.find('div', class_= 'bo-desc').find('p')):
                        outJson[soup.find('div', class_= 'bo-desc').find('h4').text.strip()] = clean_text_(soup.find('div', class_= 'bo-desc').find('p').text.strip())

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

        processed += 1
        file_all_buyer.write(json.dumps(outJson) + '\n')
        print("process : " + str(process) + ' ------- ' + str(processed) + '  from  ' + str(len(urls)))
        # if processed > 10 : break

    f_buyer.close()
    file_all_buyer.close()

############################################################

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
        apiKey='YEXDtBuyrKq3obRLwC4PUQmTZN2SjcxV',
        connectionType = 'Datacenter'
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
def login():
    login_url = "https://www.tradekey.com/index.html?action=login_signin"

    s = requests.session()
    con = s.get(login_url, proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
    if (con.status_code == 404):
        return
    else:

        payload = {
            'username': 'amootiranianInfo@gmail.com',
            'password': 'Amoot123456',
            'remember_me': '1'
        }


        # headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36'
        # headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3'
        # headers['Accept-Encoding'] = 'gzip, deflate, br'
        # headers['Accept-Language'] = 'en-US,en;q=0.9,fa;q=0.8'
        # headers['Cache-Control'] = 'max-age=0'
        # headers['Connection'] = 'keep-alive'
        # headers['Content-Length'] = '95'
        # headers['Content-Type'] = 'application/x-www-form-urlencoded'
        # headers['Cookie'] = 'PHPSESSID=g2tcobuuuktnao8bkc6oej8473; c_popup_required=no; __auc=b5d1126e16bf5883d49b27122a6; c_login_required=no; timezone=4.5; _ga=GA1.2.250903019.1563192957; __tawkuuid=e::tradekey.com::SlA6+Cow9dUw2h9MMg3fXkqEYmBRx8V2gXHGyWi6ueUa5AhmFQxL2tyhHcavonVf::2; open=yes; viewedOuibounceModal=true; banner_showed=1; timezone=4.5; c_memtype=3; c_name=ali+alavi+; c_email=amootiranianinfo%40gmail.com; c_userid=12122905; c_buyer_seller=1; __utmc=22121982; __utmz=22121982.1563699175.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); signup_popu_dely_two=1; _sbcnt=1; pm_inquiry=1; sbalg=1; _sbapp=1; bclg=3; c_member_posting_cookie=1; __asc=b39caa9316c1df58af4fbb31844; __utma=22121982.250903019.1563192957.1563867010.1563870726.6; TawkConnectionTime=0; __utmt=1; __utmb=22121982.9.10.1563870727'
        # headers['Host'] = 'www.tradekey.com'
        # headers['Origin'] = 'https://www.tradekey.com'
        headers['Referer'] = 'https://www.tradekey.com/index.html?action=login_signin'
        # headers['Upgrade-Insecure-Requests'] = '1'

        result = s.post(login_url, proxies={'http': proxy}, data = payload,  headers=headers)

        url = 'https://www.tradekey.com/profile_contact/uid/707169/Jiangyin-Daqiao-stainless-steel-tube-Co-Ltd.htm'

        outJson= {}
        con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
        if (con.status_code == 404):
            return
        else:
            html = con.content
            soup = BeautifulSoup(html, 'html.parser')


            if (soup.find('div', id='ci-body')):
                for div in soup.find('div', id='ci-body').find_all('div', class_='ci-details'):
                    outJson[div.find('label').text.strip().replace(':', '')] = clean_text_(div.find('p').text.strip())

    return outJson







f_categories = open('files/categories_importer_tradekey_buyer.txt', 'a+')
f_categories_read = open('files/categories_importer_tradekey_buyer.txt', 'r')

# file_all_pages_categories_url = open('files/categories_all_pages_importer_tradekey_supplier.txt', 'w')
# f_category = open("files/categories_importer_tradekey_supplier.txt", "r")

# file_all_product_categories_url = open('files/all_buyer_importer_tradekey.txt', 'w')
# f_page = open("files/categories_all_pages_importer_tradekey_buyer.txt", "r")

f_buyer = open('files/all_buyer_importer_tradekey.txt', 'r')
file_all_buyer = open('files/BuyerOut.txt', 'w')

f_supplier = open('files/all_supplier_importer_tradekey.txt', 'r')
file_all_supplier = open('files/SupplierOut.txt', 'w')


urls = f_categories_read.readlines()
# categories = f_category.readlines()
# pages = f_page.readlines()
buyers = f_buyer.readlines()
suppliers = f_supplier.readlines()



proxy, useragent = change_proxy()

login()

# headers['User-Agent'] = useragent

# create_category_url_buyyer(proxy)
# create_category_url_product(proxy)
# create_category_url_supplier(proxy)


# create_category_url_buyyer_layer2(proxy)
# create_category_url_product_layer2(proxy)
# create_category_url_supplier_layer2(proxy)

# create_all_pages_category_url_buyer(proxy)
# create_all_pages_category_url_product(proxy)
# create_all_pages_category_url_supplier(proxy)

# create_all_product_url(proxy)
# create_all_buyer_url(proxy)
# create_all_supplier_url(proxy)

crawlSupplier(proxy, suppliers, 1)
# crawlBuyer(proxy, buyers, 1)


# number_processes = 5
# parts = chunkIt(pages, number_processes)
#
# processes = []
#
# for i in range(number_processes):
#     processes.append(multiprocessing.Process(target=create_all_buyer_url, args=[proxy,parts[i],i]))
#     # break
#
# for p in processes:
#     p.start()
#     # break
#
# for p in processes:
#     p.join()
#     # break


# f_categories.close()
# file_all_product_categories_url.close()
