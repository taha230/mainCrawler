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

############################################################

def crawlBuyer(proxy , urls , process):

    '''
        function_name: crawlProduct
        input: proxy, urls, process
        output: last phase of crawler run in clery
        description: create json output of products form product page
    '''

    processed = 0
    for url in list(urls):

        while True:
            try:
                s = requests.session()
                con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
                if (con.status_code == 404):
                    break
                else:
                    html = con.content
                    soup = BeautifulSoup(html, 'html.parser')



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

    f_buyer.close()


############################################################

def crawlSupplier(proxy , urls , process):

    '''
        function_name: crawlSupplier
        input: proxy, urls, process
        output: last phase of crawler run in clery
        description: create json output of supplier form product page
    '''


    outJson = {}
    processed = 0
    for url in list(urls):

        while True:
            try:
                s = requests.session()
                con = s.get(url.strip(), proxies={'http': proxy}, headers=headers, timeout=30, verify=False)
                if (con.status_code == 404):
                    break
                else:
                    html = con.content
                    soup = BeautifulSoup(html, 'html.parser')

                    if (soup.find('div.company-name') and soup.find('div.company-name').find('a') and soup.find('div.company-name').find('a').find('span')):
                        outJson['companyName'] = soup.find('div.company-name').find('a').find('span').text.strip()
                    if (soup.find('div.company-name') and soup.find('div.company-name').find('p') and soup.find('div.company-name').find('p').find('span')):
                        outJson['companyAddress'] = soup.find('div.company-name').find('p').find('span').text.strip()
                    if (soup.find('div' , id == 'product-body') and soup.find('div' , id == 'product-body').find('p')):
                        outJson['productText'] = soup.find('div' , id == 'product-body').find('p').text().strip()
                    if (soup.find_all('div', class_= 'ci-details')):
                        for div in soup.find_all('div', class_= 'ci-details'):
                            if (div.find('label') and div.find('p')):
                                outJson[div.find('label').text.strip()] = div.find('p').text.strip()



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

    f_buyer.close()

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



############################################################




f_categories = open('categories_importer_tradekey_buyer.txt', 'a+')
f_categories_read = open('categories_importer_tradekey_buyer.txt', 'r')

# file_all_pages_categories_url = open('categories_all_pages_importer_tradekey_supplier.txt', 'w')
# f_category = open("categories_importer_tradekey_supplier.txt", "r")

# file_all_product_categories_url = open('all_buyer_importer_tradekey.txt', 'w')
# f_page = open("categories_all_pages_importer_tradekey_buyer.txt", "r")

f_buyer = open('all_buyer_importer_tradekey.txt', 'r')
file_all_buyer = open('BuyerOut.txt', 'w')

f_supplier = open('all_supplier_importer_tradekey.txt', 'r')
file_all_supplier = open('SupplierOut.txt', 'w')


urls = f_categories_read.readlines()
# categories = f_category.readlines()
# pages = f_page.readlines()
buyers = f_buyer.readlines()
suppliers = f_supplier.readlines()



proxy, useragent = change_proxy()
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
