# -*- coding: utf-8 -*-
import json
import urllib
import re
from bs4 import BeautifulSoup
from jsonmerge import merge
import multiprocessing
import requests
from jsonmerge import merge
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

######################################################################################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: start_urls for scrapy
    description: add products to urls from products_go4w.json file
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
######################################################################################################################
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

######################################################################################################################
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
            continue

#####################################################################################################################
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
#####################################################################################################################
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

#####################################################################################################################
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
####################################################################################################################
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
####################################################################################################################
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
####################################################################################################################
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
####################################################################################################################
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
#####################################################################################################################
def get_data_from_html(tokens, name_part):
    '''
    function_name: get_data_from_html
    input: list of string, string
    output: json
    description: extract data from html using html comment tags
    '''
    data = {}
    for i in range(0, len(tokens)):
        if tokens[i].strip().replace(' ', '') == '<!--Title-->':
            data[name_part + '_' + 'title'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip()
            continue

        if tokens[i].strip().replace(' ', '') == '<!--Country-->':
            data[name_part + '_' + 'country'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip().split('From')[
                -1]
            continue

        if tokens[i].strip().replace(' ', '') == '<!--Date-->':
            data[name_part + '_' + 'date'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip()
            continue

        if tokens[i].strip().replace(' ', '') == '<!--Description-->':
            keys = tokens[i + 1].split('<b>')

            for k in keys:
                k_ = k.split('</b>')
                if len(k_) == 2:
                    key = k_[0].replace(':', '').strip()
                    value = ' '.join(
                        BeautifulSoup(k_[1].replace(':', '').strip(), 'html.parser').text.replace('\n', '').split())

                    data[name_part + '_' + key] = value

        if tokens[i].strip().replace(' ', '') == '<!--CategoryStub-->':
            cats = BeautifulSoup(tokens[i + 1], 'html.parser').find_all('a')
            if (len(cats) > 0):
                cats = [c.text.replace('Supplier Of', '').replace('Buyer Of', '').strip() for c in cats]

            data[name_part + '_' + 'categories'] = cats

        return data
#####################################################################################################################
def product_parse(url):
    '''
    function_name: product_parse
    input: json
    output: json
    description: extract data from product page
    '''

    json_url = json.loads(url.replace("\'","\""))

    url = json_url['url']

    while True:
        try:
            if 'proxy' in locals():
                session = requests.session()
                html = session.get(url, proxies={'https': proxy}, headers=headers, timeout=30, verify=False).content
            else:
                session = requests.session()
                html = session.get(url, headers=headers, timeout=30).content

            soup = BeautifulSoup(html, 'html.parser')

            data = {}

            if json_url['type'] == 'supplier':
                data['type'] = 'supplier'

                parts = soup.find('ul', {'class': 'nav-pills'}).findAll('li')
                name_part = parts[0].text.strip()

                ####tokenize html
                tokens = re.compile('(<!--.*?-->)').split(str(soup.select('.body-container')))
                data = merge(data, get_data_from_html(tokens, name_part))
                for i in range(1,len(parts)):




            elif json_url['type'] == 'buyer':
                data['type'] = 'buyer'



        except Exception as e:
            print(e)
            proxy, useragent = change_proxy()
            headers['User-Agent'] = useragent
            continue


###########################################################
def main_parse(p, urls):

    '''
    function_name: main_parse
    input: list
    output: none
    description: first level of crawling
    '''

    ########################################################
    cnt_url = 0
    for url in urls:
        # categories
        while True:
            try:
                if 'proxy' in locals():
                    session = requests.session()
                    html = session.get(url, proxies={'https': proxy}, headers=headers, timeout=30, verify=False).content
                else:
                    session = requests.session()
                    html = session.get(url, headers=headers, timeout=30).content

                soup = BeautifulSoup(html, 'html.parser')

                links = soup.select('div.entity-rows-container')

                if (len(links) > 0):
                    if '/buyers/' in url:
                        for l in links:
                            if l.find('a'):
                                temp = {}
                                temp['url'] = 'https://www.go4worldbusiness.com' + l.find('a').attrs['href']
                                temp['type'] = 'buyer'
                                temp['category'] = str(url).split('/')[-1].split('?')[0].split('.')[0]

                                f.write(str(temp) + '\n')
                    if '/suppliers/' in url:
                        for l in links:
                            if l.find('a'):
                                temp = {}
                                temp['url'] = 'https://www.go4worldbusiness.com' + l.find('a').attrs['href']
                                temp['type'] = 'supplier'
                                temp['category'] = str(url).split('/')[-1].split('?')[0].split('.')[0]

                                f.write(str(temp) + '\n')
                else:
                    proxy, useragent = change_proxy()
                    headers['User-Agent'] = useragent
                    print('Links not found...')
                    continue

                cnt_url = cnt_url + 1
                print(f'process {p}: {cnt_url} from {len(urls)} has been done.')
                break
            except Exception as e:
                print(e)
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                continue
###############################################################################33
with open('files/go4w_products/go4w_products_pages-1.json') as ff:
    urls = ff.readlines()

f = open('files/go4w_final_results/go4w_products_pages.json', 'w')

urls = [str(url).strip() for url in urls]

for url in urls:
    product_parse(url)

# number_processes = 6
# parts = chunkIt(urls, number_processes)
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

f.close()



