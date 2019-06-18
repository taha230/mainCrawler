# -*- coding: utf-8 -*-
import json
import multiprocessing
import re
from random import randint
from incapsula import IncapSession
import requests
import mechanicalsoup
import urllib
import multiprocessing
from bs4 import BeautifulSoup
from selenium import webdriver
from incapsula import IncapSession
import time
##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

#f = open('alibaba_result.json','a')
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
###################################################
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

###########################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: list
    description: get link of all top categories page
    '''
    with open('madeInChinaTotalUrls.txt') as f:
        urls = f.readlines()

    urls = [x.strip() for x in urls]

    return urls
############################################################
def company_parse(url,data, s):
    '''
    function_name: product_parse
    input: url
    output: none
    description: crawl product page
    '''
    proxy, useragent = '',''

    ########################################################
    while True:
        try:
            #company description
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')

            company_name = soup.select('span.title-text')[0].text.strip()
            company_join_year = soup.findAll('span', class_='join-year')[0].find('span').text.strip()
            company_description = soup.find('div', class_='company-card-desc').find('div').text.strip()
            data['company_name'] = company_name
            data['company_join_year'] = company_join_year
            data['company_description'] = company_description

            #transaction description
            keys = [i.text.strip() for i in soup.select('div.transaction-detail-title')]
            values = [i.text.strip() for i in soup.select('div.transaction-detail-content')]
            for i in range(0, len(values)):
                if values[i] is not None and values[i] != 'Hidden':
                    data[('company_' + str(keys[i])).replace(' ','_')] = values[i]

            #company basic info
            basicinfo = table_to_json(str(soup.select('table.company-basicInfo')[0]))
            data['company_basic_info'] = basicinfo

            #extract data from tables
            parts = soup.select('.infoList-mod-field')
            for p in parts:
                title = p.find('h3').text.strip()
                tables = p.find_all('table', recursive=True)
                if (len(tables) == 1):
                    table = table_to_json(str(tables[0]))
                    if(table):
                        data[('company_' + str(title)).replace(' ','_')] = table
                elif (len(tables) > 1):
                    table = table_to_json_complex(tables)
                    if(len(table) > 0):
                        data[('company_' + str(title)).replace(' ','_')] = table

            data = None
            ###read data to file
            #f.write(json.dumps(data))
            #f.write('\n')

        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy()
                s.headers.update({'User-Agent': useragent})
                continue
        except:
            proxy, useragent = change_proxy()
            s.headers.update({'User-Agent': useragent})
            print('Error Occurred in company_parse function and try again')
            continue

        else:
            break
############################################################
############################################################
def product_parse(url):
    '''
    function_name: product_parse
    input: url
    output: none
    description: crawl product page
    '''
    ########################################################
    while True:
        try:
           proxy, useragent = change_proxy()
           headers['User-Agent'] = useragent
           html = requests.get(str(url), proxies={'http': proxy}, headers=headers).content
           soup = BeautifulSoup(html, 'html.parser')

           product_links = soup.select('h2.product-name')

           if(len(product_links) > 0):
                return [('https:' + p.find('a').attrs['href']) for p in product_links]


        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                continue
        except:
            proxy, useragent = change_proxy()
            headers['User-Agent'] = useragent
            print('Error Occurred in product_parse function and try again')
            continue


###########################################################
###########################################################
def main_parse(urls):
    '''
    function_name: main_parse
    input: list
    output: none
    description: first level of crawling
    '''
    ########################################################
    total_urls = []
    for url in urls:
        while True:
            try:
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                html = requests.get(str(url), proxies={'http': proxy}, headers=headers).content
                soup = BeautifulSoup(html, 'html.parser')

                product_links = soup.select('h2.product-name')

                if(len(product_links) > 0):
                    [total_urls.append('https:' + p.find('a').attrs['href']) for p in product_links]

                    total_pages = int(soup.select('a.page-dis')[0].text)

                    main_addres = 'https:' + '-'.join(soup.select('div.page-num')[0].find('a').attrs['href'].split('.html')[0].split('-')[:-1])
                    for i in range(3,total_pages):
                        lls = product_parse(main_addres + '-' + str(i) + '.html')
                        [total_urls.append(l.attrs['href']) for l in lls]
                    break

            except urllib.error.HTTPError as e:
                 print(e)
                 if (e.code == 403):
                    proxy, useragent = change_proxy()
                    headers['User-Agent'] = useragent
                    continue
            except:
                 proxy, useragent = change_proxy()
                 headers['User-Agent'] = useragent
                 print('Error Occurred in main_parse function and try again')
                 continue

############################################################
############################################################
############################################################
#f = open('alibaba_result.json','a')

urls = create_category_url()
product_urls = main_parse(urls)




#parts = chunkIt(urls, 5)

processes = []

#for i in [0,1,2,3,4]:
#    processes.append(multiprocessing.Process(target=main_parse, args=[i,parts[i]]))


#for p in processes:
#    p.start()

#for p in processes:
#    p.join()


#f.close()