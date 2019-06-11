# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerProcess
import json
import urllib
from urllib import request as urlrequest
from socket import timeout
import multiprocessing
import eventlet
import re
import requests
from tabletojson import html_to_json
from bs4 import BeautifulSoup
from selenium import webdriver
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
    while True:
        resp = requests.get(url=url, params=params)
        if resp.status_code == 200:
            resp = requests.get(url='', )
            data = json.loads(resp.text)
            break
        else:
            print('Proxy not Working ... Trying new one.')
            continue

    print('Changing Proxy ... ' + data['proxy'])
    print('********************************************')
    return data['proxy'], data['randomUserAgent']

###########################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: start_urls for scrapy
    description: add products to urls from products_alibaba.json file
    '''
    soup = BeautifulSoup(requests.get("https://www.alibaba.com/Products").content, 'html.parser')
    lis = soup.find_all('li')
    urls = [li.find('a') for li in lis if li]
    urls = [url for url in urls if url is not None]
    urls = [url.attrs['href'] for url in urls if 'pid' in str(url)]

    return urls
############################################################
############################################################
def company_parse(url,data):
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
            req = urlrequest.Request(str(url))
            req.set_proxy(proxy, 'http')
            req.add_header('User-Agent', useragent)
            response = urlrequest.urlopen(req, timeout=5)
            soup = BeautifulSoup(response.read().decode('utf8'), 'html.parser')

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


            #extract data from tables
            parts = soup.select('.infoList-mod-field')
            for p in parts:
                title = p.find('h3').text.strip()
                table = html_to_json(str(p.find('table', recursive=True)))
                if(isListEmpty(table)):
                    data['company_' + str(title)] = data

            data = None

        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy()
                #headers['User-Agent'] = useragent
                continue
        except:
            proxy, useragent = change_proxy()
            #headers['User-Agent'] = useragent
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
    data = {}
    proxy, useragent = '',''
    ########################################################
    while True:
        try:
            req = urlrequest.Request(str(url))
            req.set_proxy(proxy, 'http')
            req.add_header('User-Agent', useragent)
            response = urlrequest.urlopen(req, timeout=5)
            soup = BeautifulSoup(response.read().decode('utf8'), 'html.parser')

            title = soup.findAll('h1')[0].text.strip()
            try:
                price = soup.select("span.ma-ref-price")[0].text.replace("\\n", "").strip()
                min_order = soup.select("span.ma-min-order")[0].text.strip()
                unit = soup.select("span.ma-min-order")[0].text.strip().split('/')[1]
            except:
                print('Not found price and min order of product')
                proxy, useragent = change_proxy()
                #headers['User-Agent'] = useragent
                continue

            if title:
                data['product_name'] = title

            if price:
                data['product_price'] = price

            if min_order:
                data['product_min_order'] = min_order

            if unit:
                data['product_unit'] = unit

            parts = soup.find_all('dl', class_="do-entry-item")
            for p in parts:
                key = re.sub(' +', ' ', p.find('dt').text.strip().replace(':','').replace("\\n", "").replace('\n',''))
                value = re.sub(' +', ' ', p.find('dd').text.strip().replace("\\n", "").replace('\n',''))
                if not 'picture' in str(key):
                    data[('product_' + str(key)).replace(' ','_')] = value

            company_url = soup.select('div.card-footer')[0].find('a').attrs['href']
            data['company_url'] = company_url

            company_parse(company_url, data)


        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy()
                #headers['User-Agent'] = useragent
                continue
        # except:
        #     proxy, useragent = change_proxy()
        #     headers['User-Agent'] = useragent
        #     print('Error Occurred in product_parse function and try again')
        #     continue

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
    #headers['User-Agent'] = useragent
    ########################################################
    for url in urls:
        # Products
        for i in range(1, 101):
            while True:
                try:
                    req = urlrequest.Request(str(url) + "?spm=a2700.galleryofferlist.pagination&page=" + str(i))
                    req.set_proxy(proxy, 'http')
                    req.add_header('User-Agent', useragent)
                    response = urlrequest.urlopen(req, timeout=5)
                    soup = BeautifulSoup(response.read().decode('utf8'), 'html.parser')

                    items = soup.find_all('h2', class_='title')

                    if(len(items) == 0):
                        proxy, useragent = change_proxy()
                        #headers['User-Agent'] = useragent
                        print('cant get list of products')
                        continue

                    items_urls = [i.find('a').attrs['href'] for i in items]

                    for iu in items_urls:
                        product_parse("https:" + str(iu))

                except urllib.error.HTTPError as e:
                    print(e)
                    if (e.code == 403):
                        proxy, useragent = change_proxy()
                        #headers['User-Agent'] = useragent
                        continue
                # except:
                #     proxy, useragent = change_proxy()
                #     #headers['User-Agent'] = useragent
                #     print('Error Occurred in main_parse function and try again')
                #     continue

                else:
                    break

############################################################
############################################################
############################################################
urls = create_category_url()
main_parse(urls)