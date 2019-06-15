# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerProcess
import json
import urllib
from incapsula import IncapSession
import multiprocessing
import re
import requests
from bs4 import BeautifulSoup
from urllib import request as urlrequest
##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla\/5.0 (compatible MSIE 10.0 Windows Phone 8.0 Trident\/6.0 IEMobile\/10.0 ARM Touch NOKIA Lumia 520)',
    'referer': ''
}
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

###########################################################
def read_category_url():
    '''
    function_name: read_category_url
    input: none
    output: start_urls for Beautifulsoup
    description: add categories to urls from CategoriesLinks_globalresources.txt file
    '''

    file = open('CategoriesLinks_globalresources.txt', 'r')
    categories = file.read().split('\n')


    urls = []
    # categories = categories[0:10]
    for c in list(categories):
        urls.append(str(c))
        # return urls

    return urls
############################################################
############################################################
def product_parse(s, urls):
    '''
    function_name: product_parse
    input: url
    output: none
    description: crawl product page
    '''

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
        while True:
            try:
                # params = {'hostname': 'www.globalsources.com',
                #           'prodNo': '400', 'prod_id': '-1', 'action': 'GetPoint', 'resetbean': 'on',
                #           'view': 'grid', 'language': 'en', 'design': 'clean', 'point_id': '3000000151250', 'catalog_id': '2000000003844'}
                # resp = s.get(url=str('https://www.globalsources.com/gsol/GeneralManager'), proxies={'http': proxy}, headers=headers, json=params)
                # json_response = resp.json()
                # resp = s.get(url=str(url), proxies={'http': proxy}, headers=headers, timeout=7)
                soup = BeautifulSoup(resp.content, 'html.parser')

                ls = soup.select('h3.image_tit')
                if(not ls):
                    print('not founding product titles')
                    proxy, useragent = change_proxy()
                    s.headers.update({'User-Agent': useragent})
                    continue

                total_number_of_products = int(soup.select('h2.listing_h2')[0].text.strip().split()[0].replace(',',''))
                urls = [l.find('a').attrs['href'] for l in ls]

                products = 80
                while((products+80) < total_number_of_products):
                    products = products + 80
                    while True:
                        resp = s.get(url=str(url) + '&page=Browse&prodNo=' + str(products), headers=headers, proxies={'http': proxy})
                        soup = BeautifulSoup(resp.content, 'html.parser')

                        ls = soup.select('h3.image_tit')
                        if (not ls):
                            print('not founding product titles')
                            proxy, useragent = change_proxy()
                            s.headers.update({'User-Agent': useragent})
                            continue
                        else:
                            break
                    [urls.append(url) for url in [l.find('a').attrs['href'] for l in ls]]

                if(products < total_number_of_products and products+80 > total_number_of_products):
                    while True:
                        resp = s.get(url=str(url) + '&page=Browse&prodNo=' + str(products), headers=headers, proxies={'http': proxy})
                        soup = BeautifulSoup(resp.content, 'html.parser')

                        ls = soup.select('h3.image_tit')
                        if (not ls):
                            print('not founding product titles')
                            proxy, useragent = change_proxy()
                            s.headers.update({'User-Agent': useragent})
                            continue
                        else:
                            break
                    [urls.append(url) for url in [l.find('a').attrs['href'] for l in ls]]

                product_parse(s, urls)

            except urllib.error.HTTPError as e:
                if (e.code == 403):
                    proxy, useragent = change_proxy()
                    s.headers.update({'User-Agent': useragent})
                    continue

            except:
                proxy, useragent = change_proxy()
                s.headers.update({'User-Agent': useragent})
                print('Error Occurred in function and try again')
                continue
            else:
                break

############################################################
############################################################
############################################################

urls = read_category_url()
main_parse(urls)