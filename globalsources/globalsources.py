# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerProcess
import json
import urllib
import multiprocessing
import re
import requests
from bs4 import BeautifulSoup
import urlparse3

from pymongo import MongoClient
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

    resp = requests.get(url=url, params=params)
    data = json.loads(resp.text)
    print('********************************************')
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

    #proxy, useragent = change_proxy()
    from selenium import webdriver
    driver = webdriver.chrome()
    driver.get("https://www.globalsources.com/")
    commentHTML = driver.find_elements_by_class_name('item')

    #headers['User-Agent'] = useragent
    soup = BeautifulSoup(requests.get("https://www.globalsources.com/").content, 'html.parser')
    lis = soup.findAll("a")

    for tag in lis:
        for subtag in tag.descendants:
            print(subtag)


    urls = [li.find('a') for li in lis if li]
    urls = [url for url in urls if url is not None]
    urls = [url.attrs['href'] for url in urls if 'pid' in str(url)]

    return urls
############################################################
############################################################
def product_parse(urls):
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
    headers['User-Agent'] = useragent
    ########################################################
    for url in urls:
        # Products
        for i in range(1, 101):
            try:
                soup = BeautifulSoup(requests.get(str(url) + "?spm=a2700.galleryofferlist.pagination&page=" + str(i), proxies={'http': proxy}, headers=headers).content, 'html.parser')
                items = soup.find_all('h2', class_='title')
                items_urls = [i.find('a').attrs['href'] for i in items]

                for iu in items_urls:
                    product_parse(iu)

            except urllib.error.HTTPError as e:
                if (e.code == 403):
                    proxy, useragent = change_proxy()
                    headers['User-Agent'] = useragent
                    print('********************************************')
                    print('Changing Proxy ... ' + proxy)
                    print('********************************************')
            except:
                pass

############################################################
############################################################
############################################################

urls = create_category_url()
main_parse(urls)