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
from urllib import request as urlrequest
from socket import timeout
from socket import error as SocketError
import errno
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

    file = open('CategoriesLinks_indiamart.txt', 'r')
    categories = file.read().split('\n')


    urls = []
    # categories = categories[0:10]
    for c in list(categories):
        urls.append(str(c))
        # return urls

    return urls
############################################################
############################################################

def main_parse(urls):
    '''
    function_name: main_parse
    input: list
    output: none
    description: first level of crawling
    '''

    ########################################################
    for url in urls:
        # categories
        # taha
        proxy, useragent = change_proxy()
        s = requests.session()
        s.headers.update({'User-Agent': useragent})

        while True:
            try:


                # try:
                #     response = urlrequest.urlopen(req, timeout=5)
                # except SocketError as e:
                #     proxy, useragent = change_proxy()
                #     headers['User-Agent'] = useragent
                #     continue
                #
                #
                # soup = BeautifulSoup(response.read().decode('utf8'), 'html.parser')
                html = s.get(str(url), proxies={'http': proxy}).content
                soup = BeautifulSoup(html, 'html.parser')

                #u1 = urllib.urlretrieve(str(url))
                #soup = BeautifulSoup(requests.get(str(url), proxies={'http': proxy}, headers=headers).content, 'html.parser')
                items = soup.find_all('div', recursive=True)
                #items_urls = [i.find('a').attrs['href'] for i in items]
                ls = soup.select('div.image_tit')



            except urllib.error.HTTPError as e:
                if (e.code == 403):
                    proxy, useragent = change_proxy()
                    s.headers.update({'User-Agent': useragent})
                    continue

            # except:
            #     proxy, useragent = change_proxy()
            #     headers['User-Agent'] = useragent
            #     print('Error Occurred in function and try again')
            #     continue
            else:
                break

############################################################
############################################################
############################################################

urls = read_category_url()
main_parse(urls)