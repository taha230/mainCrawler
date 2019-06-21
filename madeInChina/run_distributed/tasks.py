from __future__ import absolute_import
import json
import multiprocessing
import requests
from random import randint
import urllib
from bs4 import BeautifulSoup
import random
from run_distributed.celery import app
import time
##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

with open('android_user_agents.txt') as f:
    lines = f.readlines()
android_user_agents = [l.strip() for l in lines]
###################################################
def get_android_user_agent():
    '''
    function_name: get_android_user_agent
    input: none
    output: string
    description: return random android user agent from lists
    '''

    return android_user_agents[randint(0, len(android_user_agents) - 1)]
###################################################
def change_proxy():
    '''
    function_name: change_proxy
    input: none
    output: none
    description: change proxy with proxyrotator api
    '''

    while True:
        try:
            url = 'http://falcon.proxyrotator.com:51337/'

            params = dict(
                apiKey='YEXDtBuyrKq3obRLwC4PUQmTZN2SjcxV'
            )

            data = ''
            resp = requests.get(url=url, params=params)
            data = json.loads(resp.text)

        except:
            print('Error in Change Proxy')
            continue

    return data['proxy'], data['randomUserAgent']

#################################################################################################################
@app.task
def product_parse(url):
    '''
    function_name: product_parse
    input: url
    output: none
    description: crawl product page
    '''
    ########################################################
    headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
    while True:
        try:
           headers['User-Agent'] = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'

           html = requests.get(str(url), headers=headers).content

           if(html is None):
               proxy, useragent = change_proxy()
               headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
               html = requests.get(str(url), proxies={'http': proxy}, headers=headers).content


           soup = BeautifulSoup(html, 'html.parser')


           product_links = soup.select('h2.product-name')
           if (len(product_links) == 0):
               product_links = soup.select('h2.pro-name')

           if(len(product_links) > 0):
               links = []
               for p in product_links:
                   if (str(p.find('a').attrs['href']).startswith('http')):
                       links.append(str(p.find('a').attrs['href']))
                   else:
                       links.append('https:' + str(p.find('a').attrs['href']))


               return links

        except urllib.error.HTTPError as e:
            if (e.code == 403):
                print('Error 403 in porduct_parse.')
                continue
        except:
            print('Error Occurred in product_parse function and try again')
            continue

#################################################################################################################
