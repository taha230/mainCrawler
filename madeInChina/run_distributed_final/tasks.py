from __future__ import absolute_import
import json
import multiprocessing
import requests
from random import randint
import urllib
from bs4 import BeautifulSoup
import random
from jsonmerge import merge
from tabletojson import table_to_json, table_to_json_complex, table_to_json_horizontal
from run_distributed_final.celery import app
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
def page_parse(urls):
    '''
    function_name: page_parse
    input: url
    output: none
    description: crawl all prducts pages
    '''
    ########################################################
    headers['User-Agent'] = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
    for url in urls:
        while True:
            try:
                if 'proxy' in locals():
                    html = requests.get(str(url), headers=headers, proxies={'http': proxy}).content
                else:
                    html = requests.get(str(url), headers=headers).content

                soup = BeautifulSoup(html, 'html.parser')

                data = {}

                try:
                    title = soup.select('h1.J-baseInfo-name')[0].text.strip()
                    data['product_title'] = title
                except:
                    title = None

                tables = soup.find_all('table')

                for t in tables:
                    table_json = table_to_json_horizontal(str(t))
                    if (bool(table_json)):
                        data = merge(data, table_json)
                        continue

                    table_json = table_to_json(str(t))
                    if (bool(table_json)):
                        data = merge(data, table_json)

                items = soup.select('div.bac-item-label')
                values = soup.select('div.bac-item-value')
                temp = {}
                for i in range(0, len(items)):
                    temp[items[i].text.strip()] = values[i].text.strip()

                data = merge(data, temp)

                try:
                    company = soup.select('div.title-txt')[0].find('a').text.strip()
                    data['company_name'] = company
                except:
                    pass

                try:
                    contact_name = soup.select('div.sr-side-contSupplier-name')[0].text
                    data['contact_name'] = contact_name
                except:
                    data['contact_name'] = 'None'



                return data

            except urllib.error.HTTPError as e:
                if (e.code == 403):
                    continue
            except:
                print('Error Occurred in page_parse function and try again')
                continue

            else:
                break
#################################################################################################################
