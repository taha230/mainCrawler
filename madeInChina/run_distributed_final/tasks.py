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
                apiKey='YEXDtBuyrKq3obRLwC4PUQmTZN2SjcxV',
                connectionType='Datacenter'
            )

            data = ''
            resp = requests.get(url=url, params=params)
            data = json.loads(resp.text)

            if ('Android' not in data['randomUserAgent']):
                break
        except:
            print('Error in chaning proxy...')
            continue

    return data['proxy'], data['randomUserAgent']
#################################################################################################################
@app.task
def page_parse(url):
    '''
    function_name: page_parse
    input: url
    output: none
    description: crawl all prducts pages
    '''
    ########################################################
    headers['User-Agent'] = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
    while True:
        try:
            if 'proxy' in locals():
                html = requests.get(str(url), headers=headers, proxies={'http': proxy}).content
            else:
                html = requests.get(str(url), headers=headers).content

            soup = BeautifulSoup(html, 'html.parser')

            data = {}

            try:
                title = soup.select('h1.J-baseInfo-name')
                if len(title) > 0:
                    data['product_title'] = title[0].text.strip()
                else:
                    title = soup.select('div.pro-name')[0].find('h1').text.strip()
                    data['product_title'] = title
            except:
                data['title'] = 'None'

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

            data = None


        except Exception as e:
            print(e)
            proxy, useragent = change_proxy()
            print('Error Occurred in page_parse function and try again')
            continue

        else:
            break

