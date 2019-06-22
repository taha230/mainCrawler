# -*- coding: utf-8 -*-
import json
import multiprocessing
import re
import requests
import random
from random import randint
import urllib
import multiprocessing
from tabletojson import table_to_json, table_to_json_complex
from bs4 import BeautifulSoup
##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

f = open('alibaba_result.json','a')
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

    data = ''
    resp = requests.get(url=url, params=params)
    data = json.loads(resp.text)

    return data['proxy'], data['randomUserAgent']

###########################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: list
    description: get link of all categories page in alibaba.com
    '''
    soup = BeautifulSoup(requests.get("https://www.alibaba.com/Products").content, 'html.parser')
    lis = soup.find_all('li')
    urls = [li.find('a') for li in lis if li]
    urls = [url for url in urls if url is not None]
    urls = [url.attrs['href'] for url in urls if 'pid' in str(url)]
    urls = [url.replace('http:', 'https:') for url in urls]

    return urls
############################################################
############################################################
def company_parse(index, url,data, s):
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

            ###read data to file
            f.write(json.dumps(data))
            f.write('\n')

        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy(index)
                s.headers.update({'User-Agent': useragent})
                continue
        except:
            proxy, useragent = change_proxy(index)
            s.headers.update({'User-Agent': useragent})
            print(f'Process {index}: Error Occurred in company_parse function and try again')
            continue

        else:
            break
############################################################
############################################################
def product_parse(index, url, s):
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
            html = s.get(str(url), proxies={'http': proxy}).content
            soup = BeautifulSoup(html, 'html.parser')

            title, price, min_order, unit = None,None,None,None
            title = soup.findAll('h1')[0].text.strip()

            try:
                price = soup.select("span.ma-ref-price")[0].text.replace("\\n", "").strip()
                min_order = soup.select("span.ma-min-order")[0].text.strip()
                unit = soup.select("span.ma-min-order")[0].text.strip().split('/')[1]
            except:
                pass

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

            company_parse(index, company_url, data, s)


        except urllib.error.HTTPError as e:
            if (e.code == 403):
                proxy, useragent = change_proxy(index)
                s.headers.update({'User-Agent': useragent})
                continue
        except:
            proxy, useragent = change_proxy(index)
            s.headers.update({'User-Agent': useragent})
            print(f'Process {index}: Error Occurred in product_parse function and try again')
            continue

        else:
            break


###########################################################
###########################################################
def main_parse(index, urls):
    '''
    function_name: main_parse
    input: list
    output: none
    description: first level of crawling
    '''
    proxy, useragent = change_proxy()
    ########################################################
    cnt_url = 0
    for url in urls:
        for i in range(1, 101):
            while True:
                try:
                    # headers['User-Agent'] = useragent
                    # headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
                    # html = requests.get(str(url) + "?spm=a2700.galleryofferlist.pagination.1.22c94f8esuLjT3&page=" + str(i), headers=headers, proxies = {'http': proxy}).content
                    #
                    # soup = BeautifulSoup(html, 'html.parser')
                    #
                    # items = soup.find_all('h2', class_='title')

                    # if(len(items) == 0):
                    #     print(f'Process {index}: cant get list of products')
                    #     continue
                    #
                    # items_urls = [i.find('a').attrs['href'] for i in items]

                    # product_parse(index, "https:" + str(iu))
                    ff.write(str(url) + "?page=" + str(i) + '\n')


                except:
                    print(f'Process {index}: Error Occurred in main_parse function and try again')
                    continue

                else:
                    break

        cnt_url = cnt_url + 1
        print(f'Process {index}: {cnt_url} from {len(urls)} has been done.')

############################################################
############################################################
############################################################
urls = create_category_url()
parts = chunkIt(urls, 5)

ff = open('alibaba-all-pages-links-1.txt','w')

processes = []

for i in [0,1,2,3,4]:
    processes.append(multiprocessing.Process(target=main_parse, args=[i,parts[i]]))


for p in processes:
    p.start()

for p in processes:
    p.join()


ff.close()