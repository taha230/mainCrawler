# -*- coding: utf-8 -*-
import json
import multiprocessing
import requests
from random import randint
import urllib
from bs4 import BeautifulSoup
import random
from jsonmerge import merge
from tabletojson import table_to_json, table_to_json_complex, table_to_json_horizontal
manager = multiprocessing.Manager()
total_urls = manager.list()
##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

with open('android_user_agents.txt') as f:
    lines = f.readlines()
android_user_agents = [l.strip() for l in lines]

f = open('madeInChina_result.json','a')
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

            if('Android' not in data['randomUserAgent']):
                break
        except:
            print('Error')
            continue


    return data['proxy'], data['randomUserAgent']

###########################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: list
    description: get link of all top categories page
    '''
    with open('madeInChinaTotalUrls-1.txt') as f:
        urls = f.readlines()

    urls = [x.strip() for x in urls]

    return urls
############################################################
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
                for i in range(0,len(items)):
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


            except urllib.error.HTTPError as e:
                if (e.code == 403):
                    continue
            except:
                print('Error Occurred in page_parse function and try again')
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
    proxy, useragent = change_proxy()
    headers['User-Agent'] = useragent
    headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
    while True:
        try:
           headers['User-Agent'] = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
           html = requests.get(str(url), proxies={'http': proxy}, headers=headers, timeout=5).content
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
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
                continue
        except:
            print('Error Occurred in product_parse function and try again')
            proxy, useragent = change_proxy()
            headers['User-Agent'] = useragent
            headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
            continue
###########################################################
###########################################################
def main_parse(index, urls):
    '''
    function_name: main_parse
    input: list
    output: none
    description: first level of crawling
    '''
    ########################################################
    cnt_url = 0
    proxy, useragent = change_proxy()
    headers['User-Agent'] = useragent
    headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
    for url in urls:
        while True:
            try:
                headers['User-Agent'] = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
                html = requests.get(str(url), proxies={'http': proxy}, headers=headers, timeout=5).content
                soup = BeautifulSoup(html, 'html.parser')

                product_links = soup.select('h2.product-name')
                if (len(product_links) == 0):
                    product_links = soup.select('h2.pro-name')

                if(len(product_links) > 0):
                    # for p in product_links:
                    #     if (str(p.find('a').attrs['href']).startswith('http')):
                    #         total_urls.append(str(p.find('a').attrs['href']))
                    #     else:
                    #         total_urls.append('https:' + str(p.find('a').attrs['href']))

                    total_pages = soup.select('a.page-dis')

                    if(len(total_pages) > 0):
                        total_pages = int(soup.select('a.page-dis')[0].text)
                    else:
                        total_pages = soup.select('div.page-num')
                        if(len(total_pages) == 0):
                            total_pages = 1
                        else:
                            total_pages = int(len(total_pages[0].find_all('a')))

                    if(total_pages > 1):
                        main_addres = 'https:' + '-'.join(soup.select('div.page-num')[0].find('a').attrs['href'].split('.html')[0].split('-')[:-1])
                        for i in range(3,total_pages):
                            total_urls.append(main_addres + '-' + str(i) + '.html')
                            #lls = product_parse(main_addres + '-' + str(i) + '.html')
                            #[total_urls.append(l) for l in lls]
                            #print(f'Process {index}: {i} from {total_pages} has been done.')
                    else:
                        total_urls.append(url)

                    break

            except urllib.error.HTTPError as e:
                 if (e.code == 403):
                    print('Error 403 in main_parse.')
                    proxy, useragent = change_proxy()
                    headers['User-Agent'] = useragent
                    headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
                    continue
            except:
                 print('Error Occurred in main_parse function and try again')
                 print(url)
                 proxy, useragent = change_proxy()
                 headers['User-Agent'] = useragent
                 headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
                 continue

        cnt_url = cnt_url + 1
        print(f'Process {index}: {cnt_url} has been done from {len(urls)}')


    #return total_urls
############################################################
############################################################
############################################################
#f = open('alibaba_result.json','a')

#urls = create_category_url()
#product_urls = main_parse(urls)



page_parse(['https://cnpinnxun.en.made-in-china.com/product/WSLJfmNVVPrG/China-Three-Phase-Asynchronous-AC-Induction-Electric-Gear-Reducer-Fan-Blower-Vacuum-Air-Compressor-Water-Pump-Universal-Industry-Machine-Motor.html'])


# parts = chunkIt(urls, 5)
#
# processes = []
#
# for i in [0,1,2,3,4]:
#     processes.append(multiprocessing.Process(target=main_parse, args=[i,parts[i]]))
#
#
# for p in processes:
#     p.start()
#
# for p in processes:
#     p.join()
#
# f = open('all_category_pages_madeInChina-2.txt','w')
#
# for l in total_urls:
#     f.write(str(l) + '\n')
#
# f.close()

