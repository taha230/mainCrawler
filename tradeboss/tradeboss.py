from __future__ import absolute_import
import json
import requests
from bs4 import BeautifulSoup
import random
from tabletojson import table_to_json, table_to_json_complex, table_to_json_horizontal
from jsonmerge import merge
import multiprocessing
import re
from tabletojson import table_to_json, table_to_json_horizontal
#####################################################################################################################
#####################################################################################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}
#####################################################################################################################
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
#####################################################################################################################
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

            print('********************************************')
            data = ''
            resp = requests.get(url=url, params=params)
            data = json.loads(resp.text)

            print('Changing Proxy ... ' + data['proxy'])
            print('********************************************')
            return data['proxy'], data['randomUserAgent']
        except:
            print('Error in changing proxy...')
            continue

#####################################################################################################################
def page_parse(index, urls):
    '''
    function_name: page_parse
    input: url
    output: none
    description: crawl category pages
    '''
    ########################################################
    headers['User-Agent'] = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
    cnt_ = 1
    for url in urls:
        while True:
            try:
                if 'proxy' in locals():
                    html = requests.get(str(url), proxies={'http': proxy}, headers=headers)
                    if (html.status_code == 404):
                        break
                    html = html.content
                else:
                    html = requests.get(str(url), headers=headers)
                    if (html.status_code == 404):
                        break
                    html = html.content

                soup = BeautifulSoup(html, 'html.parser')

                product_count = int(''.join(re.findall("\d+", re.findall('(.*?) Products found that match your criteria.', soup.text)[0])))

                links = []
                links.append(url)

                for i in range(20,product_count,20):
                    links.append(url.strip() + 'from/' + str(i) + '/')

                for l in links:
                    f.write(str(l).strip() + '\n')

                break

            except Exception as e:
                print(e)
                proxy, useragent = change_proxy()
                continue
        print(f'Process {index}: {cnt_} from {len(urls)} has been done.')
        cnt_ = cnt_ + 1
#####################################################################################################################
def main_parse(index, urls):
    '''
    function_name: product_parse
    input: url
    output: none
    description: crawl product pages
    '''
    ########################################################
    headers['User-Agent'] = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'
    headers['X-Forwarded-For'] = '.'.join([str(random.randint(0, 255)) for i in range(4)])
    cnt_ = 1
    for url in urls:
        while True:
            try:
                if 'proxy' in locals():
                    html = requests.get(str(url).strip(), proxies={'http': proxy}, headers=headers)
                    if (html.status_code == 404):
                        break
                    html = html.content
                else:
                    html = requests.get(str(url).strip(), headers=headers)
                    if (html.status_code == 404):
                        break
                    html = html.content

                soup = BeautifulSoup(html, 'html.parser')
                data = {}
                ######################################################################################



            except Exception as e:
                print(e)
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent

        print(f'Process {index}: {cnt_} from {len(urls)} has been done.')
        cnt_ = cnt_ + 1
#####################################################################################################################
with open('tradeboss_pages_links.txt') as ff:
    urls = ff.readlines()

f = open('tradeboss_final_result.txt','w')


main_parse(1, urls)

# number_processes = 6
# parts = chunkIt(urls, number_processes)
#
# processes = []
#
# for i in range(number_processes):
#     processes.append(multiprocessing.Process(target=page_parse, args=[i,parts[i]]))
#
# for p in processes:
#     p.start()
#
# for p in processes:
#     p.join()

f.close()