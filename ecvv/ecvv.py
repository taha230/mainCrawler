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
def product_parse(index, urls):
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
        try:
            while True:
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
                data = {}
                ######################################################################################
                try:
                    data['company_name'] = soup.select('td.company-name')[0].find('a').text
                except:
                    data['company_name'] = None

                try:
                    data['product_name'] = soup.find('h1', {'id': 'ViewProductname'}).text.strip()
                except:
                    data['company_name'] = None

                try:
                    data['product_extra_information'] = table_to_json_horizontal(soup.select('table.tableWidth')[0])
                except:
                    data['product_extra_information'] = None

                try:
                    temp_ = soup.select('div.specifications')[0].text.strip().split('\n')
                    keys = []
                    values = []
                    for t in temp_:
                        tts = t.split(':')
                        if len(tts)==2:
                            keys.append(re.sub(r'[0-9]\)', '' , tts[0].strip()).strip())
                            values.append(re.sub(r'[0-9]\)', '' , tts[1].strip()).strip())

                    temp = {}
                    for i in range(len(keys)):
                        if len(keys[i]) > 0 and len(values[i]) > 0 :
                            temp[keys[i]] = values[i]

                    data['product_specification'] = temp
                except:
                    data['product_specification'] = None

                try:
                    company_url = 'https:' + soup.select('td.company-name')[0].find('a').attrs['href']
                    company_url = company_url.replace('index.html','profile.html')
                    if 'proxy' in locals():
                        html = requests.get(str(company_url), proxies={'http': proxy}, headers=headers)
                        if (html.status_code == 404):
                            break
                        html = html.content
                    else:
                        html = requests.get(str(company_url), headers=headers)
                        if (html.status_code == 404):
                            break
                        html = html.content

                    soup = BeautifulSoup(html, 'html.parser')

                    try:
                        data['company_description'] = soup.select('div.CPcompany-info')[0].text.strip()
                    except:
                        data['company_description'] = None

                    try:
                        tables = soup.select('table.table')
                        temp = None

                        for t in tables:
                            temp_ = table_to_json_horizontal(t)
                            if temp:
                                temp = merge(temp, temp_)
                            else:
                                temp = temp_

                        data['company_information'] = temp

                    except Exception as e:
                        print(e)
                        data['company_information'] = None

                    try:
                        company_url = company_url.replace('profile.html', 'contact.html')
                        if 'proxy' in locals():
                            html = requests.get(str(company_url), proxies={'http': proxy}, headers=headers)
                            if (html.status_code == 404):
                                break
                            html = html.content
                        else:
                            html = requests.get(str(company_url), headers=headers)
                            if (html.status_code == 404):
                                break
                            html = html.content

                        soup = BeautifulSoup(html, 'html.parser')

                        keys = soup.find_all('dt')
                        keys = [k.text.replace(':','').strip() for k in keys]
                        values = soup.find_all('dd')
                        values = [v.text.strip() for v in values]

                        temp = {}
                        for i in range(len(keys)):
                            temp[keys[i]] = values[i]

                        data['company_contacts'] = temp

                        try:
                            data['company_contact_person'] = soup.findAll('div', {'class': 'cws'})[0].findAll('div', {'class': 'name'})[0].text.strip()
                        except:
                            data['company_contact_person'] = None

                        try:
                            sk_ = str(soup.findAll('div', {'class': 'cws'})[0].findAll('div', {'class': 'chat-feedback'})[0])
                            data['company_skype_id'] = re.findall("ChatSkype\('(.*?)',", sk_, flags=re.DOTALL)[0].strip()
                        except:
                            pass

                    except:
                        data['company_contacts'] = None

                    try:
                        data['company_extra_contacts'] = table_to_json_horizontal(soup.select('table.table')[0])
                    except:
                        data['company_extra_contacts'] = None

                    f.write(f'{data}' + '\n')
                    break

                except:
                    pass
        except Exception as e:
            print(e)
            proxy, useragent = change_proxy()
            headers['User-Agent'] = useragent

        print(f'Process {index}: {cnt_} from {len(urls)} has been done.')
        cnt_ = cnt_ + 1
#####################################################################################################################
with open('all_product_links.txt') as ff:
    urls = ff.readlines()

f = open('ecvv_final_result.txt','w')

number_processes = 10
parts = chunkIt(urls, number_processes)

processes = []

for i in range(number_processes):
    processes.append(multiprocessing.Process(target=product_parse, args=[i,parts[i]]))

for p in processes:
    p.start()

for p in processes:
    p.join()

f.close()