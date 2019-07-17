# -*- coding: utf-8 -*-
import json
import urllib
import re
from bs4 import BeautifulSoup
from jsonmerge import merge
import multiprocessing
import requests
from jsonmerge import merge
import requests.exceptions
import time
import warnings
warnings.filterwarnings("ignore")
##################################################
##################################################
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/46.0.2490.80 Safari/537.36',
    'Content-Type': 'text/html',
}

######################################################################################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: start_urls for scrapy
    description: add products to urls from products_go4w.json file
    '''
    with open('products_go4w.json') as f:
        products = json.load(f)
    #print(products[0])
    urls = []
    #products = products[0:10]
    for p in list(products):
        urls.append("https://www.go4worldbusiness.com/suppliers/" + str(p['name']).replace(',','').replace(' & ',' ').replace(' ',"-"))
        urls.append("https://www.go4worldbusiness.com/buyers/" + str(p['name']).replace(',','').replace(' & ',' ').replace(' ',"-"))
        #return urls

    return urls
######################################################################################################################
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

######################################################################################################################
def change_proxy():
    '''
    function_name: change_proxy
    input: none
    output: none
    description: change proxy with proxyrotator api
    '''
    url = 'http://falcon.proxyrotator.com:51337/'

    params = dict(
        apiKey='YEXDtBuyrKq3obRLwC4PUQmTZN2SjcxV',
        connectionType='Datacenter'
    )

    print('********************************************')
    data = ''

    while True:
        try:
            resp = requests.get(url=url, params=params, timeout=5)
            data = json.loads(resp.text)
            print('Changing Proxy ... ' + data['proxy'])
            print('********************************************')
            return data['proxy'], data['randomUserAgent']

        except:
            print('Exception occured in changeproxy')
            continue

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
def clean_html(text):
    '''
    function_name: clean_html
    input: text
    output: clean text from html tags
    description: remove html tags from text
    '''
    cleanr = re.compile('<.*?>')
    cleantext = re.sub(cleanr, '', text)
    return cleantext

#####################################################################################################################
def clean_text(text):
    '''
    function_name: clean_text
    input: json
    output: json
    description: Clean Text from non valid characters for add to database
    '''
    json_text = json.loads(json.dumps(text))
    for att, value in json_text.items():
        if (not (type(value) == bool)):
            if (type(value) == list):
                text_1 = clean_html(str(value)[2:-2])  # remove html tags from text
                text_2 = text_1.replace("\']", "").replace("[\'", "")
                text_3 = text_2.lstrip().rstrip()  # remove whitespaces from begin and end of string
                text_4 = text_3.replace('\\n', '').strip()  # remove new line chracters from string
                json_text[att] = re.sub(' +', ' ', text_4)

    return json_text
####################################################################################################################
def clean_text_(text):
    '''
    function_name: clean_text_
    input: text
    output: text
    description: Clean Text from non valid characters for add to database
    '''
    text_1 = clean_html(str(text.replace('\n', '')))  # remove html tags from text
    text_2 = text_1.lstrip().rstrip()  # remove whitespaces from begin and end of string
    text_3 = text_2.replace('\\n', '').strip()  # remove new line chracters from string
    text_4 = re.sub(' +', ' ', text_3)
    text_5 = text_4.replace('[]', '').strip()  # remove  [] for empty list
    text_6 = text_5.replace('\']', '').replace('[\'', '')  # remove [' and '] for start and end list
    text_7 = text_6.replace('\\r', '')  # remove \r from text
    text_8 = text_7.replace('\r', '')  # remove \r from text

    return text_8
####################################################################################################################
def clean_backslashN_array(inputArray):
    '''
    function_name: clean_backslashN_array
    input: array
    output: cleaned array
    description: Clean all elements of array have \n
    '''
    outArray = []
    for item in inputArray:
        if re.match('.*[a-zA-Z0-9].*', str(item)):
            outArray.append(item)

    return outArray
####################################################################################################################
def clean_rn_text(text):
    '''
    function_name: clean_rn_text
    input: string
    output: string
    description: remove all \r and \n from text
    '''
    return ' '.join(text.replace('\n', ' ').replace('\r',' ').split())
####################################################################################################################
def is_html(txt):
    '''
    function_name: is_html
    input: string
    output: boolean
    description: check if txt is html or not
    '''
    return bool(BeautifulSoup(txt, "html.parser").find())

####################################################################################################################
def tokenize_text(txt):
    '''
    function_name: tokenize_text
    input: json
    output: json
    description: tokenize the text according to ":"
    '''

    newResult = {}

    try:
        temp = str(txt)
        temp = re.sub(' +', ' ', temp)
        tokens_ = temp.split("\n")
        for t in tokens_:
            clean_t_ = t.split(":")
            if (len(clean_t_) == 2 and not ('Contact Details:' in t)): # except Contact Details: to add in result
                newResult[clean_text_(clean_t_[0])] = clean_text_(clean_t_[1].split("\n")[0])
    except:
        pass

    return newResult
####################################################################################################################
def tokenize_buyer_or_supplier_text(result):
    '''
    function_name: tokenize_buyer_or_supplier_text
    input: json
    output: json
    description: tokenize the supplierText or buyerText
    '''

    newResult = {}  # json to add MongoDB

    if (result['isSupplier']):

        try:

            #####Extract information from 'supplierText' string
            temp = str(result['supplierText'])
            temp = re.sub(' +', ' ', temp)
            tokens_ = temp.split("\n")
            for t in tokens_:
                clean_t_ = t.split(":")
                if (len(clean_t_) == 2 and len(clean_t_[0].replace('\n', '').replace(' ', '').strip()) > 0 and len(
                        clean_t_[1].replace(' ', '').replace('\n', '').strip()) > 0):
                    newResult[str(clean_text_(clean_t_[0]))] = clean_t_[
                        1].replace(' ', '').replace('\n', '').strip()

        except:
            pass

    elif (result['isBuyer']):

        #####Extract information from 'buyerText' string
        temp = str(result['buyerText'])
        temp = re.sub(' +', ' ', temp)
        tokens_ = temp.split("\n")
        for t in tokens_:
            clean_t_ = t.split(":")
        if (len(clean_t_) == 2 and len(clean_t_[0].replace('\n', '').replace(' ', '').strip()) > 0 and len(
                clean_t_[1].replace('\n', '').replace(' ', '').strip()) > 0):
            newResult[str(clean_t_[0].replace('\n', '').replace(' ', '').strip())] = clean_t_[1].replace('\n',
                                                                                                          '').replace(
                ' ', '').strip()


    return newResult
#####################################################################################################################
def get_data_from_html(tokens, name_part):
    '''
    function_name: get_data_from_html
    input: list of string, string
    output: json
    description: extract data from html using html comment tags
    '''
    data = {}
    ###############################################################################################
    #############Global############################################################################
    if name_part == 'global':
        for i in range(0, len(tokens)):
            if tokens[i].strip().replace(' ', '') == '<!--Title-->':
                data['global_title'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip()

            if tokens[i].strip().replace(' ', '') == '<!--Date-->':
                data['global_date'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip()

            if tokens[i].strip().replace(' ', '') == '<!--Description-->':
                data['global_description'] = BeautifulSoup(tokens[i + 1], 'html.parser').text

            if tokens[i].strip().replace(' ', '') == '<!--CategoryStub-->':
                cats = BeautifulSoup(tokens[i + 1], 'html.parser').find_all('a')
                if (len(cats) > 0):
                    cats = [' '.join(c.text.strip().replace('\n', ' ').split()) for c in cats]
                    temp = []
                    temp_index = None
                    for j in range(len(cats)):
                        c_ = cats[j].split('Of')
                        if (len(c_) > 1):
                            if (temp_index):
                                data['global_' + (temp_index + ' Of').replace(' ', '_')] = temp
                                temp = []
                            temp_index = c_[0]
                            temp.append(c_[1])
                        else:
                            temp.append(cats[j])
                    data['global_' + (temp_index + ' Of').replace(' ', '_')] = temp

    ###############################################################################################
    #############Buyer#############################################################################
    if name_part == 'buyer':
        for i in range(0, len(tokens)):
            if tokens[i].strip().replace(' ', '') == '<!--Title-->':
                data['buyer_title'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip()

            if tokens[i].strip().replace(' ', '') == '<!--Date-->':
                data['buyer_date'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip()

            if tokens[i].strip().replace(' ', '') == '<!--Description-->':
                data['buyer_description'] = BeautifulSoup(tokens[i + 1], 'html.parser').text

            if tokens[i].strip().replace(' ', '') == '<!--CategoryStub-->':
                cats = BeautifulSoup(tokens[i + 1], 'html.parser').find_all('a')
                if (len(cats) > 0):
                    cats = [' '.join(c.text.strip().replace('\n', ' ').split()) for c in cats]
                    temp = []
                    temp_index = None
                    for j in range(len(cats)):
                        c_ = cats[j].split('Of')
                        if (len(c_) > 1):
                            if (temp_index):
                                data[(temp_index + ' Of').replace(' ', '_')] = temp
                                temp = []
                            temp_index = c_[0]
                            temp.append(c_[1])
                        else:
                            temp.append(cats[j])
                    data[(temp_index + ' Of').replace(' ', '_')] = temp
    ###############################################################################################
    #############Company###########################################################################
    if name_part == 'Company':
        for i in range(0, len(tokens)):
            if tokens[i].strip().replace(' ', '') == '<!--Title-->':
                data['company_name'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip()

            if tokens[i].strip().replace(' ', '') == '<!--Date-->':
                data['buyer_date'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip()

            if tokens[i].strip().replace(' ', '') == '<!--Description-->':
                keys = re.findall('<b>(.*?)</b>(.*?)<br/>', tokens[i + 1], re.DOTALL)
                for k in keys:
                    if (not is_html(k[1])):
                        data['company_' + k[0].strip().replace(':', '').replace(' ', '_')] = k[1].strip().replace(':',
                                                                                                                  '')
                try:
                    data['company_address'] = BeautifulSoup(tokens[i + 1], 'html.parser').find('address').text.strip()
                except:
                    data['company_address'] = None
            if tokens[i].strip().replace(' ', '') == '<!--CategoryStub-->':
                cats = BeautifulSoup(tokens[i + 1], 'html.parser').find_all('a')
                if (len(cats) > 0):
                    cats = [' '.join(c.text.strip().replace('\n', ' ').split()) for c in cats]
                    temp = []
                    temp_index = None
                    for j in range(len(cats)):
                        c_ = cats[j].split('Of')
                        if (len(c_) > 1):
                            if (temp_index):
                                data[(temp_index + ' Of').replace(' ', '_')] = temp
                                temp = []
                            temp_index = c_[0]
                            temp.append(c_[1])
                        else:
                            temp.append(cats[j])
                    data[(temp_index + ' Of').replace(' ', '_')] = temp
    ###############################################################################################
    #############Company###########################################################################
    if name_part == 'Our Company':
        for i in range(0, len(tokens)):
            if tokens[i].strip().replace(' ', '') == '<!--Title-->':
                data['company_name'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip()

            if tokens[i].strip().replace(' ', '') == '<!--Date-->':
                data['buyer_date'] = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip()

            if tokens[i].strip().replace(' ', '') == '<!--Description-->':
                keys = re.findall('<b>(.*?)</b>(.*?)<br/>', tokens[i+1], re.DOTALL)
                for k in keys:
                    if(not is_html(k[1])):
                        data['company_'+k[0].strip().replace(':','').replace(' ','_')] = k[1].strip().replace(':','')
                try:
                    data['company_address'] = BeautifulSoup(tokens[i + 1], 'html.parser').find('address').text.strip()
                except:
                    data['company_address'] = None
            if tokens[i].strip().replace(' ', '') == '<!--CategoryStub-->':
                cats = BeautifulSoup(tokens[i + 1], 'html.parser').find_all('a')
                if (len(cats) > 0):
                    cats = [' '.join(c.text.strip().replace('\n',' ').split()) for c in cats]
                    temp = []
                    temp_index = None
                    for j in range(len(cats)):
                        c_ = cats[j].split('Of')
                        if(len(c_)>1):
                            if(temp_index):
                                data[(temp_index + ' Of').replace(' ','_')] = temp
                                temp = []
                            temp_index = c_[0]
                            temp.append(c_[1])
                        else:
                            temp.append(cats[j])
                    data[(temp_index + ' Of').replace(' ','_')] = temp
    ###############################################################################################
    #############Products##########################################################################
    if name_part == 'Products':
        product_list = []
        temp = {}
        pass_title = False
        for i in range(0, len(tokens)):
            if tokens[i].strip().replace(' ', '') == '<!--Title-->':
                if pass_title:
                    t_ = BeautifulSoup(tokens[i + 1], 'html.parser').text.strip().split('\n')
                    t_ = [t.strip() for t in t_ if len(t)>0]
                    temp['title'] = t_[0].strip()
                    temp['date'] = t_[1].strip()
                    continue
                else:
                    pass_title = True
                    continue

            if tokens[i].strip().replace(' ', '') == '<!--Description-->':
                desc = BeautifulSoup(tokens[i + 1], 'html.parser').text.replace('Inquire Now','').replace('Add to Favorites','').replace('\r\n','\n').strip()
                temp['description'] = desc
                if 'title' in temp:
                    product_list.append(temp)
                    temp = {}

        data[name_part] = product_list

    return data
#####################################################################################################################
def product_parse(index, urls):
    '''
    function_name: product_parse
    input: json
    output: json
    description: extract data from product page
    '''
    cnt_ = 1
    for url_ in urls:

        json_url = json.loads(url_.replace("\'", "\""))

        url = json_url['url']
        while True:
            try:
                if 'proxy' in locals():
                    session = requests.session()
                    conn = session.get(url, proxies={'https': proxy}, headers=headers, timeout=30, verify=False)
                    if conn.status_code == 404:
                        print(f'{url} not found')
                        break
                    else:
                        html = conn.content
                else:
                    session = requests.session()
                    conn = session.get(url, headers=headers, timeout=30, verify=False)
                    if conn.status_code == 404:
                        print(f'{url} not found')
                        break
                    else:
                        html = conn.content

                soup = BeautifulSoup(html, 'html.parser')

                data = {}

                data['type'] = json_url['type']

                new_tokens = re.compile('(<!--.*?-->)').split(str(soup.select('.body-container')[0].find('div')))
                data = merge(data, get_data_from_html(new_tokens, 'global'))


                parts = soup.find('ul', {'class': 'nav-pills'}).findAll('a')
                parts = [(p.text.strip(), p.attrs['href']) for p in parts if 'Company' in p.text or 'Products' in p.text]
                parts = list(set(parts))
                if(len(parts) > 0):
                    for p in parts:
                        new_url = 'https://www.go4worldbusiness.com' + p[1]
                        new_name_part = p[0]
                        while True:
                            try:
                                if 'proxy' in locals():
                                    session = requests.session()
                                    conn = session.get(url, proxies={'https': proxy}, headers=headers, timeout=30, verify=False)
                                    if conn.status_code == 404:
                                        print(f'{url} not found')
                                        break
                                    else:
                                        html = conn.content
                                else:
                                    session = requests.session()
                                    conn = session.get(url, headers=headers, timeout=30, verify=False)
                                    if conn.status_code == 404:
                                        print(f'{url} not found')
                                        break
                                    else:
                                        html = conn.content

                                soup = BeautifulSoup(html, 'html.parser')
                                new_tokens = re.compile('(<!--.*?-->)').split(str(soup.select('.body-container')[0].find('div')))
                                data = merge(data, get_data_from_html(new_tokens, new_name_part))
                                break

                            except Exception as e:
                                print(e)
                                proxy, useragent = change_proxy()
                                headers['User-Agent'] = useragent
                                continue
                    f.write(str(data) + '\n')
                    break
                else:
                    new_tokens = re.compile('(<!--.*?-->)').split(str(soup.select('.body-container')[0].find('div')))
                    data = merge(data, get_data_from_html(new_tokens, 'buyer'))
                    f.write(str(data) + '\n')
                    break

            except Exception as e:
                print(e)
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                continue
        print(f'Process {index}: {cnt_} from {len(urls)} has been done.')
        cnt_ = cnt_ + 1
###########################################################
def main_parse(p, urls):

    '''
    function_name: main_parse
    input: list
    output: none
    description: first level of crawling
    '''

    ########################################################
    cnt_url = 0
    for url in urls:
        # categories
        while True:
            try:
                if 'proxy' in locals():
                    session = requests.session()
                    html = session.get(url, proxies={'https': proxy}, headers=headers, timeout=30, verify=False).content
                else:
                    session = requests.session()
                    html = session.get(url, headers=headers, timeout=30).content

                soup = BeautifulSoup(html, 'html.parser')

                links = soup.select('div.entity-rows-container')

                if (len(links) > 0):
                    if '/buyers/' in url:
                        for l in links:
                            if l.find('a'):
                                temp = {}
                                temp['url'] = 'https://www.go4worldbusiness.com' + l.find('a').attrs['href']
                                temp['type'] = 'buyer'
                                temp['category'] = str(url).split('/')[-1].split('?')[0].split('.')[0]

                                f.write(str(temp) + '\n')
                    if '/suppliers/' in url:
                        for l in links:
                            if l.find('a'):
                                temp = {}
                                temp['url'] = 'https://www.go4worldbusiness.com' + l.find('a').attrs['href']
                                temp['type'] = 'supplier'
                                temp['category'] = str(url).split('/')[-1].split('?')[0].split('.')[0]

                                f.write(str(temp) + '\n')
                else:
                    proxy, useragent = change_proxy()
                    headers['User-Agent'] = useragent
                    print('Links not found...')
                    continue

                cnt_url = cnt_url + 1
                print(f'process {p}: {cnt_url} from {len(urls)} has been done.')
                break
            except Exception as e:
                print(e)
                proxy, useragent = change_proxy()
                headers['User-Agent'] = useragent
                continue
###############################################################################33
with open('files/go4w_products/go4w_products_pages-1.json') as ff:
    urls = ff.readlines()

f = open('files/go4w_final_results/go4w_final-1.json', 'w')

urls = [str(url).strip() for url in urls]

number_processes = 6
parts = chunkIt(urls, number_processes)

processes = []

for i in range(number_processes):
    processes.append(multiprocessing.Process(target=product_parse, args=[i,parts[i]]))


for p in processes:
    p.start()

for p in processes:
    p.join()

f.close()



