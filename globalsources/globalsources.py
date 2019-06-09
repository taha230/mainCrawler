import requests
from bs4 import BeautifulSoup
import json
import urllib
import multiprocessing
###########################################################
###########################################################
headers = {
    'User-Agent': ''
}
manager = multiprocessing.Manager()
final_list = manager.list()
###########################################################
###########################################################
def chunkIt(seq, num):
    avg = len(seq) / float(num)
    out = []
    last = 0.0

    while last < len(seq):
        out.append(seq[int(last):int(last + avg)])
        last += avg

    return out
###########################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: start_urls for scrapy
    description: add products to urls from products_go4w.json file
    '''
    with open('CategoriesLinks_gr.json') as f:
        products = json.load(f)
    urls = []
    for p in list(products):
        urls.append(str(p['name']))
    return urls
###########################################################
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

    return data['proxy'], data['randomUserAgent']
################################################################
def get_pages(index, urls):

    proxy, useragent = change_proxy()

    headers = {
        'User-Agent': 'Mozilla\/5.0 (compatible MSIE 10.0 Windows Phone 8.0 Trident\/6.0 IEMobile\/10.0 ARM Touch NOKIA Lumia 520)',
    }

    proxy = "101.51.141.123:8080"

    #headers['User-Agent'] = useragent
    cnt = 0
    for url in urls:
        print('AAAAAAAAAAAAAAAAAAAAAAAAA')

        # try:
        #     soup = BeautifulSoup(requests.get(url).content, 'html.parser')
        #     lis = soup.select("span[class=nonLink]").text
        #     if(len(lis)>0):
        #         tokens = str(lis[len(lis)-1].get('href')).split('&')
        #         for t in tokens:
        #             if "pg_" in str(t):
        #                 page = int(t.split('=')[-1])
        #                 final_list.append(page)
        #                 break
        # except urllib.error.HTTPError as e:
        #     if(e.code == 403 and False):
        #         proxy, useragent = change_proxy()
        #         headers['User-Agent'] = useragent
        #         print('************************************')
        #         print('Changing Proxy...')
        #         print('************************************')
        #
        # cnt = cnt + 1
        # print('Process ' + str(index+1) + ': ' + str(cnt) + ' has been done from ' + str(len(urls)))
########################################################################

def getAllCategoryLinks():

    categoriesLinkFile = open("CategoriesLinks_gr.txt", "w")

    soup = BeautifulSoup(requests.get('https://www.globalsources.com/').content, 'html.parser')
    #lis = soup.select("ul[class=list]")
    dt_data = soup.find_all("li", {"class": "item"})    #soup.find('a').text
    categoriesLinkFile.close()

urls = ['https://www.globalsources.com/'] #create_category_url()
getAllCategoryLinks()
urls = create_category_url()

parts = chunkIt(urls, 5)

processes = []

for i in [0,1,2,3,4]:
    processes.append(multiprocessing.Process(target=get_pages, args=[i,parts[i]]))


for p in processes:
    p.start()

for p in processes:
    p.join()


total_pages = 0
for s in final_list:
    total_pages = total_pages + int(s)

print("Total Page = " + str(total_pages))











