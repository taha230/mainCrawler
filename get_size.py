# import requests
# from bs4 import BeautifulSoup
# import json
# import urllib
# import multiprocessing
# ###########################################################
# ###########################################################
# headers = {
#     'User-Agent': ''
# }
# manager = multiprocessing.Manager()
# final_list = manager.list()
# ###########################################################
# ###########################################################
# def chunkIt(seq, num):
#     avg = len(seq) / float(num)
#     out = []
#     last = 0.0
#
#     while last < len(seq):
#         out.append(seq[int(last):int(last + avg)])
#         last += avg
#
#     return out
# ###########################################################
# def create_category_url():
#     '''
#     function_name: create_category_url
#     input: none
#     output: start_urls for scrapy
#     description: add products to urls from products_alibaba.json file
#     '''
#     with open('products_go4w.json') as f:
#         products = json.load(f)
#     urls = []
#     for p in list(products):
#         urls.append("https://www.go4worldbusiness.com/suppliers/" + str(p['name']).replace(',','').replace(' & ',' ').replace(' ',"-"))
#         urls.append("https://www.go4worldbusiness.com/buyers/" + str(p['name']).replace(',','').replace(' & ',' ').replace(' ',"-"))
#     return urls
# ###########################################################
# def change_proxy():
#     '''
#     function_name: change_proxy
#     input: none
#     output: none
#     description: change proxy with proxyrotator api
#     '''
#     url = 'http://falcon.proxyrotator.com:51337/'
#
#     params = dict(
#         apiKey='YEXDtBuyrKq3obRLwC4PUQmTZN2SjcxV'
#     )
#
#     resp = requests.get(url=url, params=params)
#     data = json.loads(resp.text)
#
#     return data['proxy'], data['randomUserAgent']
# ################################################################
# def get_pages(index, urls):
#     proxy, useragent = change_proxy()
#     headers['User-Agent'] = useragent
#     cnt = 0
#     for url in urls:
#         try:
#             soup = BeautifulSoup(requests.get(url, proxies={'http': proxy}, headers=headers).content, 'html.parser')
#             lis = soup.select('.pagination a')
#             if(len(lis)>0):
#                 tokens = str(lis[len(lis)-1].get('href')).split('&')
#                 for t in tokens:
#                     if "pg_" in str(t):
#                         page = int(t.split('=')[-1])
#                         final_list.append(page)
#                         break
#         except urllib.error.HTTPError as e:
#             if(e.code == 403):
#                 proxy, useragent = change_proxy()
#                 headers['User-Agent'] = useragent
#                 print('************************************')
#                 print('Changing Proxy...')
#                 print('************************************')
#
#         cnt = cnt + 1
#         print('Process ' + str(index+1) + ': ' + str(cnt) + ' has been done from ' + str(len(urls)))
# ########################################################################
# urls = create_category_url()
#
# parts = chunkIt(urls, 5)
#
# processes = []
#
# for i in [0,1,2,3,4]:
#     processes.append(multiprocessing.Process(target=get_pages, args=[i,parts[i]]))
#
#
# for p in processes:
#     p.start()
#
# for p in processes:
#     p.join()
#
#
# total_pages = 0
# for s in final_list:
#     total_pages = total_pages + int(s)
#
# print("Total Page = " + str(total_pages))
#
#
#
#
#
#
#
#
#
#
#

from seleniumrequests import Firefox

# Simple usage with built-in WebDrivers:
webdriver = Firefox()
response = webdriver.request('GET', 'https://www.google.com/')
print(response)


# More complex usage, using a WebDriver from another Selenium-related module:
from seleniumrequests.request import RequestMixin
from someothermodule import CustomWebDriver


class MyCustomWebDriver(CustomWebDriver, RequestMixin):
    pass


custom_webdriver = MyCustomWebDriver()
response = custom_webdriver.request('GET', 'https://www.google.com/')
print(response)
