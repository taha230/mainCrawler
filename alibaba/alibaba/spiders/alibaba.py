# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerProcess
import json
import re
from bs4 import BeautifulSoup
from pymongo import MongoClient
##################################################
##################################################
##################################################
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
    print('************************************')
    print('Changing Proxy...')
    print('************************************')
    return data['proxy'], data['randomUserAgent']

####################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: start_urls for scrapy
    description: add products to urls from products_alibaba.json file
    '''
    with open('products_alibaba.json') as f:
        products = json.load(f)
    print(products[0])
    urls = []
    for p in list(products):
        urls.append("https://www.alibaba.com/trade/search?fsb=y&IndexArea=product_en&CatId=&SearchText=" + str(p['name']).replace(' ',"+")) #Products
        return urls
        urls.append("https://www.alibaba.com/trade/search?fsb=y&IndexArea=company_en&CatId=&SearchText=" + str(p['name']).replace(' ',"+")) #Suppliers
    return urls
##################################################
####################################################
class alibabaSpider(scrapy.Spider):
    name = 'alibaba_spider'
    allowed_domains = ['alibaba.com']
    start_urls = create_category_url()

    ##################################################
    def clean_html(self, text):
        '''
        function_name: clean_html
        input: text
        output: clean text from html tags
        description: remove html tags from text
        '''
        cleanr = re.compile('<.*?>')
        cleantext = re.sub(cleanr, '', text)
        return cleantext

    ###################################################
    def clean_text(self, text):
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
                    text_1 = self.clean_html(str(value)[2:-2])  # remove html tags from text
                    text_2 = text_1.replace("\']", "").replace("[\'", "")
                    text_3 = text_2.lstrip().rstrip()  # remove whitespaces from begin and end of string
                    text_4 = text_3.replace('\\n', '').strip()  # remove new line chracters from string
                    json_text[att] = re.sub(' +', ' ', text_4)

        return json_text

    ###################################################
    def clean_text_(self, text):
        '''
        function_name: clean_text_
        input: text
        output: text
        description: Clean Text from non valid characters for add to database
        '''
        text_1 = self.clean_html(str(text)[2:-2])  # remove html tags from text
        text_2 = text_1.lstrip().rstrip()  # remove whitespaces from begin and end of string
        text_3 = text_2.replace('\\n', '').strip()  # remove new line chracters from string
        text_4 = re.sub(' +', ' ', text_3)

        return text_4

    ###################################################
    def clean_backslashN_array(self, inputArray):
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

    ###################################################
    def tokenize_buyer_or_supplier_text(self, result):
        '''
        function_name: tokenize_buyer_or_supplier_text
        input: json
        output: json
        description: tokenize the supplierText or buyerText
        '''
        if (result['isSupplier']):
            result['date'] = self.clean_text_(str(result['date']))
            result['supplierCountry'] = self.clean_text_(str(result['supplierCountry']))
            result['supplierSupplierOF'] = self.clean_text_(str(result['supplierSupplierOF']))
            result['supplierCompanyName'] = self.clean_text_(str(result['supplierCompanyName']))
            ###############################################
            #####Extract information from 'supplierText' string
            temp = str(result['supplierText'])[2:-2]
            temp = re.sub(' +', ' ', temp)
            tokens_ = temp.split("\\r\\n")
            for t in tokens_:
                clean_t_ = t.split(":")
                if (len(clean_t_) == 2 and len(clean_t_[0]) > 0 and len(clean_t_[1]) > 0):
                    result[clean_t_[0].replace('\\n', '').strip()] = clean_t_[1].replace('\\n', '').strip()
        elif (result['isBuyer']):
            result['date'] = self.clean_text_(str(result['date']))
            result['buyerProductName'] = self.clean_text_(str(result['buyerProductName']))
            result['buyerCountry'] = self.clean_text_(str(result['buyerCountry']))
            result['buyerBuyerOF'] = self.clean_text_(str(result['buyerBuyerOF']))
            result['buyerCompanyName'] = self.clean_text_(str(result['buyerCompanyName']))
            ###############################################
            #####Extract information from 'buyerText' string
            temp = str(result['buyerText'])[2:-2]
            temp = re.sub(' +', ' ', temp)
            tokens_ = temp.split("\\r\\n")
            for t in tokens_:
                clean_t_ = t.split(":")
            if (len(clean_t_) == 2 and len(clean_t_[0]) > 0 and len(clean_t_[1]) > 0):
                result[clean_t_[0].replace('\\n', '').strip()] = clean_t_[1].replace('\\n', '').strip()

        return result
    ##################################################
    def productCrawler(self, response):
        '''
        function_name: productCrawler
        input: self, response
        output:
        description: parse each product part of alibaba.com
        '''

        allProductsOnPage = response.css('.item-main').extract()
        allProductsLink = []
        for p in allProductsOnPage:
            soup = BeautifulSoup(p, "html.parser")
            product_link = soup.select('h2.title a[href]')[0]['href']
            allProductsLink.append("https:" + str(product_link))


        for link in allProductsLink:
            yield scrapy.Request(url=link,callback=self.productUrlCrawler)
    ####################################################
    def productUrlCrawler(self, response):
        '''
        function_name: productUrlCrawler
        input: self, response
        output:
        description: parse each product page of alibaba.com
        '''
        result = {}

        content = response.css('.main-content').extract()
        soup = BeautifulSoup(str(content), "html.parser")

        title, price, min_order, unit = (None,)*4

        title = soup.select("h1.ma-title")[0].text.strip()

        try:
            price = soup.select("span.ma-ref-price")[0].text.replace("\\n","").strip()
            min_order = soup.select("span.ma-min-order")[0].text.strip()
            unit = soup.select("span.ma-min-order")[0].text.strip().split('/')[1]
        except:
            pass

        ######################################################3
        result['product_name'] = title
        result['product_price'] = price
        result['product_min_order'] = min_order
        result['product_unit'] = unit

        keys = soup.find_all('dt', class_='do-entry-item')
        [keys.append(k) for k in soup.find_all('dt', class_="do-entry-item-key")]
        values = soup.find_all('dd', class_='do-entry-item-val')

        keys = [str(k.text).replace(":","").strip().replace("\\n", "") for k in keys]
        keys = [" ".join(k.split()) for k in keys]

        values = [str(v.text).replace(":","").strip().replace("\\n", "") for v in values]
        values = [" ".join(v.split()) for v in values]


        #remove pictrue fields and leadTime fields
        keys = [k for k in keys if not "picture" in str(k)]



        for i in range(0,min(len(keys),len(values))):
            result[keys[i]] = values[i]

        company_url = response.xpath('//div[@class="card-footer"]/a/@href').extract()[0]
        result['company_url'] = company_url

        yield scrapy.Request(url=company_url, callback=self.companyUrlCrawler, meta={'result': result})

    ########################################################################
    def companyUrlCrawler(selfs, response):
        '''
        function_name: companyUrlCrawler
        input: self, response
        output:
        description: parse page of each company in alibaba.com
        '''
        result = response.meta["result"]

    ########################################################################
    def supplierCrawler(self, response):
        '''
        function_name: supplierCrawler
        input: self, response
        output:
        description: parse each supplier page of alibaba.com
        '''

    ##################################################
    def parse(self, response):
        '''
        function_name: parse
        input: self, response
        output: list of result as json
        description: parse all information from search result in www.alibaba.com and go throw the next page until the last page of products and supplier
        '''

        ##Products
        if("product_en" in str(response.request.url)):
            for i in range(1,101):
                yield scrapy.Request(url=str(response.request.url) + "&page=" + str(i) + "&n=50&viewtype=G", callback=self.productCrawler)
                return

        #Suppliers
        elif("company_en" in str(response.request.url)):
            for i in range(1,101):
                yield scrapy.Request(url=str(response.request.url) + "&page=" + str(i) + "&n=50&viewtype=G", callback=self.supplierCrawler)



process = CrawlerProcess({
        'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(alibabaSpider)
process.start()