# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerProcess
import json
import re
from pymongo import MongoClient
import time
from progress.bar import Bar
from scrapy.conf import settings


##################################################
##################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: start_urls for scrapy
    description: add products to urls from products_alibaba.json file
    '''
    with open('products_eworldtrade.json') as f:
        products = json.load(f)
    #print(products[0])
    urls = []
    products = products[0:1]
    for p in list(products):
        urls.append("https://www.eworldtrade.com/search/?type=product&s=" + str(p['name']).replace(' ',"+"))
        urls.append("https://www.eworldtrade.com/search/?type=seller&s=" + str(p['name']).replace(' ',"+"))
        return urls

    return urls
##################################################
####################################################
class eworldtradeSpider(scrapy.Spider):
    name = 'eworldtrade_spider'
    allowed_domains = ['eworldtrade.com']
    start_urls = create_category_url()

    # Connect to MongoDB
    #client = MongoClient('192.168.1.3', 27017)
    #db = client['CrawlingData']
    #collection_eworldtrade_data = db['eworldtrade_data']

    crawlingTimefile = open("crawlingTime_eworldtrade.txt", "w")
    crawlingTimefile.writelines("Crawling Time for eworldtrade Categories :"  + '\n')
    bar = Bar('Crawling eworldtrade', max=len(start_urls))
    progressBarIter = 1

    #settings.set('SCHEDULER_ORDER', 'DFO')
    #settings.set('DOWNLOAD_TIMEOUT', 30 * 60)
    #print(settings.attributes.get('SCHEDULER_ORDER'))
    #a =0

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
            if (not(type(value) == bool)):
                if(type(value) == list):
                    text_1 = self.clean_html(str(value)[2:-2]) # remove html tags from text
                    text_2 = text_1.replace("\']","").replace("[\'","")
                    text_3 = text_2.lstrip().rstrip() # remove whitespaces from begin and end of string
                    text_4 = text_3.replace('\\n','').strip() # remove new line chracters from string
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
        text_1 = self.clean_html(str(text)[2:-2]) # remove html tags from text
        text_2 = text_1.lstrip().rstrip() # remove whitespaces from begin and end of string
        text_3 = text_2.replace('\\n','').strip() # remove new line chracters from string
        text_4 = re.sub(' +', ' ', text_3)
        text_5 = text_4.replace('[]','').strip() # remove  [] for empty list
        text_6 = text_5.replace('\']', '').replace('[\'', '')  # remove [' and '] for start and end list
        text_7 = text_6.replace('\\r', '')  # remove \r from text

        return text_7
    ###################################################
    def clean_backslashN_array(self, inputArray):
        '''
        function_name: clean_backslashN_array
        input: array
        output: cleaned array
        description: Clean all elements of array have \n
        '''
        outArray=[]
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

        newResult = {} # json to add MongoDB

        if(result['isSupplier']):

            #####Extract information from 'supplierText' string
            temp = str(result['supplierText'])[2:-2]
            temp = re.sub(' +', ' ', temp)
            tokens_ = temp.split("\\r\\n")
            for t in tokens_:
                clean_t_ = t.split(":")
                if (len(clean_t_) == 2 and len(clean_t_[0]) > 0 and len(clean_t_[1]) > 0):
                    newResult[str(clean_t_[0].replace('\\n', '').strip())] = clean_t_[1].replace('\\n', '').strip()

        elif(result['isBuyer']):

            #####Extract information from 'buyerText' string
            temp = str(result['buyerText'])[2:-2]
            temp = re.sub(' +', ' ', temp)
            tokens_ = temp.split("\\r\\n")
            for t in tokens_:
                clean_t_ = t.split(":")
            if (len(clean_t_) == 2 and len(clean_t_[0]) > 0 and len(clean_t_[1]) > 0):
                newResult[str(clean_t_[0].replace('\\n', '').strip())] = clean_t_[1].replace('\\n', '').strip()

        # update result document in MongoDB

       # if (newResult != {}) : self.collection_eworldtrade_data.update({'Key': result['Key']},{'$set': newResult})

    def productCrawler(self, response):
        '''
        function_name: buyerCrawler
        input: response
        output: crawlerDataset
        description: crawl all information from buyer in www.eworldtrade.com
        '''
        return
        searchResultSelector = '//div[@class="col-xs-12 nopadding search-results"]'
        dateSearchResultSelector = './/div[@class="col-xs-3 col-sm-2 xs-padd-lr-2 nopadding"]/small/text()'
        buyerProductNameSelector = './/h2[@class="text-capitalize entity-row-title h2-item-title"]/span/text()'
        buyerCountrySelector = './/span[@class="pull-left subtitle text-capitalize"]/text()'
        buyerTextSelector = './/div[@class="col-xs-12 entity-row-description-search xs-padd-lr-5"]/p/text()'
        buyerBuyerOFSelector = './/div[@class="col-xs-12 xs-padd-lr-5"]/div/a/text()'
        buyerCompanyNameSelector = './/h2[@class="entity-row-title h2-item-title"]/span/text()'
        buyerCompanyLinkSelector = './/span[@class="pull-left"]/a/@href'

        outJson = []
        for searchResultSet in response.xpath(searchResultSelector):
            result = {
                'buyerCompanyName': searchResultSet.xpath(buyerCompanyNameSelector).extract(),
                'date': searchResultSet.xpath(dateSearchResultSelector).extract(),
                'buyerProductName': self.clean_text_(str(searchResultSet.xpath(buyerProductNameSelector).extract()).replace('Wanted :','')),
                'buyerCountry': self.clean_text_(str(searchResultSet.xpath(buyerCountrySelector).extract()).replace('Buyer From','')),
                'buyerText': searchResultSet.xpath(buyerTextSelector).extract(),
                'buyerBuyerOF': self.clean_text_(str(searchResultSet.xpath(buyerBuyerOFSelector).extract()).replace('Buyer Of','')),
                'buyerCompanyLink': searchResultSet.xpath(buyerCompanyLinkSelector).extract(),
                'isSupplier': False,
                'isBuyer': True,
                'Key': str(self.clean_text_(str(searchResultSet.xpath(dateSearchResultSelector).extract())) + ' , ' + self.clean_text_(str(searchResultSet.xpath(buyerCompanyLinkSelector).extract()))).replace(' ',''),
                'searchCategory' : str(response.request.url).replace('https://www.go4worldbusiness.com/find?searchText=','').replace('&FindBuyers','')
            }

            # Add to MongoDB
            clean_result = self.clean_text(result)
            self.collection_go4w_data.insert_one(clean_result)

            #Nested company link crawler
            if (result['buyerCompanyName'] != []): # crawl nested company link for cases have type buyerCompanyName (not person link and with star in front)
                nestedURLCompany = "https://www.go4worldbusiness.com" + str(result['buyerCompanyLink'])[2:-2]
                yield scrapy.Request(url=nestedURLCompany, callback=self.nestedURLGeneralCompanyCrawler, meta={'inputJson': result})
            else:
                self.tokenize_buyer_or_supplier_text(result)

            outJson.append(result)

        self.bar.next()
        self.progressBarIter += 1


        return outJson
    ###################################################
    def supplierCrawler(self, response):
        '''
        function_name: supplierCrawler
        input: response
        output: list of result as json
        description: crawl all information from supplier in www.eworldtrade.com
        '''
        return

        searchResultSelector = '//div[@class="col-xs-12 nopadding search-results"]'
        dateSearchResultSelector = './/div[@class="col-xs-3 col-sm-2 xs-padd-lr-2 nopadding"]/small/text()'
        supplierCountrySelector = './/span[@class="pull-left subtitle text-capitalize"]/text()'
        supplierCompanyLinkSelector = './/span[@class="pull-left"]/a/@href'
        supplierCompanyNameSelector = './/span[@class="pull-left"]/a/h2/span/text()'
        supplierTextSelector = './/div[@class="col-xs-12 entity-row-description-search product-list-search xs-padd-lr-5"]/p/text()'
        supplierSupplierOFSelector = './/div[@class="col-xs-12 xs-padd-lr-5"]/div/a/text()'

        outJson = []
        for searchResultSet in response.xpath(searchResultSelector):

            result = {
                'date': searchResultSet.xpath(dateSearchResultSelector).extract(),
                'supplierCompanyName': searchResultSet.xpath(supplierCompanyNameSelector).extract(),
                'supplierCountry': self.clean_text_(str(searchResultSet.xpath(supplierCountrySelector).extract()).replace('Supplier From','')),
                'supplierCompanyLink': searchResultSet.xpath(supplierCompanyLinkSelector).extract(),
                'supplierText': searchResultSet.xpath(supplierTextSelector).extract(),
                'supplierSupplierOF': self.clean_text_(str(searchResultSet.xpath(supplierSupplierOFSelector).extract()).replace('Supplier Of','')),
                'isSupplier': True,
                'isBuyer': False,
                'Key':  str (self.clean_text_(str(searchResultSet.xpath(dateSearchResultSelector).extract())) + ' , ' + self.clean_text_(str(searchResultSet.xpath(supplierCompanyLinkSelector).extract()))).replace(' ',''),
                'searchCategory': str(response.request.url).replace('https://www.go4worldbusiness.com/find?searchText=','').replace('&FindSuppliers', '')
            }

            # Add to MongoDB
            clean_result = self.clean_text(result)



            self.collection_go4w_data.insert_one(clean_result)

            # crawl nested company link
            if (result['supplierCompanyLink'] != []): # crawl nested company link for cases have supplier company link
                self.tokenize_buyer_or_supplier_text(result)
                nestedURLCompany = "https://www.go4worldbusiness.com" + str(result['supplierCompanyLink'])[2:-2]
                yield scrapy.Request(url=nestedURLCompany, callback=self.nestedURLGeneralCompanyCrawler, meta={'inputJson': result})

            outJson.append(result)

        self.bar.next()
        self.progressBarIter += 1

        return outJson
    ##################################################
    def parse(self, response):
        '''
        function_name: parse
        input: self, response
        output: list of result as json
        description: parse all information from search result in www.eworldtrade.com and go throw the next page until the last page of supplier and buyer
        '''

        lastPageSelector = './/ul[@class="pagination"]/li/a/@href'
        lastPagelist = response.xpath(lastPageSelector).extract()
        lastPageBuyerhref = lastPagelist[len(lastPagelist) - 1]
        #########################################################

        isProductSelected, isSupplierSelected = False, False
        if ("/product/" in str(response.request.url)):
            isProductSelected = True
        if ("/seller/" in str(response.request.url)):
            isSupplierSelected = True

        if (isProductSelected):
            productTotalPageNum = int (str(lastPageBuyerhref).split('&')[1].split('=')[1]) # parse the buyer total page number
            for productPageNum in range(productTotalPageNum) :
                next_page = response.request.url+"&page=" + str(productPageNum+1) # +1 to start from 1 to buyerPageNum
                yield scrapy.Request(url=next_page, callback=self.productCrawler)
                if (productPageNum > 50):
                    break
                #break

        elif (isSupplierSelected):
            supplierTotalPageNum = int (str(lastPageBuyerhref).split('&')[1].split('=')[1]) # parse the supplier total page number
            for supplierPageNum in range(supplierTotalPageNum) :
                next_page = response.request.url+"&pg_suppliers=" + str(supplierPageNum+1) # +1 to start from 1 to supplierPageNum
                yield scrapy.Request(url=next_page, callback=self.supplierCrawler)
                if (supplierPageNum > 50):
                    break
                #break



process = CrawlerProcess({
    'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})


process.crawl(eworldtradeSpider)
process.start()


