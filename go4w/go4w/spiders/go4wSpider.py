# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerProcess
import json
import re
from pymongo import MongoClient
import time
from progress.bar import Bar
from scrapy.conf import settings
from scrapy.spiders import Spider
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from twisted.internet.error import TCPTimedOutError, TimeoutError


##################################################
##################################################
def create_category_url():
    '''
    function_name: create_category_url
    input: none
    output: start_urls for scrapy
    description: add products to urls from products_alibaba.json file
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
###########################################################################################
class go4wSpider(scrapy.Spider):
    name = 'go4w_spider'
    allowed_domains = ['go4worldbusiness.com']
    start_urls = create_category_url()

    # Connect to MongoDB
    client = MongoClient('192.168.1.3', 27017)
    db = client['CrawlingData']
    collection_go4w_data = db['go4w_data']

    crawlingEntityCategoryCount = open("totalEntityCategory_go4w.txt", "w")
    #crawlingEntityCategoryCount2 = open("totalEntityCategory_go4w_total.txt", "w")
    bar = Bar('Crawling go4w', max=len(start_urls))
    progressBarIter = 0
    totalEntityNumber = 0
    collection_go4w_data.delete_many({})  # Delete all documents in collection
    #settings.overrides['SCHEDULER_ORDER'] = 'DFO'

    #settings.set('SCHEDULER_ORDER', 'BFO')
    #settings.set('DOWNLOAD_TIMEOUT', 1 * 60)


    #print(settings.attributes.get('SCHEDULER_ORDER'))
    print(settings.attributes.get('DOWNLOADER_MIDDLEWARES'))

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

            try:

            #####Extract information from 'supplierText' string
                temp = str(result['supplierText'])[2:-2]
                temp = re.sub(' +', ' ', temp)
                tokens_ = temp.split("\\r\\n")
                for t in tokens_:
                    clean_t_ = t.split(":")
                    if (len(clean_t_) == 2 and len(clean_t_[0].replace('\\n', '').replace(' ','').strip()) > 0 and len(clean_t_[1].replace(' ','').replace('\\n', '').strip()) >0):
                        newResult[str(clean_t_[0].replace('\\n', '').replace(' ','').replace('.','').strip())] = clean_t_[1].replace(' ','').replace('\\n', '').strip()
                        #newResult['a'] = clean_t_[1].replace(' ','').replace('\\n', '').strip()

            except:
                pass

        elif(result['isBuyer']):

            #####Extract information from 'buyerText' string
            temp = str(result['buyerText'])[2:-2]
            temp = re.sub(' +', ' ', temp)
            tokens_ = temp.split("\\r\\n")
            for t in tokens_:
                clean_t_ = t.split(":")
            if (len(clean_t_) == 2 and len(clean_t_[0].replace('\\n', '').replace(' ','').strip()) > 0 and len(clean_t_[1].replace('\\n', '').replace(' ','').strip()) > 0):
                newResult[str(clean_t_[0].replace('\\n', '').replace(' ','').strip())] = clean_t_[1].replace('\\n', '').replace(' ','').strip()

        # update result document in MongoDB

        if (newResult != {}):
            self.collection_go4w_data.update({'Key': result['Key']},{'$set': newResult})

    ############################################################
    def nestedURLGeneralCompanyCrawler(self, response):
        '''
        function_name: nestedURLBuyerCompanyCrawler
        input: response
        output: json
        description: crawl all inforamtion from company nested url
        '''
        result = response.meta["inputJson"]

        ourCompanySelector = '//ul[@class="nav nav-pills center-pills"]/li/a[contains(text(),"Company")]/@href'
        productsSelector = '//ul[@class="nav nav-pills center-pills"]/li/a[contains(text(),"Products")]/@href'
        managementSelector = '//ul[@class="nav nav-pills center-pills"]/li/a[contains(text(),"Management")]/@href'
        facilitiesSelector = '//ul[@class="nav nav-pills center-pills"]/li/a[contains(text(),"Facilities")]/@href'
        newsRoomSelector = '//ul[@class="nav nav-pills center-pills"]/li/a[contains(text(),"News Room")]/@href'

        isOurCompanySelected = response.xpath(ourCompanySelector).extract()
        isProductsSelected = response.xpath(productsSelector).extract()
        isManagementSelected = response.xpath(managementSelector).extract()
        isFacilitiesSelector = response.xpath(facilitiesSelector).extract()
        isNewsRoomSelector = response.xpath(newsRoomSelector).extract()

        if ("html" in str(isOurCompanySelected) and (str(isOurCompanySelected)[2:-2] in response.request.url)): # the second condition for nested page in company page

            # new class-based selector
            ourCompanyTextSelector = '//div[@class="padd-lr-10 mar-top-10 "]/div[@class="row"]/div[@class="col-xs-12"]/p/text()'
            annualSalesRegionBSelector = '//div[@class="padd-lr-10 mar-top-10 "]/b/text()'
            annualSalesRegionBTextSelector = '//div[@class="padd-lr-10 mar-top-10 "]/text()'
            contactDetailsTextSelector = '//div[@class="padd-lr-10 mar-top-10 "]/address'
            companyLogoSelector = '//div[@class="nopadding  mar-top-10 col-xs-offset-0 col-xs-11 gold-menu text-center"]/img/@src'


            companyLogo = response.xpath(companyLogoSelector).extract()

            annualSalesBTagsList = response.xpath(annualSalesRegionBSelector).extract()
            annualSalesBTagsValueList = self.clean_backslashN_array(response.xpath(annualSalesRegionBTextSelector).extract())



            ourCompanyText = str(response.xpath(ourCompanyTextSelector).extract())

            contactDetailText = str(response.xpath(contactDetailsTextSelector).extract()).replace('<address>','').replace('</address>', '').replace('<br>' ,',').replace('<b>',',').replace('</b>',',')


            newResult = {}
            newResult['logoSrc'] = self.clean_text_(str(companyLogo))
            newResult['ourCompany'] = self.clean_text_(ourCompanyText)
            newResult['contactDetail'] = self.clean_text_(contactDetailText)

            for i in range(len(annualSalesBTagsList)):
                if i < len(annualSalesBTagsValueList):
                    newResult[str(annualSalesBTagsList[i])] = annualSalesBTagsValueList[i].replace(' : ','')

            # update result document in MongoDB

            self.collection_go4w_data.update({'Key': result['Key']},
                                 {'$set': newResult})



        if ((str(isOurCompanySelected)[2:-2] not in response.request.url) and ("html" in str(isOurCompanySelected))):
            nestedURLOurCompany = "https://www.go4worldbusiness.com" + str(isOurCompanySelected)[2:-2]
            yield scrapy.Request(url=nestedURLOurCompany, callback=self.nestedURLOurCompanyCrawler,
                                 meta={'inputJson': result})
        if ("html" in str(isProductsSelected)):
           nestedURLProductsCompany = "https://www.go4worldbusiness.com" + str(isProductsSelected)[2:-2]
           yield scrapy.Request(url=nestedURLProductsCompany, callback=self.nestedURLProductsCompanyCrawler,
                                meta={'inputJson': result})

        if ("html" in str(isManagementSelected)):
           nestedURLManagementCompany = "https://www.go4worldbusiness.com" + str(isManagementSelected)[2:-2]
           yield scrapy.Request(url=nestedURLManagementCompany, callback=self.nestedURLManagementCompanyCrawler,
                                   meta={'inputJson': result})
        if ("html" in str(isFacilitiesSelector)):
           nestedURLFaculitiesCompany = "https://www.go4worldbusiness.com" + str(isFacilitiesSelector)[2:-2]
           yield scrapy.Request(url=nestedURLFaculitiesCompany, callback=self.nestedURLFacilitiesCompanyCrawler,
                                   meta={'inputJson': result})
        if ("html" in str(isNewsRoomSelector)):
           nestedURLNewsRoomCompany = "https://www.go4worldbusiness.com" + str(isNewsRoomSelector)[2:-2]
           yield scrapy.Request(url=nestedURLNewsRoomCompany, callback=self.nestedURLNewsRoomCompanyCrawler,
                                   meta={'inputJson': result})

    ########################################################################
    def nestedURLOurCompanyCrawler(self, response):
        '''
        function_name: nestedURLBuyerOurCompanyCrawler
        input: response
        output: buyer Our company information as json
        description: crawl all information from buyer Ourcompany product from nested url
        '''

        result = response.meta["inputJson"]

        # new class-based selector
        ourCompanyTextSelector = '//div[@class="padd-lr-10 mar-top-10 "]/div[@class="view"]/div[@class="col-xs-12"]/p/text()'
        annualSalesRegionBSelector = '//div[@class="padd-lr-10 mar-top-10 "]/b/text()'
        annualSalesRegionBTextSelector = '//div[@class="padd-lr-10 mar-top-10 "]/text()'
        contactDetailsTextSelector = '//div[@class="padd-lr-10 mar-top-10 "]/address'
        companyLogoSelector = '//div[@class="nopadding  mar-top-10 col-xs-offset-0 col-xs-11 gold-menu text-center"]/img/@src'
        companyNameSelector = '//h1[@class="mar-bot-0"]/text()'

        companyLogo = response.xpath(companyLogoSelector).extract()

        ourCompanyName = response.xpath(companyNameSelector).extract()


        annualSalesBTagsList = response.xpath(annualSalesRegionBSelector).extract()
        annualSalesBTagsValueList = self.clean_backslashN_array(
        response.xpath(annualSalesRegionBTextSelector).extract())

        ourCompanyText = str(response.xpath(ourCompanyTextSelector).extract())

        contactDetailText = str(response.xpath(contactDetailsTextSelector).extract()).replace('<address>', '').replace(
            '</address>', '').replace('<br>', ',').replace('<b>', ',').replace('</b>', ',')

        newResult = {}
        newResult['logoSrc'] = self.clean_text_(str(companyLogo))
        newResult['ourCompany'] = self.clean_text_(ourCompanyText)
        newResult['contactDetail'] = self.clean_text_(contactDetailText)
        newResult['ourCompanyName'] = self.clean_text_(ourCompanyName)

        for i in range(len(annualSalesBTagsList)):
            if i < len(annualSalesBTagsValueList):
                newResult[str(annualSalesBTagsList[i])] = annualSalesBTagsValueList[i].replace(' : ', '')


        # update result document in MongoDB

        self.collection_go4w_data.update({'Key': result['Key']},
                                         {'$set': newResult})

    ########################################################################
    def nestedURLProductsCompanyCrawler(self, response):
        '''
        function_name: nestedURLBuyerProductsCompanyCrawler
        input: response
        output: buyer company product information as json
        description: crawl all information from buyer company product from nested url
        '''

        result = response.meta["inputJson"]

        productsDivSelector = "//div[@class='padd-lr-10 mar-top-10 ']/div/div"

        productNameList = []
        productTextList = []
        productImageList = []

        # get all div section in products
        for divSet in response.xpath(productsDivSelector):
            productNameString = str(divSet.xpath("./div/a/h5/span/text()").extract())
            if (productNameString != "[]"): productNameList.append(productNameString)
            productTextString = str(divSet.xpath("./div/p/text()").extract())
            if (productTextString != "[]"): productTextList.append(productTextString)
            productImageString = str(divSet.xpath("./div/img/@src").extract())
            if (productImageString != "[]"): productImageList.append(productImageString)

        productList = []

        for i in range(len(productNameList)):
            productTex = ""
            ProductImageSrc = ""

            if (i < len(productTextList)): productTex = productTextList[i]
            if (i< len(productImageList)): ProductImageSrc = productImageList[i]

            product = {
                'productName': self.clean_text_(productNameList[i]),
                'productText': self.clean_text_(productTex),
                'ProductImageSrc': self.clean_text_(ProductImageSrc),
            }
            productList.append(product)

        newResult = {}

        newResult["productList"] = productList

        # update result document in MongoDB

        self.collection_go4w_data.update({'Key': result['Key']},
                                         {'$set': newResult})

    #########################################################################
    def nestedURLManagementCompanyCrawler(self, response):
        '''
        function_name: nestedURLBuyerManagementCompanyCrawler
        input: response
        output: buyer company Management information as json
        description: crawl all information from buyer company Management from nested url
        '''

        result = response.meta["inputJson"]

        managementDiv1Selector = '//div[@class="row"]/div/p/text()'


        managementText = str(response.xpath(managementDiv1Selector).extract())

        newResult ={}
        newResult['managementText'] = self.clean_text_(managementText)

        # update result document in MongoDB

        self.collection_go4w_data.update({'Key': result['Key']},
                                         {'$set': newResult})

    ##########################################################################
    def nestedURLFacilitiesCompanyCrawler(self, response):
        '''
        function_name: nestedURLBuyerFacilitiesCompanyCrawler
        input: response
        output: buyer company Facilities information as json
        description: crawl all information from buyer company Facilities from nested url
        '''

        result = response.meta["inputJson"]

        facilitiesDivSelector = "//div[@class='padd-lr-10 mar-top-10 ']/div"
        facilitiesText ="";

        # get all div section in facilities
        for divSet in response.xpath(facilitiesDivSelector):
            facilitiesText += str(divSet.xpath("./div/p/text()").extract())
            facilitiesText += str(divSet.xpath("./div/b/text()").extract())
            facilitiesText += str(divSet.xpath("./div/text()").extract())

        newResult = {}
        newResult['facilitiesText'] = self.clean_text_(facilitiesText)

        # update result document in MongoDB

        self.collection_go4w_data.update({'Key': result['Key']},
                                         {'$set': newResult})

    ##########################################################################
    def nestedURLNewsRoomCompanyCrawler(self, response):
        '''
        function_name: nestedURLBuyerNewsRoomCompanyCrawler
        input: response
        output: buyer company NewsRoom information as json
        description: crawl all information from buyer company NewsRoom from nested url
        '''

        result = response.meta["inputJson"]

        newsRoomDiv1Selector = '//div[@class="row"]/div/p/text()'


        newsRoomText = str(response.xpath(newsRoomDiv1Selector).extract())

        newResult = {}
        newResult['newsRoomText'] = self.clean_text_(newsRoomText)

        # update result document in MongoDB

        self.collection_go4w_data.update({'Key': result['Key']},
                                         {'$set': newResult})

    #########################################################################
    def buyerCrawler(self, response):
        '''
        function_name: buyerCrawler
        input: response
        output: crawlerDataset
        description: crawl all information from buyer in www.go4worldbusiness.com
        '''
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

        #self.bar.next()
        #self.progressBarIter += 1


        return outJson

    ###################################################
    def supplierCrawler(self, response):
        '''
        function_name: supplierCrawler
        input: response
        output: list of result as json
        description: crawl all information from supplier in www.go4worldbusiness.com
        '''
        searchResultSelector = '//div[@class="col-xs-12 nopadding search-results"]'
        dateSearchResultSelector = './/div[@class="col-xs-3 col-sm-2 xs-padd-lr-2 nopadding"]/small/text()'
        supplierCountrySelector = './/span[@class="pull-left subtitle text-capitalize"]/text()'
        supplierCompanyLinkSelector = './/span[@class="pull-left"]/a/@href'
        supplierCompanyNameSelector = './/span[@class="pull-left"]/a/h2/span/text()'
        supplierTextSelector = './/div[@class="col-xs-12 entity-row-description-search xs-padd-lr-5"]/p/text()'
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

        #self.bar.next()
        #self.progressBarIter += 1

        return outJson

    ##################################################
    def parse(self, response):
        '''
        function_name: parse
        input: self, response
        output: list of result as json
        description: parse all information from search result in www.go4worldbusiness.com and go throw the next page until the last page of supplier and buyer
        '''

        lastPageSelector = './/ul[@class="pagination"]/li/a/@href'

        lastPagelist = response.xpath(lastPageSelector).extract()
        #########################################################
        isBuyerSelected, isSupplierSelected = False, False
        if("/suppliers/" in str(response.request.url)):
            isSupplierSelected = True
        if ("/buyers/" in str(response.request.url)):
            isBuyerSelected = True

        self.progressBarIter += 1
        self.bar.next()

        #self.collection_go4w_data.delete_many({}) # Delete all documents in collection

        start_time = time.time()

        if (isBuyerSelected):
            try:
                lastPageBuyerhref = lastPagelist[len(lastPagelist) - 1]
                if ( "pg_buyers" not in lastPageBuyerhref ): # category has only one page of buyer
                    yield scrapy.Request(url=response.request.url, callback=self.buyerCrawler)
                else:
                    buyerTotalPageNum = int (str(lastPageBuyerhref).split('pg_buyers')[1].split('=')[1]) # parse the buyer total page number
                    self.totalEntityNumber += (buyerTotalPageNum * 20)
                    self.crawlingEntityCategoryCount.write(str(response.request.url).replace('https://www.go4worldbusiness.com/','') + "--- %s search result ---" % (buyerTotalPageNum  * 20) + '\n')
                    for buyerPageNum in range(buyerTotalPageNum) :
                        next_page = response.request.url+"?region=worldwide&pg_buyers=" + str(buyerPageNum+1) # +1 to start from 1 to buyerPageNum
                        yield scrapy.Request(url=next_page, callback=self.buyerCrawler)
                        #break
            except (IndexError, ConnectionError) as error:
                pass

        elif (isSupplierSelected):
            try:
                lastPageSupplierhref = lastPagelist[len(lastPagelist) - 1]
                if ( "pg_suppliers" not in lastPageSupplierhref ): # category has only one page of supplier
                    yield scrapy.Request(url=response.request.url, callback=self.supplierCrawler)
                else:
                    supplierTotalPageNum = int (str(lastPageSupplierhref).split('pg_suppliers')[1].split('=')[1]) # parse the supplier total page number
                    self.totalEntityNumber += (supplierTotalPageNum * 20)
                    self.crawlingEntityCategoryCount.write(str(response.request.url).replace('https://www.go4worldbusiness.com/','') + "--- %s search result ---" % (supplierTotalPageNum * 20)+ '\n')
                    for supplierPageNum in range(supplierTotalPageNum) :
                        next_page = response.request.url+"?region=worldwide&pg_suppliers=" + str(supplierPageNum+1) # +1 to start from 1 to supplierPageNum
                        yield scrapy.Request(url=next_page, callback=self.supplierCrawler)
                        #break
            except (IndexError, ConnectionError) as error:
                pass

        #time.sleep(1)


        if (self.progressBarIter == len(self.start_urls)) :
            self.crawlingEntityCategoryCount.write(">>>>>>>>>>>>>>>>>>>>>>>>> %s TOTAL search result <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<" % self.totalEntityNumber + '\n')

            self.bar.finish()
            time.sleep(10)
            self.crawlingEntityCategoryCount.flush()
            self.crawlingEntityCategoryCount.close()



process = CrawlerProcess({
    #'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'
})

process.crawl(go4wSpider)

process.start()
