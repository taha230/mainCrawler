# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/spider-middleware.html

from scrapy import signals
from scrapy.downloadermiddlewares.retry import RetryMiddleware
from twisted.internet.error import TCPTimedOutError, TimeoutError
import logging
from twisted.internet import defer
from twisted.internet.error import TimeoutError, DNSLookupError, \
        ConnectionRefusedError, ConnectionDone, ConnectError, \
        ConnectionLost, TCPTimedOutError
from twisted.web.client import ResponseFailed

from scrapy.exceptions import NotConfigured
from scrapy.utils.response import response_status_message
from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.utils.python import global_object_name
import requests
import json
import urllib

class EworldtradeSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.

        # Should return None or raise an exception.
        return None

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.

        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        pass

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)


class EworldtradeDownloaderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the downloader middleware does not modify the
    # passed objects.

    EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
                           ConnectionRefusedError, ConnectionDone, ConnectError,
                           ConnectionLost, TCPTimedOutError, ResponseFailed,
                           IOError, TunnelError)
    requestCallCount = 0

    headers = {
        'User-Agent': 'Mozilla\/5.0 (compatible MSIE 10.0 Windows Phone 8.0 Trident\/6.0 IEMobile\/10.0 ARM Touch NOKIA Lumia 520)',
    }

    ##################################################
    def change_proxy(self):
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

    @classmethod

    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls()
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        return s

    def process_request(self, request, spider):

        self.requestCallCount += 1

        # changed the proxy for each 100 requests count
        if self.requestCallCount == 10:
            proxy, userAgent = self.change_proxy()
            request.meta['proxy'] = "http://" + proxy
            self.headers['User-Agent'] = userAgent
            request.meta['headers'] = self.headers
            self.requestCallCount = 0

        return None

    def process_response(self, request, response, spider):
        # Called with the response returned from the downloader.

        # Must either;
        # - return a Response object
        # - return a Request object
        # - or raise IgnoreRequest
        return response

    def process_exception(self, request, exception, spider):

        if isinstance(exception, self.EXCEPTIONS_TO_RETRY):
            print("Network Connection Exception Occured >>>>>>>>>>>>> Retry ")
            return self._retry(request, exception, spider)
        elif isinstance(exception, urllib.error.HTTPError):
            if exception.code == 403:
                proxy, userAgent = self.change_proxy()
                request.meta['proxy'] = "http://" + proxy
                self.headers['User-Agent'] = userAgent
                request.headers = self.headers
                self.requestCallCount = 0
                self._retry(request, exception, spider)
        else:
            pass

    def _retry(self, request, reason, spider):
        '''
            function_name: _retry
            input: request, reason, spider
            output: request
            description: retry the request fo connection exceptions
        '''
        retryreq = request.copy()
        retryreq.dont_filter = True
        return retryreq

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
