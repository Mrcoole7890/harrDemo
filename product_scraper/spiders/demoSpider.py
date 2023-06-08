import json
import scrapy
from urllib.parse import urljoin
import re
import mysql.connector
import requests
import os
from dotenv import load_dotenv

load_dotenv()


class AmazonSearchProductSpider(scrapy.Spider):
    name = "amazon_search_product"

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Cache-Control": "max-age=0",
    }

    custom_settings = {
        'DOWNLOAD_DELAY': 1 # 2 seconds of delay
    }

    discoverModeFlag = "discover"
    auditModeFlag = "audit"
    amazonKeywordSearchUrlFormat = "https://www.amazon.com/s?k={}&page={}"

    def getDBConnection(self):
        return mysql.connector.connect(
                host=os.getenv("dbHostname"),
                user=os.getenv("dbUsername"),
                password=os.getenv("dbPassword"),
                database=os.getenv("dbName")
            )

    def start_requests(self):

        if self.mode == self.discoverModeFlag:
            keyword_list = ['laptops']
            for keyword in keyword_list:
                amazon_search_url = amazonKeywordSearchUrlFormat.format(keyword,1)
                yield scrapy.Request(url=amazon_search_url, callback=self.discover_product_urls, meta={'keyword': keyword, 'page': 1}, headers=self.HEADERS)
        elif self.mode == self.auditModeFlag:
            for urls in self.getUrlList():
                yield scrapy.Request(url=urls, callback=self.parse_product_data, headers=self.HEADERS)

    def discover_product_urls(self, response):
        page = response.meta['page']
        keyword = response.meta['keyword'] 

        cssPathPerListedProduct = "div.s-result-item[data-component-type=s-search-result]"
        cssPathPerLinkOfListedProduct = "h2>a::attr(href)"
        amazonHTTPAdress = "https://www.amazon.com/"
        cssPathToNumberOfPagesAvailible = '//*[contains(@class, "s-pagination-item")][not(has-class("s-pagination-separator"))]/text()'

        ## Discover Product URLs
        search_products = response.css(cssPathPerListedProduct)
        for product in search_products:
            relative_url = product.css(cssPathPerLinkOfListedProduct).get()
            product_url = urljoin(amazonHTTPAdress, relative_url).split("?")[0]
            yield scrapy.Request(url=product_url, callback=self.parse_product_data, meta={'keyword': keyword, 'page': page})
            
        ## Get All Pages
        if page == 1:
            available_pages = response.xpath(cssPathToNumberOfPagesAvailible).getall()
            last_page = available_pages[-1]
            for page_num in range(2, int(last_page)):
                amazon_search_url = amazonKeywordSearchUrlFormat.format(keyword,page)
                yield scrapy.Request(url=amazon_search_url, callback=self.discover_product_urls, meta={'keyword': keyword, 'page': page_num})

    def parse_product_data(self, response):

        cssPathOfProductName = "#productTitle::text"
        cssPathOfPrice = '.a-price span[aria-hidden="true"] ::text'
        cssPathOfProductImage = '#ivLargeImage > img::attr(src)'
        cssPathOfPrice2 = '.a-price .a-offscreen ::text'
        sqlInsertNewEntry = "INSERT INTO awslistings (asin, price, name) VALUES ({}, {}, {})"
        sqlSelectAllASINawsListings = "SELECT asin FROM awslistings WHERE asin='{}'"
        sqlSelectAllPricwawsListings = "SELECT price FROM awslistings WHERE asin='{}'"
        noResultsForProductWithASIN = "AHHHHHHHHHHHHHHHHHHH {} is not found"
        priceDifferenceLog = "\n\n\n\n\n\n\n {} : {} \n\n\n\n\n\n\n"

        asin = response.url.split("/")[4].replace("?th=1", "") if self.mode == self.auditModeFlag else response.url.split("/")[5]
        name = response.css(cssPathOfProductName).get("").strip()
        price = response.css(cssPathOfPrice).get("")
        img = response.css(cssPathOfProductImage).get("")
        if not price:
            price = response.css(cssPathOfPrice2).get("")
        yield {
            "name": name,
            "price": price,
            "ASIN": asin
        }

        if self.mode != self.auditModeFlag:
            mydb = self.getDBConnection()
            mycursor = mydb.cursor()
            mycursor.execute(sqlInsertNewEntry.format(asin, price, name))
            mydb.commit()
            mydb.close()

        else:
            mydb = self.getDBConnection()
            mycursor = mydb.cursor()

            mycursor.execute(sqlSelectAllASINawsListings.format(asin))
            asinFromDB = mycursor.fetchone()
            if asinFromDB == None:
                print(noResultsForProductWithASIN.format(asin))
                return
            mycursor.execute(sqlSelectAllPricwawsListings.format(asin))
            priceFromDB = mycursor.fetchone()
            if priceFromDB == None:
                print(noResultsForProductWithASIN.format(asin))
                return

            print(price)

            if self.isNewPriceLessThanOldOne(float(priceFromDB[0][1:].replace(",", "")), float(price[1:].replace(",", ""))):
                print(priceDifferenceLog.format(price, priceFromDB[0]))
                mes = [name, str(float(priceFromDB[0][1:].replace(",", "")) - float(price[1:].replace(",", ""))), response.url, img]
                self.sendMessageToDiscord(mes)

            
            mydb.close()
            
    def getUrlList(self):

        selectAllAsin = "SELECT asin FROM awslistings"
        amazonURLByAsin = "http://www.amazon.com/dp/{}"

        mydb = self.getDBConnection()
        mycursor = mydb.cursor()
        mycursor.execute(selectAllAsin)
        myresult = mycursor.fetchall()
        mydb.close()
        finalresult = []
        for x in myresult:
            finalresult.append(amazonURLByAsin.format(x[0]))

        return finalresult


    def getTestUrl(self):
        return ["https://www.amazon.com/dp/B0BSB1T1NW?th=1"]


    def sendMessageToDiscord(self, message):
        # The API endpoint to communicate with
        url_post = "https://discord.com/api/webhooks/1109646951793840158/6GdjC3ME3yvDJh8k1ou4EFHRJMwZox_She0bWxPjLyXrKEgJSd0W4yldHmkWyvNY_dPN"
        new_data = {
            "embeds": [
                {
                    "type": "rich",
                    "title": message[0],
                    "description": "_Price difference:_ {0} \n\n\n _Link:_ {1}".format(message[1], message[2]),
                    "color": 65535,
                    "thumbnail": {
                        "url": message[3],
                        "height": 0,
                        "width": 0
                    }
                }
            ]
        }
        # A POST request to tthe API
        post_response = requests.post(url_post, json=new_data)

    def isNewPriceLessThanOldOne(self, value1, value2):
        return value1 > value2
        #return float(priceFromDB[0][1:].replace(",", "")) > float(price[1:].replace(",", ""))