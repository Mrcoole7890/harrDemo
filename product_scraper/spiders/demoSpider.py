import json
import scrapy
from urllib.parse import urljoin
import re
import mysql.connector
import requests



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

    def start_requests(self):

        if self.mode == "discover":
            keyword_list = ['laptops']
            for keyword in keyword_list:
                amazon_search_url = f'https://www.amazon.com/s?k={keyword}&page=1'
                yield scrapy.Request(url=amazon_search_url, callback=self.discover_product_urls, meta={'keyword': keyword, 'page': 1}, headers=self.HEADERS)
        elif self.mode == "audit":
            for urls in self.getUrlList():
                yield scrapy.Request(url=urls, callback=self.parse_product_data, headers=self.HEADERS)

    def discover_product_urls(self, response):
        page = response.meta['page']
        keyword = response.meta['keyword'] 

        ## Discover Product URLs
        search_products = response.css("div.s-result-item[data-component-type=s-search-result]")
        for product in search_products:
            relative_url = product.css("h2>a::attr(href)").get()
            product_url = urljoin('https://www.amazon.com/', relative_url).split("?")[0]
            yield scrapy.Request(url=product_url, callback=self.parse_product_data, meta={'keyword': keyword, 'page': page})
            
        ## Get All Pages
        if page == 1:
            available_pages = response.xpath(
                '//*[contains(@class, "s-pagination-item")][not(has-class("s-pagination-separator"))]/text()'
            ).getall()

            last_page = available_pages[-1]
            for page_num in range(2, int(last_page)):
                amazon_search_url = f'https://www.amazon.com/s?k={keyword}&page={page_num}'
                yield scrapy.Request(url=amazon_search_url, callback=self.discover_product_urls, meta={'keyword': keyword, 'page': page_num})

    def parse_product_data(self, response):
        asin = response.url.split("/")[4].replace("?th=1", "") if self.mode == "audit" else response.url.split("/")[5]
        name = response.css("#productTitle::text").get("").strip()
        price = response.css('.a-price span[aria-hidden="true"] ::text').get("")
        img = response.css('#ivLargeImage > img::attr(src)').get("")
        if not price:
            price = response.css('.a-price .a-offscreen ::text').get("")


        yield {
            "name": name,
            "price": price,
            "ASIN": asin
        }

        if self.mode != "audit":
            mydb = mysql.connector.connect(
                host="localhost",
                user="root",
                password="password",
                database="flipper"
            )

            mycursor = mydb.cursor()

            sql = "INSERT INTO awslistings (asin, price, name) VALUES (%s, %s, %s)"
            val = (asin, price, name)
            mycursor.execute(sql, val)
            mydb.commit()
            mydb.close()

        else:
            mydb = mysql.connector.connect(
                host="localhost",
                user="root",
                password="password",
                database="flipper"
            )

            mycursor = mydb.cursor()

            mycursor.execute("SELECT asin FROM awslistings WHERE asin='{}'".format(asin))

            asinFromDB = mycursor.fetchone()
            if asinFromDB == None:
                print("AHHHHHHHHHHHHHHHHHHH {} is not found".format(asin))
                return
            
            mycursor.execute("SELECT price FROM awslistings WHERE asin='{}'".format(asin))

            priceFromDB = mycursor.fetchone()
            if priceFromDB == None:
                print("AHHHHHHHHHHHHHHHHHHH {} is not found".format(asin))
                return

            print(price)

            if float(priceFromDB[0][1:].replace(",", "")) > float(price[1:].replace(",", "")):
                print("\n\n\n\n\n\n\n {} : {} \n\n\n\n\n\n\n".format(price, priceFromDB[0]))
                mes = [name, str(float(priceFromDB[0][1:].replace(",", "")) - float(price[1:].replace(",", ""))), response.url, img]
                self.sendMessageToDiscord(mes)

            
            mydb.close()
            
            
    #"http://www.amazon.com/dp/B07XKXQL79"
    def getUrlList(self):
        mydb = mysql.connector.connect(
            host="localhost",
            user="root",
            password="password",
            database="flipper"
        )

        mycursor = mydb.cursor()

        mycursor.execute("SELECT asin FROM awslistings")

        myresult = mycursor.fetchall()
        mydb.close()
        finalresult = []
        for x in myresult:
            finalresult.append("http://www.amazon.com/dp/{}".format(x[0]))

        return finalresult


    def getTestUrl(self):
        return ["https://www.amazon.com/dp/B0BSB1T1NW?th=1"]


    def sendMessageToDiscord(self, message):
        
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

        # The API endpoint to communicate with
        url_post = "https://discord.com/api/webhooks/1109646951793840158/6GdjC3ME3yvDJh8k1ou4EFHRJMwZox_She0bWxPjLyXrKEgJSd0W4yldHmkWyvNY_dPN"

        # A POST request to tthe API
        post_response = requests.post(url_post, json=new_data)

        