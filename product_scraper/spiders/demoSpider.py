import json
import scrapy
from urllib.parse import urljoin
import re
import mysql.connector



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
        price = response.css('.a-offscreen ::text').get("")
        asin = "N/A" if self.mode == "audit" else response.url.split("/")[5]
        name = response.css("#productTitle::text").get("").strip()
        
        if not price:
            price = response.css('.a-price span[aria-hidden="true"] ::text').get("")

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

            
 
    def getUrlList(self):
        return ["http://www.amazon.com/dp/B07XKXQL79", "http://www.amazon.com/dp/B0B647KQFK", "http://www.amazon.com/dp/B009SB0RJG", "http://www.amazon.com/dp/B07SY773GF"]