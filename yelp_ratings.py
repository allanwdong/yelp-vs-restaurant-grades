import scrapy
import pandas as pd
import os.path

# Import df_clean to generate urllist for scraping
path = '/Users/allandong/ds/metis/metisgh/Project-two-Luther/df_clean.csv'
df_clean = pd.read_csv(path)
urllist = list(df_clean['URL'])
urls = urllist
print('*******TEST: ', urls)

class yelpSpider(scrapy.Spider):
    name ='yelp_ratings'

    custom_settings = {
        "DOWNLOAD_DELAY": 3,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 3,
        "HTTPCACHE_ENABLED": True
        }

    def parse(self, response):
        global urls
        for href in urls:
            yield scrapy.Request(
                url = href,
                callback = self.parse_yelp,
                meta = {'url' : href}
            )


    def parse_yelp(self, response):
        # url is drawn from meta - df list - pre made to include restaurant
        #   name, address, and borough in search terms
        # yelp_rating is the rating in "#stars star rating" form, drawn from
        #   first real yelp link on url page
        # stars returns just "#stars" from yelp_rating

        url = response.request.meta['url']

        yelp_ratings = response.xpath('//a[@class="biz-name js-analytics-click"]/../../following-sibling::div/div/@title').extract()[0]
        stars = yelp_ratings.split()[0]


        yield {
            'url' : url,
            'yelp_ratings' : yelp_ratings
        }
