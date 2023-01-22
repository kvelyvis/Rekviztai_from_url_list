import scrapy


class Rekvizitai_from_url_listItem(scrapy.Item):
    #url = scrapy.Field()
    company_url = scrapy.Field()
    scrape_time = scrapy.Field()
    title = scrapy.Field()
    bankrupt_reason = scrapy.Field()
    company_id = scrapy.Field()
    address = scrapy.Field()
    employees = scrapy.Field()
    wage_avg = scrapy.Field()
    income = scrapy.Field()
    profit = scrapy.Field()
    account = scrapy.Field()
    sectors = scrapy.Field()
    webpage = scrapy.Field()
    phone_list = scrapy.Field()
