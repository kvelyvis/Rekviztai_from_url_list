import scrapy
import logging
from scrapy.loader import ItemLoader
from rekvizitai.items import Rekvizitai_from_url_listItem
from datetime import datetime
import re
import time
import pandas as pd
from scrapy.exceptions import CloseSpider


logger = logging.getLogger(__name__)


class Rekvizitai_from_url_listSpider(scrapy.Spider):
    name = "rekvizitaispider"
    scrape_time: str

    def __init__(self, **kwargs):
        self.scrape_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        self.safe_scrape_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        super().__init__(**kwargs)

    def start_requests(self):
        # df = pd.read_json('/Users/kristijonas/PycharmProjects/rekvizitai_url_fix/url_list_sample_5.json')
        # mask = df['work'].str.contains("todo")
        # urls = df[mask]['url']
        #
        # for url in urls:
        #     yield scrapy.Request(url=url, callback=self.parse_product)

        yield scrapy.Request('https://rekvizitai.vz.lt/miesto-imones/trakai/34/', self.parse)

    def parse(self, response):
        pr_list = response.css("div.firm div.info a::attr(href)").getall()

        for pr in pr_list:
            yield response.follow(pr, self.parse_product)

    def parse_product(self, response):
        logger.info(f"url = {response.url}")

        if 'neiprastas-srautas' in response.url:
            raise CloseSpider(reason='neiprastas-srautas')

        loader = ItemLoader(item=Rekvizitai_from_url_listItem(), response=response)

        loader.add_value("company_url", response.url)
        loader.add_value("scrape_time", self.scrape_time)

        title_list = response.css("div.content.firmView.item")
        if len(title_list) != 1:
            logger.error(f"expected title list length = 1, got length = {len(title_list)}")
            logger.error(f"url = {response.url}")
            return

        title_list_two = title_list.css("div.name.floatLeft")
        if len(title_list_two) != 1:
            logger.error(f"expected title list two length = 1, got length = {len(title_list_two)}")
            logger.error(f"url = {response.url}")
            return

        title = title_list_two.css("h1::text").get().replace("\"", "")
        if not 3 < len(title) < 150:
            logger.error(f"expected title length between 3 and 150, got length = {len(title)}")
            logger.error(f"url = {response.url}")
            return

        loader.add_value("title", title)

        bankrupt_info = response.css("div.bankruptInfo")

        if bankrupt_info:
            bankrupt_reason = bankrupt_info.xpath("text()").get().replace("\n\t\t\t", "").replace("\t\t", "")
        else:
            bankrupt_reason = False

        loader.add_value("bankrupt_reason", bankrupt_reason)

        description_info_list = response.css("div.info tr td::text").getall()
        if not 1 < len(description_info_list) < 150:
            logger.error(f"expected description info list between 1 and 150, got length = {len(description_info_list)}")
            logger.error(f"url = {response.url}")
            return

        for item in description_info_list:
            if "Įmonės kodas" in item:
                company_id_index = description_info_list.index(item)
                company_id = description_info_list[company_id_index + 1]
                if not 4 < len(company_id) < 12:
                    logger.error(f"expected company id length between 4 and 12, got length = {len(company_id)}")
                    logger.error(f"url = {response.url}")
                    return

                loader.add_value("company_id", company_id)

            if "Adresas" in item:
                address_index = description_info_list.index(item)
                address = description_info_list[address_index + 1].replace("\n\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t", "")
                if not 5 < len(address) < 150:
                    logger.error(f"expected address length between 5 and 150, got length = {len(address)}")
                    logger.error(f"url = {response.url}")
                    return

                loader.add_value("address", address)

            if "Atsiskaitomoji sąskaita" in item:
                account_index = description_info_list.index(item)
                account = description_info_list[account_index + 1]
                if not 15 < len(account) < 25:
                    logger.error(f"expected account length between 15 and 25, got length = {len(account)}")
                    logger.error(f"url = {response.url}")
                    return

                loader.add_value("account", account)

            if "Darbuotojai" in item:
                employees_index = description_info_list.index(item)
                employees_messy = description_info_list[employees_index + 1]
                employees_list = [int(s) for s in employees_messy.split() if s.isdigit()]
                if len(employees_list) == 2:
                    employees = str(employees_list[0]) + str(employees_list[1])
                elif len(employees_list) > 2:
                    logger.error(f"expected employees list length = 1, got length = {len(employees_list)}")
                    logger.error(f"url = {response.url}")
                    return
                elif len(employees_list) == 0:
                    employees = False
                else:
                    employees = str(employees_list[0])

                loader.add_value("employees", employees)

            if "Vidutinis atlyginimas" in item:
                wage_avg_index = description_info_list.index(item)
                wage_avg = description_info_list[wage_avg_index + 1].replace("\n\t\t\t\t\t\t\t\t\t", "")
                if not 4 < len(wage_avg) < 120:
                    logger.error(f"expected wage average length between 4 and 120, got length = {len(wage_avg)}")
                    logger.error(f"url = {response.url}")
                    return

                loader.add_value("wage_avg", wage_avg)

            # if "pajamos" in item:
            #     income_index = description_info_list.index(item)
            #     income_messy = description_info_list[income_index + 1]
            #     income_list = [int(s) for s in income_messy.split() if s.isdigit()]
            #     if not 1 <= len(income_list) <= 6:
            #         logger.error(f"expected income list length between 1 and 6, got length = {len(income_list)}")
            #         logger.error(f"url = {response.url}")
            #         return
            #
            #     if len(income_list) == 1:
            #         income = str(income_list[0])
            #         if not 1 <= len(income) < 13:
            #             logger.error(f"expected income length between 1 and 13, got length = {len(income)}")
            #             logger.error(f"url = {response.url}")
            #             return
            #     elif 1 < len(income_list) < 6:
            #         income = "".join(str(s) for s in income_list)
            #         if not 1 <= len(income) < 13:
            #             logger.error(f"expected income length between 1 and 13, got length = {len(income)}")
            #             logger.error(f"url = {response.url}")
            #             return
            #     else:
            #         income = False

            if "pajamos" in item:
                income_index = description_info_list.index(item)
                income_messy = description_info_list[income_index + 1]
                income_list = re.findall('[\d/-]+', income_messy)
                income_list_excluded = []
                years_list = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020",
                              "2021", "2022"]
                for number in income_list:
                    if all(year not in number for year in years_list):
                        income_list_excluded.append(number)
                income = "".join(str(s) for s in income_list_excluded)
                if not 1 <= len(income) < 14:
                    logger.error(f"expected income length between 1 and 14, got length = {len(income)}")
                    logger.error(f"url = {response.url}")
                    return

                loader.add_value("income", income)

            if "Grynasis" in item:
                profit_index = description_info_list.index(item)
                profit_messy = description_info_list[profit_index + 1]
                profit_list = re.findall('[\d/-]+', profit_messy)
                profit_list_excluded = []
                years_list = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018", "2019", "2020",
                              "2021", "2022"]
                for number in profit_list:
                    if all(year not in number for year in years_list):
                        profit_list_excluded.append(number)
                profit = "".join(str(s) for s in profit_list_excluded)
                if not 1 <= len(profit) < 12:
                    logger.error(f"expected profit length between 1 and 12, got length = {len(profit)}")
                    logger.error(f"url = {response.url}")
                    return

                loader.add_value("profit", profit)

        sector_list = response.css("div.content.firmView div.floatLeft.about")
        if len(sector_list) != 1:
            logger.error(f"expected sector list length = 1, got length = {len(sector_list)}")
            logger.error(f"url = {response.url}")
            return

        sector_list_two = sector_list.css("a.withBullet::text").getall()
        if not 1 <= len(sector_list_two) < 30:
            logger.error(f"expected sector list two length between 1 and 30, got length = {len(sector_list_two)}")
            logger.error(f"url = {response.url}")
            return
        else:
            sectors = " | ".join(sector_list_two)
            if not 5 < len(sectors) < 1000:
                logger.error(f"expected sectors str length between 5 and 1000, got length = {len(sectors)}")
                logger.error(f"url = {response.url}")
                return

            loader.add_value("sectors", sectors)

        href_link_list = response.css("div.info tr td a::attr(href)").getall()
        if not 1 <= len(href_link_list) < 30:
            logger.error(f"expected href link list length between 1 and 30, got length = {len(href_link_list)}")
            logger.error(f"url = {response.url}")

        webpage_list = [x for x in href_link_list if "http" in x]

        url_matches = ["imone", "naujienos", "rekvizitai"]
        webpage_list_filtered = [x for x in webpage_list if not any(match in x for match in url_matches)]
        if len(webpage_list_filtered) == 1:
            webpage = webpage_list_filtered[0]
            if not 5 < len(webpage) < 150:
                logger.error(f"expected webpage length between 5 and 100, got length = {len(webpage)}")
                logger.error(f"url = {response.url}")
                return
        elif len(webpage_list_filtered) == 0:
            webpage = False
        elif len(webpage_list_filtered) == 2:
            webpage = webpage_list_filtered[0] + ', ' + webpage_list_filtered[1]
        else:
            logger.error(f"webpage list length > 2, url = {response.url}")
            webpage = False

        loader.add_value("webpage", webpage)

        gif_phone_name_list = response.css("div.info tr td").xpath("./img/@alt").getall()

        gif_phone_list_messy = response.css("div.info tr td img::attr(src)").getall()
        gif_phone_list = [ph for ph in gif_phone_list_messy if ".gif" in ph]

        if len(gif_phone_list) != len(gif_phone_name_list):
            logger.error(f"Number of telephones does not correspond to number of their names/usage")
            logger.error(f"length phones = {len(gif_phone_list)}")
            logger.error(f"length phones names = {len(gif_phone_name_list)}")
            logger.error(f"url = {response.url}")
            return

        if len(gif_phone_list) == 0:
            phone_list = False
        elif len(gif_phone_list) > 20:
            logger.error(f"phone list length more than 20, check it (len = {len(gif_phone_list)}")
            logger.error(f"url = {response.url}")
            return
        else:
            phone_list = [x + ' | ' + y for x, y in zip(gif_phone_list, gif_phone_name_list)]

        loader.add_value("phone_list", phone_list)



        time.sleep(2.7)

        return loader.load_item()
