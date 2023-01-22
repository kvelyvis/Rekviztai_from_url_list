import json

from itemadapter import ItemAdapter
from scrapy.exporters import JsonLinesItemExporter
from datetime import datetime


class RekvizitaiPipeline:

    def __init__(self, **kwargs):
        self.scrape_time = datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        self.safe_scrape_time = datetime.now().strftime("%Y%m%d_%H%M%S")
        super().__init__(**kwargs)

    file = None

    def open_spider(self, spider):
        self.file = open(
            f"/Users/kristijonas/PycharmProjects/Rekviztai_from_url_list/rekvizitai/output/{self.safe_scrape_time}_item.json",
            "wb",
        )

    def close_spider(self, spider):
        self.file.close()

    def process_item(self, item, spider):
        # line = json.dumps(ItemAdapter(item).asdict(), ensure_ascii=False).encode(
        line = json.dumps(dict(item), ensure_ascii=False).encode("utf-8") + "\n".encode(
            "utf-8"
        )
        self.file.write(line)
        return