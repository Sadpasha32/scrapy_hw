from typing import Iterable, Any

import scrapy

from ..items import BookItem


class BookSpiderSpider(scrapy.spiders.SitemapSpider):
    name = "book_spider"
    allowed_domains = ["chitai-gorod.ru"]
    sitemap_urls = ["https://www.chitai-gorod.ru/sitemap.xml"]
    custom_settings = {
        "CLOSESPIDER_ITEMCOUNT": 1000,
        "MONGO_URI": "mongodb://admin:admin@localhost:27017",
        "MONGO_DATABASE": "admin",
        "ITEM_PIPELINES": {
            'project.pipelines.MongoPipeline': 300,
        }
    }

    def sitemap_filter(self, entries: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
        for entry in entries:
            if "product" in entry["loc"]:
                yield entry

    def parse(self, response):
        try:
            item = BookItem()
            item["title"] = response.xpath("//h1/text()").get().strip()
            if author := response.xpath("//*[@itemprop='author']/a/text()").getall():
                item["author"] = " ".join(map(str.strip, author))
            if description := response.xpath("//article[contains(@class,'detail-description__text')]/text()").get():
                item["description"] = description.strip()
            if price_amount := response.xpath("//*[@itemprop='price']/@content").get():
                item["price_amount"] = int(price_amount.strip())
            if price_currency := response.xpath("//*[@itemprop='priceCurrency']/@content").get():
                item["price_currency"] = price_currency.strip()
            if rating_value := response.xpath("//*[@itemprop='ratingValue']/@content").get():
                item["rating_value"] = float(rating_value.strip())
            if rating_count := response.xpath("//*[@itemprop='ratingCount']/@content").get():
                item["rating_count"] = int(rating_count.strip())
            if review_count := response.xpath("//*[@itemprop='reviewCount']/@content").get():
                item["rating_count"] = int(review_count.strip())
            item["publication_year"] = int(response.xpath("//*[@itemprop='datePublished']/text()").get().strip())
            item["isbn"] = response.xpath("//*[@itemprop='isbn']/text()").get().strip()
            item["pages_cnt"] = int(response.xpath("//*[@itemprop='numberOfPages']/text()").get().strip())
            if publisher := response.xpath("//*[@itemprop='publisher']/@content").get():
                item["publisher"] = publisher.strip()
            if book_cover := response.xpath("//img[@class='product-info-gallery__poster']/@src").get():
                item["book_cover"] = book_cover
            item["source_url"] = response.url
            return item
        except AttributeError as e:
            self.logger.warning(e)
            return
