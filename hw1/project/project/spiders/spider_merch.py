import scrapy

from ..items import MerchantpointItem


class SpiderMerchSpider(scrapy.Spider):
    name = "spider_merch"
    allowed_domains = ["merchantpoint.ru"]
    start_urls = ["https://merchantpoint.ru"]
    custom_settings = {
        "CLOSESPIDER_ITEMCOUNT": 1000
    }

    def parse(self, response):
        brands_url = response.xpath('//a[contains(@href, "/brands")]/@href').get()
        if brands_url:
            yield scrapy.Request(url=response.urljoin(brands_url), callback=self.parse_brands)

    def parse_brands(self, response):
        for brand_url in response.xpath('//a[contains(@href, "brand/")]/@href').getall():
            yield scrapy.Request(url=response.urljoin(brand_url), callback=self.parse_point_of_sale)
        next_page = response.xpath("//a[contains(text(),'Вперед')]/@href").get()
        if next_page:
            yield scrapy.Request(url=response.urljoin(next_page), callback=self.parse_brands)

    def parse_point_of_sale(self, response):
        all_points = response.xpath("//div[@class='table-responsive']//a[contains(@href,'merchant/')]/@href").getall()
        meta = {}
        meta["org_name"] = response.xpath("//h1[@class='h2']/text()").get().strip()
        meta["org_description"] = response.xpath(
            "//div[@class='col-lg-10 mt-3']//div[@class='form-group mb-2']/p[2]/text()").get().strip()
        if all_points:
            for point_url in all_points:
                yield scrapy.Request(url=response.urljoin(point_url), callback=self.parse_one_point, meta=meta)

    def parse_one_point(self, response):
        item = MerchantpointItem()
        merchant_name = response.xpath("//b[contains(text(),'MerchantName')]/following-sibling::text()[1]").get()
        item['merchant_name'] = str(merchant_name).replace("—", "").strip()
        item['mcc'] = response.xpath("//b[contains(text(),'MCC')]/following-sibling::a/text()").get()
        address = response.xpath("//b[contains(text(),'Адрес торговой точки')]/following-sibling::text()[1]").get()
        if address:
            item['address'] = str(address).replace("—", "").strip()
        geo_coordinates = response.xpath("//b[contains(text(),'Геокоординаты: ')]/following-sibling::text()[1]").get()
        if geo_coordinates:
            item['geo_coordinates'] = geo_coordinates
        item['source_url'] = response.url
        item['org_name'] = response.meta.get("org_name")
        item['org_description'] = response.meta.get("org_description")
        return item
