import scrapy
from datetime import datetime
from scrapy_selenium import SeleniumRequest


class WimoveisSpider(scrapy.Spider):
    name = 'wimoveis'
    start_url = 'https://www.wimoveis.com.br/imoveis-aluguel-aparecida-de-goiania-go.html'

    def start_requests(self):
        self.page = 1

        yield SeleniumRequest(
            url=self.start_url,
            wait_time=10,
            callback=self.parse,
        )

    def parse(self, response):
        if response.request.url == response.url:
            offers = response.selector.xpath(
                '//div[contains(@class,"list-card-container")]/div[@data-id]'
            )
            for offer in offers:
                now = datetime.now().isoformat()

                item = {}
                item['source_id'] = offer.xpath(
                    '@data-id'
                ).extract_first()
                item['price'] = offer.xpath(
                    './/span[@class="firstPrice"]/@data-price'
                ).extract_first()
                item['location'] = offer.xpath(
                    'normalize-space(.//span[contains(@class,"postingCardLocationTitle")]//text())'
                ).extract_first()
                item['url'] = response.urljoin(
                    offer.xpath(
                        './/a[@class="go-to-posting"]/@href'
                    ).extract_first()
                )
                item['first_seen'] = now
                item['last_updated'] = now

                yield item

            yield SeleniumRequest(
                url=self.get_next_page(),
                wait_time=10,
                callback=self.parse,
            )
        else:
            pass

    def get_next_page(self):
        self.page = self.page + 1
        paginate = '-pagina-{0}.html'.format(self.page)
        next_page = self.start_url.replace('.html', paginate)
        return next_page
