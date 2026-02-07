import requests
from lxml import html
from typing import List, Dict, Any, Tuple


class VendrScraper:
    """Клас для витягування даних з HTML."""
    BASE_URL = "https://www.vendr.com"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36..."
        })

    def get_subcategories(self, url: str) -> List[Tuple[str, str]]:
        try:
            resp = self.session.get(url, timeout=10)
            tree = html.fromstring(resp.content)
            sub_links = tree.xpath('//a[contains(@href, "/categories/")]')
            return [(self.BASE_URL + l.get('href'), l.text_content().strip())
                    for l in sub_links if l.get('href').count('/') > 2]
        except:
            return []

    def get_product_links(self, url: str) -> List[str]:
        try:
            resp = self.session.get(url, timeout=10)
            tree = html.fromstring(resp.content)
            links = tree.xpath('//a[contains(@href, "/marketplace/")]/@href')
            return [self.BASE_URL + l if l.startswith('/') else l for l in links if '?' not in l]
        except:
            return []

    def parse_product(self, url: str, cat: str, subcat: str) -> Dict[str, Any]:
        try:
            resp = self.session.get(url, timeout=10)
            tree = html.fromstring(resp.content)

            desc_nodes = tree.xpath(
                '//div[contains(@class, "read-more-box")]//p//text() | //div[contains(@class, "read-more-box")]//text()')
            median = tree.xpath(
                '//span[contains(text(), "Median buyer pays")]/following-sibling::div//span[contains(text(), "$")]/text()')
            slider = tree.xpath('//div[contains(@class, "_rangeSlider_")]//span[contains(text(), "$")]/text()')

            return {
                "name": (tree.xpath('//h1//text()') or ["N/A"])[0].strip(),
                "category": cat, "subcategory": subcat, "url": url,
                "description": " ".join([t.strip() for t in desc_nodes if t.strip()]) or "N/A",
                "pricing": {
                    "median": median[0] if median else "N/A",
                    "low": slider[0] if len(slider) > 0 else "N/A",
                    "high": slider[-1] if len(slider) > 1 else "N/A"
                }
            }
        except:
            return None