import os
import time
import sys
import multiprocessing
from abc import ABC, abstractmethod
from queue import Empty
from typing import List, Dict, Any, Optional

import psycopg2
from playwright.sync_api import sync_playwright
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Централізоване керування конфігурацією через змінні оточення."""
    BASE_URL = str(os.getenv("BASE_URL", "https://books.toscrape.com/")).strip()
    CDP_URL = os.getenv("CDP_URL")


    PROCESS_COUNT = int(os.getenv("PROCESS_COUNT"))


    DB_PARAMS = {
        "dbname": str(os.getenv("DB_NAME")).strip(),
        "user": str(os.getenv("DB_USER")).strip(),
        "password": str(os.getenv("DB_PASSWORD")).strip(),
        "host": str(os.getenv("DB_HOST")).strip(),
        "port": str(os.getenv("DB_PORT")).strip(),
        "client_encoding": "utf8"
    }


class DatabaseManager:
    """Низькорівнева взаємодія з PostgreSQL (без ORM)."""

    def __init__(self, db_params: dict):
        self.db_params = db_params
        self.conn = self._connect()
        if self.conn:
            self._initialize_schema()

    def _connect(self):
        try:
            return psycopg2.connect(**self.db_params)
        except Exception as e:
            print(f"\n[!] Помилка підключення до БД: {e}")
            return None

    def _initialize_schema(self):
        """Створення схеми даних (SOLID: відповідальність за структуру)."""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS books_data (
                    id SERIAL PRIMARY KEY,
                    title TEXT, category TEXT, price TEXT, rating TEXT,
                    stock TEXT, description TEXT, upc TEXT UNIQUE,
                    image_url TEXT, url TEXT UNIQUE
                );
            """)
            self.conn.commit()

    def save_book(self, data: Dict[str, Any]):
        if not self.conn: return
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO books_data (title, category, price, rating, stock, description, upc, image_url, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO NOTHING;
                """, (
                    data['title'], data['category'], data['price'], data['rating'],
                    data['stock'], data['description'], data['product_info'].get('UPC'),
                    data['image_url'], data['url']
                ))
                self.conn.commit()
        except Exception:
            self.conn.rollback()

    def close(self):
        if self.conn: self.conn.close()


class BaseScraper(ABC):
    """Абстракція для скрейперів (Open/Closed Principle)."""

    @abstractmethod
    def scrape(self, url: str) -> Any:
        pass


class BookScraper(BaseScraper):
    """Вилучення деталей книги через Playwright Browser Context."""

    def __init__(self, context):
        self.context = context

    def scrape(self, url: str) -> Optional[Dict[str, Any]]:
        page = self.context.new_page()
        try:
            page.goto(url, timeout=60000, wait_until="domcontentloaded")
            main = page.locator(".product_main")

            return {
                "title": main.locator("h1").inner_text(),
                "category": page.locator(".breadcrumb li:nth-child(3) a").inner_text(),
                "price": main.locator(".price_color").first.inner_text(),
                "rating": (main.locator(".star-rating").get_attribute("class") or "").replace("star-rating ", ""),
                "stock": main.locator(".availability").inner_text().strip(),
                "image_url": Config.BASE_URL + page.locator(".item.active img").get_attribute("src").replace("../../",
                                                                                                             ""),
                "description": page.locator("#product_description + p").inner_text() if page.locator(
                    "#product_description").count() > 0 else "N/A",
                "product_info": self._get_table(page),
                "url": url
            }
        except Exception:
            return None
        finally:
            page.close()

    def _get_table(self, page) -> dict:
        info = {}
        rows = page.locator("table.table-striped tr")
        for i in range(rows.count()):
            k = rows.nth(i).locator("th").inner_text()
            v = rows.nth(i).locator("td").inner_text()
            info[k] = v
        return info


class LinkProvider:
    """Клас для збору посилань (Separation of Concerns)."""

    def __init__(self, base_url: str):
        self.base_url = base_url

    def get_total_pages(self) -> int:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(self.base_url)
            pager_text = page.locator("li.current").inner_text()
            total = int(pager_text.strip().split("of")[-1])
            browser.close()
            return total

    def _fetch_links_from_page(self, page_num: int) -> List[str]:
        url = f"{self.base_url}catalogue/page-{page_num}.html"
        links = []
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            try:
                page.goto(url, timeout=30000)
                anchors = page.locator("h3 a")
                for i in range(anchors.count()):
                    href = anchors.nth(i).get_attribute("href")
                    links.append(f"{self.base_url}catalogue/{href}")
            except:
                pass
            finally:
                browser.close()
        return links

    def collect_all_links(self) -> List[str]:
        total = self.get_total_pages()
        print(f"[*] Знайдено сторінок: {total}. Починаємо збір посилань...")
        with multiprocessing.Pool(processes=min(total, 10)) as pool:
            results = pool.map(self._fetch_links_from_page, range(1, total + 1))
        return [link for sublist in results for link in sublist]


def worker_routine(task_q, res_q):
    """Робочий процес: один браузер на весь життєвий цикл воркера (KISS/DRY)."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        # Оптимізація: відключаємо картинки для швидкості
        context.route("**/*.{png,jpg,jpeg,svg}", lambda route: route.abort())
        scraper = BookScraper(context)

        while True:
            try:
                url = task_q.get(timeout=5)
                if url is None: break
                result = scraper.scrape(url)
                if result: res_q.put(result)
            except Empty:
                break
        browser.close()


class ScrapeProcessManager:
    """Керування паралельними процесами та чергами."""

    def __init__(self, urls: List[str], process_count: int):
        self.urls = urls
        self.process_count = process_count
        self.task_q = multiprocessing.Queue()
        self.res_q = multiprocessing.Queue()
        self.procs = []

    def run(self, db_manager: DatabaseManager):
        for url in self.urls: self.task_q.put(url)
        for _ in range(self.process_count): self.task_q.put(None)

        for _ in range(self.process_count):
            p = multiprocessing.Process(target=worker_routine, args=(self.task_q, self.res_q))
            p.start()
            self.procs.append(p)

        processed_count = 0
        total = len(self.urls)

        while processed_count < total:
            try:
                item = self.res_q.get(timeout=1)
                db_manager.save_book(item)
                processed_count += 1
                sys.stdout.write(f"\r[Прогрес]: {processed_count}/{total}")
                sys.stdout.flush()
            except Empty:
                if not any(p.is_alive() for p in self.procs) and self.res_q.empty():
                    break

        for p in self.procs: p.join()


if __name__ == "__main__":
    # 1. Збір посилань
    link_provider = LinkProvider(Config.BASE_URL)
    all_links = link_provider.collect_all_links()
    print(f"\n[+] Отримано {len(all_links)} посилань.")

    # 2. Ініціалізація БД та Менеджера процесів
    db = DatabaseManager(Config.DB_PARAMS)
    process_manager = ScrapeProcessManager(all_links, Config.PROCESS_COUNT)

    try:
        if db.conn:
            process_manager.run(db)
    finally:
        db.close()

    print("\n[+] Роботу завершено. Дані збережено в БД.")