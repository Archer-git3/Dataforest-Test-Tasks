import sys
import time
import threading
from queue import Queue, Empty
from dotenv import load_dotenv
from database import DatabaseManager
from scraper import VendrScraper
import os
load_dotenv()


def db_writer(res_queue: Queue, db: DatabaseManager):
    while True:
        data = res_queue.get()
        if data is None: break
        db.insert_product(data)
        res_queue.task_done()
    res_queue.task_done()


def worker(task_queue: Queue, res_queue: Queue, scraper: VendrScraper):
    while True:
        try:
            url, cat, subcat = task_queue.get(timeout=2)
            data = scraper.parse_product(url, cat, subcat)
            if data: res_queue.put(data)
            task_queue.task_done()
        except Empty:
            break


def main():
    db = DatabaseManager()
    scraper = VendrScraper()
    task_queue, res_queue = Queue(), Queue()

    categories = [
        ("https://www.vendr.com/categories/data-analytics-and-management", "Data Analytics"),
        ("https://www.vendr.com/categories/devops", "DevOps"),
        ("https://www.vendr.com/categories/it-infrastructure", "IT Infrastructure"),
    ]

    print("[*] Фаза 1: Пошук посилань...")
    for m_url, m_name in categories:
        subcats = scraper.get_subcategories(m_url) or [(m_url, "General")]
        for s_url, s_name in subcats:
            for p_link in scraper.get_product_links(s_url):
                task_queue.put((p_link, m_name, s_name))

    total = task_queue.qsize()

    threading.Thread(target=db_writer, args=(res_queue, db), daemon=True).start()
    for _ in range(int(os.getenv("MAX_THREADS", 10))):
        threading.Thread(target=worker, args=(task_queue, res_queue, scraper), daemon=True).start()

    # Прогрес-бар
    while task_queue.unfinished_tasks > 0:
        done = total - task_queue.unfinished_tasks
        sys.stdout.write(f"\r[Прогрес]: {done}/{total} ({(done / total) * 100:.1f}%)")
        sys.stdout.flush()
        time.sleep(1)

    task_queue.join()
    res_queue.put(None)
    db.close()
    print("\n[+] Успішно завершено!")


if __name__ == "__main__":
    main()