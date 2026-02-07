import psycopg2
import os
from typing import Dict, Any

class DatabaseManager:
    """Відповідає виключно за збереження даних у PostgreSQL."""
    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        self._create_table()

    def _create_table(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    name TEXT,
                    category TEXT,
                    subcategory TEXT,
                    median_price TEXT,
                    low_price TEXT,
                    high_price TEXT,
                    description TEXT,
                    url TEXT UNIQUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            self.conn.commit()

    def insert_product(self, data: Dict[str, Any]):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO products (name, category, subcategory, median_price, low_price, high_price, description, url)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (url) DO UPDATE SET 
                    subcategory = EXCLUDED.subcategory; -- Оновлюємо підкатегорію, якщо знайшли нову
                """, (
                    data['name'], data['category'], data['subcategory'],
                    data['pricing']['median'], data['pricing']['low'],
                    data['pricing']['high'], data['description'], data['url']
                ))
                self.conn.commit()
        except Exception:
            self.conn.rollback()

    def close(self):
        self.conn.close()