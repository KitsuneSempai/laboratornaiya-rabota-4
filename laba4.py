import asyncio
import aiohttp
import json
import re
import logging
import random
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')


class WebCrawler:
    def __init__(self, start_url, max_depth=2, max_pages=10):
        self.start_url = start_url
        self.base_domain = urlparse(start_url).netloc
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.visited = set()
        self.results = []
        self.queue = None

    def clean_text(self, html):
        soup = BeautifulSoup(html, 'html.parser')
        for s in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
            s.decompose()
        return soup.get_text(separator=' ', strip=True)
    def analyze(self, text):
        words = re.findall(r'[a-zA-Zа-яА-ЯёЁ]{5,}', text.lower())
        return dict(Counter(words).most_common(10))
    async def fetch(self, session, url):
        await asyncio.sleep(random.uniform(1.0, 3.0))
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
            'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.google.com/',
            'DNT': '1',
        }

        try:
            async with session.get(url, headers=headers, timeout=15) as response:
                if response.status == 200:
                    return await response.text()
                logging.warning(f"Статус {response.status} для {url}")
        except Exception as e:
            logging.error(f"Ошибка запроса: {e}")
        return None
    async def worker(self, session):
        while True:
            url, depth = await self.queue.get()
            if url in self.visited or len(self.visited) >= self.max_pages:
                self.queue.task_done()
                continue
            self.visited.add(url)
            logging.info(f"Обработка ({len(self.visited)}/{self.max_pages}): {url}")
            html = await self.fetch(session, url)
            if html:
                text = self.clean_text(html)
                self.results.append({
                    "url": url,
                    "depth": depth,
                    "analysis": self.analyze(text)
                })
                if depth < self.max_depth:
                    soup = BeautifulSoup(html, 'html.parser')
                    for link in soup.find_all('a', href=True):
                        full_url = urljoin(url, link['href']).split('#')[0]
                        if urlparse(full_url).netloc == self.base_domain:
                            if full_url not in self.visited:
                                await self.queue.put((full_url, depth + 1))

            self.queue.task_done()

    async def run(self):
        self.queue = asyncio.Queue()
        await self.queue.put((self.start_url, 0))
        async with aiohttp.ClientSession() as session:
            worker_task = asyncio.create_task(self.worker(session))
            await self.queue.join()
            worker_task.cancel()

    def save(self):
        with open("results.json", "w", encoding="utf-8") as f:
            json.dump(self.results, f, ensure_ascii=False, indent=4)
        print(f"\nДанные сохранены в results.json. Всего страниц: {len(self.results)}")


if __name__ == "__main__":
    TEST_URL = "https://docs.python.org/3/tutorial/introduction.html"
    crawler = WebCrawler(start_url=TEST_URL, max_depth=1, max_pages=5)
    try:
        asyncio.run(crawler.run())
    finally:
        crawler.save()

    #комментарий: данный код не работает с wikipedia.org, основная ошибка которая не была решена  даже с #
    # использованием user-agents: 403 #
    