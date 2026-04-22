import asyncio
import aiohttp
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import json
import re
from collections import Counter
from datetime import datetime
import logging
from typing import Set, Dict, List, Optional
import sys
import random
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)
class WikipediaCrawler:
    """Поисковый робот для сбора и анализа контента Wikipedia"""

    # Список User-Agent для ротации
    USER_AGENTS = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
    ]

    def __init__(self, start_url: str, topic: str, max_depth: int = 2, max_concurrent: int = 2):
        """
        Инициализация краулера

        Args:
            start_url: Начальный URL для обхода
            topic: Тема для анализа (ключевые слова)
            max_depth: Максимальная глубина обхода
            max_concurrent: Максимальное количество одновременных запросов
        """
        self.start_url = start_url
        self.topic = topic.lower()
        self.max_depth = max_depth
        self.max_concurrent = max_concurrent
        self.domain = urlparse(start_url).netloc

        # Множества для хранения обработанных и запланированных URL
        self.visited_urls: Set[str] = set()
        self.queue: asyncio.Queue = asyncio.Queue()

        # Сбор данных для анализа
        self.page_data: Dict[str, Dict] = {}
        self.all_texts: List[str] = []

        # Семафор для ограничения количества одновременных запросов
        self.semaphore = asyncio.Semaphore(max_concurrent)

        # Статистика
        self.stats = {
            'pages_processed': 0,
            'pages_failed': 0,
            'total_links_found': 0,
            'start_time': None,
            'end_time': None
        }

        # Для задержек между запросами
        self.request_delay = 0.5  # Задержка в секундах между запросами

    def get_headers(self) -> dict:
        """Получение заголовков для HTTP запроса"""
        return {
            'User-Agent': random.choice(self.USER_AGENTS),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0',
            'DNT': '1',
            'Referer': 'https://en.wikipedia.org/'
        }

    def is_valid_wikipedia_url(self, url: str) -> bool:
        """
        Проверка, является ли URL допустимой страницей Wikipedia
        """
        parsed = urlparse(url)

        # Проверка домена
        if parsed.netloc != self.domain:
            return False

        # Исключаем специальные страницы Wikipedia
        excluded_patterns = [
            '/wiki/Special:',
            '/wiki/File:',
            '/wiki/Category:',
            '/wiki/Template:',
            '/wiki/Help:',
            '/wiki/Portal:',
            '/wiki/Wikipedia:',
            '/wiki/MediaWiki:',
            'action=edit',
            'printable=yes',
            '#',
            'oldid=',
            'diff=',
            '&printable=',
            'title=Special',
            'curid='
        ]

        for pattern in excluded_patterns:
            if pattern in url:
                return False

        # Проверка, что это основное пространство имен (статьи)
        path = parsed.path
        if path.startswith('/wiki/') and len(path) > 6:
            # Проверяем, что URL не содержит параметров
            if not parsed.query:
                return True

        return False

    def extract_links(self, html: str, current_url: str) -> Set[str]:
        """
        Извлечение внутренних ссылок из HTML
        """
        soup = BeautifulSoup(html, 'html.parser')
        links = set()

        # Ищем ссылки в основном контенте
        main_content = soup.find('div', {'id': 'mw-content-text'})
        if not main_content:
            main_content = soup.find('div', {'class': 'mw-parser-output'})
        if not main_content:
            main_content = soup

        # Поиск всех тегов <a>
        for link in main_content.find_all('a', href=True):
            href = link['href']

            # Пропускаем пустые ссылки и якоря
            if not href or href.startswith('#') or href.startswith('javascript:'):
                continue

            # Пропускаем ссылки на редактирование и служебные
            if 'action=edit' in href or 'redlink=1' in href:
                continue

            # Пропускаем ссылки на файлы и медиа
            if href.startswith('/wiki/File:') or href.startswith('/wiki/Media:'):
                continue

            # Преобразование относительной ссылки в абсолютную
            absolute_url = urljoin('https://en.wikipedia.org', href)

            # Проверка валидности URL для Wikipedia
            if self.is_valid_wikipedia_url(absolute_url):
                links.add(absolute_url)

        return links

    def extract_clean_text(self, html: str) -> str:
        """
        Извлечение чистого текста из HTML
        """
        soup = BeautifulSoup(html, 'html.parser')

        # Удаляем скрипты и стили
        for script in soup(["script", "style", "meta", "link", "noscript", "svg", "canvas"]):
            script.decompose()

        # Получаем основной контент Wikipedia
        content_div = soup.find('div', {'id': 'mw-content-text'})
        if not content_div:
            content_div = soup.find('div', {'class': 'mw-parser-output'})

        if content_div:
            # Удаляем элементы навигации, инфобоксы и таблицы
            for nav in content_div.find_all(['table', 'div'],
                                            class_=['navbox', 'sidebar', 'infobox', 'metadata', 'toc',
                                                    'mw-references-wrap']):
                nav.decompose()

            # Удаляем элементы с маленьким текстом (сноски, примечания)
            for small in content_div.find_all(['small', 'sup', 'sub']):
                small.decompose()

            text = content_div.get_text()
        else:
            text = soup.get_text()

        # Очистка текста
        lines = (line.strip() for line in text.splitlines())
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        text = ' '.join(chunk for chunk in chunks if chunk)

        # Удаление лишних пробелов и специальных символов
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\[\d+\]', '', text)  # Удаление ссылок [1], [2] и т.д.
        text = re.sub(r'\[edit\]', '', text)
        text = re.sub(r'\[citation needed\]', '', text)

        return text.strip()

    def analyze_text(self, text: str, url: str) -> Dict:
        """
        Анализ текста страницы
        """
        # Разбиваем текст на слова (только буквенные, длиной от 3 символов)
        words = re.findall(r'\b[a-zA-Z]{3,}\b', text.lower())

        if not words:
            return {
                'url': url,
                'title': self.extract_title_from_url(url),
                'word_count': 0,
                'char_count': len(text),
                'sentence_count': 0,
                'keyword_occurrences': {},
                'total_keyword_matches': 0,
                'unique_words': 0,
                'top_10_words': {}
            }

        # Ключевые слова по теме
        topic_keywords = [kw.strip() for kw in self.topic.split() if len(kw.strip()) > 2]

        # Подсчет вхождений ключевых слов
        keyword_counts = {}
        total_matches = 0

        for keyword in topic_keywords:
            # Ищем точные вхождения и в составе других слов
            count = sum(1 for word in words if keyword in word or word in keyword)
            keyword_counts[keyword] = count
            total_matches += count

        # Общая статистика
        analysis = {
            'url': url,
            'title': self.extract_title_from_url(url),
            'word_count': len(words),
            'char_count': len(text),
            'sentence_count': len(re.findall(r'[.!?]+', text)),
            'keyword_occurrences': keyword_counts,
            'total_keyword_matches': total_matches,
            'unique_words': len(set(words)),
            'top_10_words': dict(Counter(words).most_common(10))
        }

        return analysis

    def extract_title_from_url(self, url: str) -> str:
        """Извлечение заголовка статьи из URL Wikipedia"""
        match = re.search(r'/wiki/([^#?]+)', url)
        if match:
            title = match.group(1).replace('_', ' ')
            # Декодируем URL-encoded символы
            title = title.replace('%28', '(').replace('%29', ')').replace('%2C', ',')
            return title
        return url

    async def fetch_page(self, session: aiohttp.ClientSession, url: str) -> Optional[str]:
        """
        Загрузка HTML страницы
        """
        # Добавляем задержку между запросами
        await asyncio.sleep(self.request_delay)

        for attempt in range(3):  # 3 попытки
            try:
                headers = self.get_headers()

                async with session.get(url, headers=headers, timeout=15,
                                       allow_redirects=True) as response:

                    if response.status == 200:
                        logger.info(f"✓ Загружена: {self.extract_title_from_url(url)}")
                        return await response.text()
                    elif response.status == 403:
                        logger.warning(f"Попытка {attempt + 1}/3: 403 для {self.extract_title_from_url(url)}")
                        if attempt < 2:
                            await asyncio.sleep(2)  # Ждем перед повторной попыткой
                            continue
                        else:
                            self.stats['pages_failed'] += 1
                            return None
                    else:
                        logger.warning(f"✗ Ошибка {response.status} для {url}")
                        self.stats['pages_failed'] += 1
                        return None

            except asyncio.TimeoutError:
                logger.error(f"Таймаут при загрузке {url} (попытка {attempt + 1}/3)")
                if attempt < 2:
                    await asyncio.sleep(2)
                    continue
                self.stats['pages_failed'] += 1
                return None
            except Exception as e:
                logger.error(f"Ошибка при загрузке {url}: {str(e)}")
                if attempt < 2:
                    await asyncio.sleep(2)
                    continue
                self.stats['pages_failed'] += 1
                return None

        return None

    async def process_page(self, session: aiohttp.ClientSession, url: str, depth: int):
        """
        Обработка одной страницы
        """
        async with self.semaphore:
            if url in self.visited_urls:
                return

            self.visited_urls.add(url)
            title = self.extract_title_from_url(url)
            logger.info(f"Обработка [глубина {depth}]: {title}")

            # Загрузка страницы
            html = await self.fetch_page(session, url)
            if not html:
                return

            # Извлечение чистого текста
            clean_text = self.extract_clean_text(html)
            if clean_text and len(clean_text) > 200:  # Игнорируем слишком короткие страницы
                self.all_texts.append(clean_text)

                # Анализ текста
                analysis = self.analyze_text(clean_text, url)
                self.page_data[url] = analysis

                relevance = "⭐" * min(3, analysis['total_keyword_matches'] // 5 + 1)
                logger.info(f"  → Слов: {analysis['word_count']}, "
                            f"Уникальных: {analysis['unique_words']}, "
                            f"Релевантность: {analysis['total_keyword_matches']} {relevance}")
            else:
                logger.warning(f"  → Страница содержит мало текста ({len(clean_text)} символов)")

            # Если глубина не достигла максимума, извлекаем ссылки
            if depth < self.max_depth:
                links = self.extract_links(html, url)
                new_links = 0
                for link in links:
                    if link not in self.visited_urls:
                        await self.queue.put((link, depth + 1))
                        new_links += 1

                self.stats['total_links_found'] += len(links)
                if new_links > 0:
                    logger.info(f"  → Найдено новых ссылок: {new_links}")

            self.stats['pages_processed'] += 1

            # Вывод прогресса
            if self.stats['pages_processed'] % 5 == 0:
                self.print_progress()

    def print_progress(self):
        """Вывод прогресса обработки"""
        logger.info(f"📊 Прогресс: Обработано {self.stats['pages_processed']} страниц, "
                    f"В очереди: {self.queue.qsize()}, "
                    f"Найдено ссылок: {self.stats['total_links_found']}")

    async def crawl(self):
        """Основной метод обхода веб-сайта"""
        logger.info(f"🚀 Запуск краулера Wikipedia")
        logger.info(f"📌 Стартовый URL: {self.start_url}")
        logger.info(f"🎯 Тема анализа: {self.topic}")
        logger.info(f"📏 Максимальная глубина: {self.max_depth}")
        logger.info(f"⚡ Максимум одновременных запросов: {self.max_concurrent}")
        logger.info("=" * 60)

        self.stats['start_time'] = datetime.now()

        # Добавляем стартовый URL в очередь
        await self.queue.put((self.start_url, 0))

        # Создаем TCP-коннектор
        connector = aiohttp.TCPConnector(limit=self.max_concurrent,
                                         limit_per_host=self.max_concurrent,
                                         ttl_dns_cache=300,
                                         ssl=False)  # Отключаем проверку SSL для скорости

        async with aiohttp.ClientSession(connector=connector) as session:
            workers = []
            # Запускаем обработчиков
            for _ in range(self.max_concurrent):
                worker = asyncio.create_task(self.worker(session))
                workers.append(worker)

            # Ждем завершения всех обработчиков
            await self.queue.join()

            # Отменяем задачи после завершения
            for worker in workers:
                worker.cancel()

            await asyncio.gather(*workers, return_exceptions=True)

        self.stats['end_time'] = datetime.now()
        logger.info("=" * 60)
        logger.info("✅ Обход завершен!")
        self.print_final_stats()

    async def worker(self, session: aiohttp.ClientSession):
        """
        Worker для обработки страниц из очереди
        """
        while True:
            try:
                url, depth = await self.queue.get()
                await self.process_page(session, url, depth)
                self.queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Ошибка в worker: {str(e)}")
                self.queue.task_done()

    def print_final_stats(self):
        """Вывод итоговой статистики"""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()

        logger.info("📊 ИТОГОВАЯ СТАТИСТИКА:")
        logger.info(f"  • Обработано страниц: {self.stats['pages_processed']}")
        logger.info(f"  • Страниц с ошибками: {self.stats['pages_failed']}")
        logger.info(f"  • Уникальных URL: {len(self.visited_urls)}")
        logger.info(f"  • Всего найдено ссылок: {self.stats['total_links_found']}")
        logger.info(f"  • Время выполнения: {duration:.2f} секунд")

        if self.all_texts:
            all_text = ' '.join(self.all_texts)
            all_words = re.findall(r'\b[a-zA-Z]{3,}\b', all_text.lower())
            logger.info(f"  • Всего слов собрано: {len(all_words)}")
            logger.info(f"  • Уникальных слов: {len(set(all_words))}")

    def save_results(self, filename: str = "crawler_results.json"):
        """
        Сохранение результатов анализа в JSON файл
        """
        # Создаем сериализуемую версию данных
        serializable_data = {}
        for url, data in self.page_data.items():
            serializable_data[url] = {
                'url': data['url'],
                'title': data['title'],
                'word_count': data['word_count'],
                'char_count': data['char_count'],
                'sentence_count': data['sentence_count'],
                'keyword_occurrences': data['keyword_occurrences'],
                'total_keyword_matches': data['total_keyword_matches'],
                'unique_words': data['unique_words'],
                'top_10_words': data['top_10_words']
            }

        results = {
            'metadata': {
                'start_url': self.start_url,
                'topic': self.topic,
                'max_depth': self.max_depth,
                'max_concurrent': self.max_concurrent,
                'start_time': self.stats['start_time'].isoformat() if self.stats['start_time'] else None,
                'end_time': self.stats['end_time'].isoformat() if self.stats['end_time'] else None,
                'duration_seconds': (self.stats['end_time'] - self.stats['start_time']).total_seconds()
                if self.stats['start_time'] and self.stats['end_time'] else 0,
                'pages_processed': self.stats['pages_processed'],
                'pages_failed': self.stats['pages_failed'],
                'unique_pages': len(self.visited_urls)
            },
            'page_analysis': serializable_data,
            'summary': {
                'total_keyword_matches': sum(
                    page['total_keyword_matches'] for page in self.page_data.values()
                ),
                'pages_with_keywords': sum(
                    1 for page in self.page_data.values()
                    if page['total_keyword_matches'] > 0
                ),
                'average_words_per_page': sum(
                    page['word_count'] for page in self.page_data.values()
                ) / len(self.page_data) if self.page_data else 0
            }
        }

        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        logger.info(f"💾 Результаты сохранены в файл: {filename}")

        # Сохраняем текстовый отчет
        if self.page_data:
            self.save_text_report(filename.replace('.json', '_report.txt'))

    def save_text_report(self, filename: str):
        """Сохранение текстового отчета"""
        with open(filename, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("ОТЧЕТ ПО АНАЛИЗУ WIKIPEDIA\n")
            f.write("=" * 80 + "\n\n")

            f.write(f"Тема анализа: {self.topic}\n")
            f.write(f"Стартовый URL: {self.start_url}\n")
            f.write(f"Глубина обхода: {self.max_depth}\n")
            f.write(f"Обработано страниц: {self.stats['pages_processed']}\n\n")

            f.write("=" * 80 + "\n")
            f.write("СТРАНИЦЫ, РЕЛЕВАНТНЫЕ ТЕМЕ:\n")
            f.write("=" * 80 + "\n\n")

            # Сортируем страницы по релевантности
            sorted_pages = sorted(
                self.page_data.values(),
                key=lambda x: x['total_keyword_matches'],
                reverse=True
            )

            for i, page in enumerate(sorted_pages[:20], 1):
                if page['total_keyword_matches'] > 0:
                    f.write(f"{i}. {page['title']}\n")
                    f.write(f"   URL: {page['url']}\n")
                    f.write(f"   Совпадений: {page['total_keyword_matches']}\n")
                    f.write(f"   Всего слов: {page['word_count']}\n")
                    f.write(f"   Уникальных слов: {page['unique_words']}\n")
                    f.write(f"   Топ-5 слов: {', '.join(list(page['top_10_words'].keys())[:5])}\n\n")


async def main():
    """Главная функция"""
    print("\n" + "=" * 60)
    print("🔍 WIKIPEDIA WEB CRAWLER - Лабораторная работа №4")
    print("=" * 60 + "\n")

    # Параметры запуска
    START_URL = "https://en.wikipedia.org/wiki/Artificial_intelligence"
    TOPIC = "artificial intelligence machine learning neural networks deep learning"
    MAX_DEPTH = 2  # Глубина обхода
    MAX_CONCURRENT = 2  # Параллельных запросов

    print(f"Настройки:")
    print(f"  • Стартовая страница: {START_URL}")
    print(f"  • Тема анализа: {TOPIC}")
    print(f"  • Глубина: {MAX_DEPTH}")
    print(f"  • Параллельных запросов: {MAX_CONCURRENT}")
    print("\nНачинаем обход...\n")

    # Создание и запуск краулера
    crawler = WikipediaCrawler(
        start_url=START_URL,
        topic=TOPIC,
        max_depth=MAX_DEPTH,
        max_concurrent=MAX_CONCURRENT
    )

    try:
        await crawler.crawl()
        crawler.save_results("wikipedia_analysis.json")

        # Вывод краткого отчета
        print("\n" + "=" * 60)
        print("📈 КРАТКИЙ ОТЧЕТ ПО АНАЛИЗУ:")
        print(f"Тема: {TOPIC}")
        print(f"Всего проанализировано страниц: {len(crawler.page_data)}")

        # Поиск страниц с наибольшим количеством ключевых слов
        if crawler.page_data:
            top_pages = sorted(
                crawler.page_data.values(),
                key=lambda x: x['total_keyword_matches'],
                reverse=True
            )[:5]

            print("\n🏆 Топ-5 страниц по релевантности теме:")
            for i, page in enumerate(top_pages, 1):
                if page['total_keyword_matches'] > 0:
                    print(f"  {i}. {page['title']}")
                    print(f"     Совпадений: {page['total_keyword_matches']}")
                    print(f"     Слов: {page['word_count']}")
                    print(f"     Уникальных слов: {page['unique_words']}")

        print("\n✅ Результаты сохранены в файлы:")
        print("  • wikipedia_analysis.json - полные данные в JSON")
        print("  • wikipedia_analysis_report.txt - текстовый отчет")
        print("  • crawler.log - лог выполнения")

    except KeyboardInterrupt:
        logger.info("\n⚠️ Программа прервана пользователем")
        if crawler.page_data:
            crawler.save_results("wikipedia_analysis_partial.json")
            print("\nЧастичные результаты сохранены в wikipedia_analysis_partial.json")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Запуск асинхронной главной функции
    asyncio.run(main())