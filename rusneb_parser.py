import asyncio
import os
import re
import sys
import configparser
from urllib.parse import quote_plus
import aiohttp
import logging
from time import time

from aiofile import async_open
from fake_useragent import UserAgent
import aiohttp.client_exceptions
from tqdm import tqdm
from bs4 import BeautifulSoup
from aiohttp.client_exceptions import ClientOSError, ClientPayloadError

main_url = "https://rusneb.ru/"
collections_url = "https://rusneb.ru/collections/?page=page-{page}"
catalogs_urls_template = [
    "https://rusneb.ru/search/?by=document_publishyearsort&q_author=&q_name=&text=&c%5B%5D=3&access={access}&publishyear_prev=900&publishyear_next=+2024&publisher=&publishplace=&lang=&idlibrary=&isbn=&bbkindex=&bbksection=&udk=&declarant=&patentholder=&search=Y",
    "https://rusneb.ru/search/?by=document_publishyearsort&q_author=&q_name=&text=&c%5B%5D=5&access={access}&publishyear_prev=900&publishyear_next=+2024&publisher=&publishplace=&lang=&idlibrary=&isbn=&bbkindex=&bbksection=&udk=&declarant=&patentholder=&search=Y",
    "https://rusneb.ru/search/?by=document_publishyearsort&q_author=&q_name=&text=&c%5B%5D=23&access={access}&publishyear_prev=900&publishyear_next=+2024&publisher=&publishplace=&lang=&idlibrary=&isbn=&bbkindex=&bbksection=&udk=&declarant=&patentholder=&search=Y",
    "https://rusneb.ru/search/?by=document_publishyearsort&q_author=&q_name=&text=&c%5B%5D=20&access={access}&publishyear_prev=900&publishyear_next=+2024&publisher=&publishplace=&lang=&idlibrary=&isbn=&bbkindex=&bbksection=&udk=&declarant=&patentholder=&search=Y",
    "https://rusneb.ru/search/?by=document_publishyearsort&q_author=&q_name=&text=&c%5B%5D=2&access={access}&publishyear_prev=900&publishyear_next=+2024&publisher=&publishplace=&lang=&idlibrary=&isbn=&bbkindex=&bbksection=&udk=&declarant=&patentholder=&search=Y",
    "https://rusneb.ru/search/?by=document_publishyearsort&q_author=&q_name=&text=&c%5B%5D=4&access={access}&publishyear_prev=900&publishyear_next=+2024&publisher=&publishplace=&lang=&idlibrary=&isbn=&bbkindex=&bbksection=&udk=&declarant=&patentholder=&search=Y",
    "https://rusneb.ru/search/?by=document_publishyearsort&q_author=&q_name=&text=&c%5B%5D=25&access={access}&publishyear_prev=900&publishyear_next=+2024&publisher=&publishplace=&lang=&idlibrary=&isbn=&bbkindex=&bbksection=&udk=&declarant=&patentholder=&search=Y",
]
books_access_variants = ["open", "closed", "seal"]
additional_url_template = "https://rusneb.ru/search/?q={search}&access={access}"

csv_titles = ["АРТИКУЛ", "СЕРИЯ", "ИЗДАТЕЛЬСТВО", "ISBN", "КОЛИЧЕСТВО СТРАНИЦ", "АВТОР", "НАЗВАНИЕ", "ГОД", "КАТЕГОРИЯ", "МЕСТО ИЗДАНИЯ", "НАЗВАНИЕ ФАЙЛА КНИГИ"]
titles = ["АРТИКУЛ", "серия", "издательство", "isbn", "объем", "автор", "заглавие", "год издания", "коллекции", "место издания", "НАЗВАНИЕ ФАЙЛА КНИГИ"]

ua_ = UserAgent()

csv_file_path = "./parsed_datas.csv"
config_file_path = "./parser.temp"
books_files_urls_file_path = "./books_files_urls.txt"
images_path = "./images/"
books_path = "./books/"

additional_search_file_path = "./search.txt"

chunk_size = 5 * 2**20

config = configparser.ConfigParser()
pbar = None

def get_logger(name=__file__, file='log.txt', encoding='utf-8'):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)

    formatter = logging.Formatter('[%(asctime)s] %(lineno)d %(message)s')
        
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.DEBUG)
    stdout_handler.setFormatter(formatter)

    fh = logging.FileHandler(file, encoding=encoding)
    fh.setFormatter(formatter)
    
    log.addHandler(fh)
    log.addHandler(stdout_handler)

    return log

logger = get_logger()

def clear_file_name(docname,
                slash_replace='-',
                quote_replace='',
                multispaces_replace='\x20',
                quotes="""“”«»'\""""
                ):
    
    docname = re.sub(r'[' + quotes + ']', quote_replace, docname)
    docname = re.sub(r'[/]', slash_replace, docname)
    docname = re.sub(r'[|*?<>:\\\n\r\t\v]', '', docname) 
    docname = re.sub(r'\s{2,}', multispaces_replace, docname)
    docname = docname.strip()
    docname = docname.rstrip('-')
    docname = docname.rstrip('.')
    docname = docname.strip()
    return docname

def find_elem(soup, tag, class_, var="") -> str:
    if elem := soup.find(tag, class_=class_):
        if var:
            return elem.get(var, "")
        return elem.text
    return ""

def save_to_csv(infos, file_path):
    while True:
        try:
            with open(file_path, "a", encoding="utf-8") as file:
                info_arr = ["<sep>".join(info) for info in infos]
                file.write("\n".join(info_arr) + "\n")
                return
        except PermissionError:
            pass
        
def create_csv_file(file_path):
    while True:
        try:
            with open(file_path, "w", encoding="utf-8") as file:
                lables_str = "<sep>".join(filter(bool, map(lambda title: title, csv_titles)))
                file.write(lables_str + "\n")
            return
        except PermissionError:
            pass

def save_config():
    while True:
        try:
            with open(config_file_path, "w") as config_file:
                config.write(config_file)
            return
        except PermissionError:
            pass
        
async def check_parsed_url(book_id):
    async with async_open(books_files_urls_file_path, encoding="utf-8") as file:
        file_data = await file.read()
        for row in file_data.split("\n"):
            if book_id in row:
                return True
    return False
        
async def get_response(url, **kwargs):
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers={"User-Agent": ua_.random}, **kwargs) as response:
                    if response.status == 404:
                        break
                    if response.status != 200:
                        if response.status != 503 and pbar:
                            pbar.desc = f"{pbar.desc} ({response.status})"
                            pbar.refresh()
                        continue
                    
                    if pbar and "ошибка сети" in pbar.desc:
                        pbar.desc = pbar.desc.removesuffix(" (ошибка сети)")
                        pbar.refresh()
                        
                    return await response.text()
        except (ClientOSError, ClientPayloadError, asyncio.TimeoutError, aiohttp.ServerDisconnectedError):
            if pbar:
                if "ошибка сети" not in pbar.desc:
                    pbar.desc = f"{pbar.desc} (ошибка сети)"
                    pbar.refresh()
            else:
                logger.error("Ошибка сети")
        except Exception as e:
            logger.error(f"Ошибка ({url}) {e}", exc_info=True)
        
async def get_books_url(catalog_url):
    response = await get_response(catalog_url)
    if response:
        soup = BeautifulSoup(response, "lxml")
        
        if soup.find("h1", class_="title title--small title--work"):
            return None, None
        
        books_url = map(lambda elem: main_url[:-1] + elem.get("href", "javascript"), soup.find_all("a", class_="search-list__item_link"))
        books_url_filtered = []
        
        for url in books_url:
            if "collections" in url:
                books_category_urls = await get_books_url(url)
                if books_category_urls[-1]:
                    books_url_filtered += books_category_urls[-1]
            elif ("javascript" not in url) and (not await check_parsed_url(url.split("/")[-2])):
                books_url_filtered.append(url)
                
        title_collection_name = find_elem(soup, "a", class_="search-nav__a active").strip() or \
            find_elem(soup, "input", class_=["fields", "js-fields-reset", "ui-autocomplete-input"], var="value").strip() or\
            find_elem(soup, "h1", class_=["title", "title--work", "title-collection", "title--h2"]).strip()
        
        return title_collection_name, books_url_filtered
    logging.error(f"Не удалось получить каталог {catalog_url}")
    return []

async def add_to_books_files_file(url, file_name):
    while True:
        try:
            with open(books_files_urls_file_path, 'ab') as file:
                file.write(f"{file_name}<sep>{url}\n".encode('utf-8'))
            return
        except PermissionError:
            pass
                
async def get_book_file(soup):
    for book_file_block in soup.find_all("a", class_=["button", "button--full", "button--empty", "button--h52"]):
        if "PDF" in book_file_block.text.upper() or "EPUB" in book_file_block.text.upper():
            file_url = main_url[:-1] + book_file_block.get("href")
            file_name = clear_file_name(file_url.split('name=')[-1].replace("&doc_type=", ".")).split("-", 1)[-1]
            await add_to_books_files_file(file_url, file_name)
                
            return file_name
    else:
        return ""
                
async def get_image(soup):
    if image := (soup.find("div", class_="cards__album") or soup.find("div", class_="cards__album_short")):
        file_url = main_url[:-1] + image.find("img").get("src")
        file_name = clear_file_name(f"{file_url.split('url=')[-1].split('&')[0]}.png")
        await add_to_books_files_file(file_url, file_name)
            
        return file_name
    else:
        return ""
                
async def get_book_info(book_url):
    try:
        response = await get_response(book_url)
        if response:
            soup = BeautifulSoup(response, "lxml")
            
            articul, book_file_name = await asyncio.gather(get_image(soup), get_book_file(soup))
            
            if not articul and not book_file_name:
                await add_to_books_files_file(book_url, "Пустая книга")
            
            info = {
                "АРТИКУЛ": articul,
                "НАЗВАНИЕ ФАЙЛА КНИГИ": book_file_name
            }
            
            for infos_block in soup.find_all("div", class_="cards-section"):
                if "Детальная информация" in find_elem(infos_block, "h2", class_=["title", "title--smalls"]):
                    for info_block in infos_block.find_all("div", class_="cards-table__row"):
                        info_key = find_elem(info_block, "div", class_="cards-table__left").strip().lower()
                        info_value = find_elem(info_block, "div", class_="cards-table__right").strip().replace("\n", "")
                        if info_key in titles:
                            info[info_key] = info_value.strip()
            
            info_sorted = []
            for title in titles:
                info_sorted.append(info.get(title, ""))
            
            pbar.update(1)
                    
            return info_sorted
    except KeyboardInterrupt:
        pass
    
    await add_to_books_files_file(book_url, "Пустая книга")
    return []
    
async def get_books_collections_urls(collections_url):
    response = await get_response(collections_url)
    soup = BeautifulSoup(response, "lxml")
    
    posts = soup.find_all("a", class_="post__elem_item")
    books_posts = list(filter(lambda elem: not elem.find("div", class_="post__elem_photo-count hide"), posts))
    
    books_collections_urls = []
    for book_post in books_posts:
        books_collections_urls.append(main_url[:-1] + book_post.get("href") + "?page=page-{page}")
    
    return books_collections_urls
    
async def get_books_collections_pages_count(collections_url):
    response = await get_response(collections_url)
    soup = BeautifulSoup(response, "lxml")
    
    pages = soup.find_all("a", class_="pagination__a")
    pages_count = pages[-1].text
    
    if pages_count.isnumeric():
        return int(pages_count)
    return 0

async def parse_catalog(catalog_url, index, pages, catalog_attr_name):
    global pbar
    while True:
        start_time = time()
        catalog_name, books_url = await get_books_url(catalog_url.format(page=pages[index]))
        
        if books_url:
            with tqdm(total=len(books_url), desc=f"{catalog_name}; страница {pages[index]}") as pbar:
                get_book_info_tasks = [asyncio.create_task(get_book_info(book_url)) for book_url in books_url]
                infos = await asyncio.gather(*get_book_info_tasks)
                
                save_to_csv(infos, csv_file_path)
                
                sec_to_iter = round((time() - start_time) / len(books_url), 2)
                pbar.postfix = f"{sec_to_iter}s/it"
        else:
            break
            
        pages[index] += 1
        config["runtime"][catalog_attr_name] = ",".join(map(str, pages))
        save_config()

async def main():
    global pbar
        
    if not os.path.exists(images_path):
        os.mkdir(images_path)
    if not os.path.exists(books_path):
        os.mkdir(books_path)
        
    if not os.path.exists(books_files_urls_file_path):
        open(books_files_urls_file_path, 'w', encoding="utf-8").close()
        
    catalogs_urls = []
    
    # for catalog_url in catalogs_urls_template:
    #     for access in books_access_variants:
    #         catalogs_urls.append(catalog_url.format(access=access) + "&PAGEN_1={page}")
        
    # if os.path.exists(additional_search_file_path):
    #     with open(additional_search_file_path, encoding="utf-8") as f:
    #         for search in f.readlines():
    #             for access in books_access_variants:
    #                 catalogs_urls.append(additional_url_template.format(search=quote_plus(search), access=access) + "&PAGEN_1={page}")
        
    if not os.path.exists(config_file_path):
        config["runtime"] = {
            "catalogs_pages": ",".join(["1"] * len(catalogs_urls)),
            "collections_pages": "1",
        }
        
        catalogs_start_pages = [1] * len(catalogs_urls)
        collections_start_pages = [1]
        save_config()
    else:
        config.read(config_file_path)
        catalogs_start_pages = list(map(int, config["runtime"]["catalogs_pages"].split(",")))
        catalogs_start_pages += [1] * (len(catalogs_urls) - len(catalogs_start_pages))
        collections_start_pages = list(map(int, config["runtime"]["collections_pages"].split(",")))
        
    if not os.path.exists(csv_file_path):
        create_csv_file(csv_file_path)

    pages = catalogs_start_pages
                    
    print(f"Всего кталогов: {len(catalogs_urls)}")
    
    # for index, catalog_url in enumerate(catalogs_urls):
    #     await parse_catalog(catalog_url, index, pages, "catalogs_pages")

    pages = collections_start_pages
    books_collections_urls = []
    books_collections_pages = await get_books_collections_pages_count(collections_url.format(page=1000000))
    
    with tqdm(total=books_collections_pages, desc=f"Поиск коллекций") as pbar:
        for page in range(books_collections_pages):
            books_collections_urls_part = await get_books_collections_urls(collections_url.format(page=page))
            if books_collections_urls_part:
                books_collections_urls += books_collections_urls_part
            else:
                break
            
            pbar.desc = f"\rПоиск коллекций (найдено {len(books_collections_urls)})"
            pbar.update(1)
            
            pages[0] = page
            config["runtime"]["collections_pages"] = ",".join(map(str, pages))
            save_config()
    
    pages += [1] * (len(books_collections_urls) + 1 - len(pages))
    config["runtime"]["collections_pages"] = ",".join(map(str, pages))
    save_config()
    
    for index, url in enumerate(books_collections_urls):
        await parse_catalog(url, index + 1, pages, "collections_pages")
    
if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(e, exc_info=True)