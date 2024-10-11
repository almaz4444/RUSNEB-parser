import asyncio
import os
import sys
import configparser
import aiohttp
import logging

from aiofile import async_open
import aiohttp.client_exceptions
from tqdm import tqdm
from aiohttp.client_exceptions import ClientOSError, ClientPayloadError


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
}

books_files_urls_file_path = "./books_files_urls.txt"
images_path = "./images/"
books_path = "./books/"

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
        
async def get_response(url, file_path):
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 404:
                        break
                    if response.status != 200:
                        print(response.status)
                        continue
                    
                    if pbar and "ошибка сети" in pbar.desc:
                        pbar.desc = pbar.desc.removesuffix(" (ошибка сети)")
                        
                    data_to_read = True
                    
                    with open(file_path, "wb") as file:
                        while data_to_read:
                            data = bytearray()
                            red = 0
                            while red < chunk_size:
                                chunk = await response.content.read(chunk_size - red)
                                if not chunk:
                                    data_to_read = False
                                    break
                                data.extend(chunk)
                                red += len(chunk)
                            file.write(data)
            break
        except (ClientOSError, ClientPayloadError, asyncio.TimeoutError, aiohttp.ServerDisconnectedError):
            if pbar:
                if "ошибка сети" not in pbar.desc:
                    pbar.desc = f"{pbar.desc} (ошибка сети)"
            else:
                logger.error("Ошибка сети")
            pbar.refresh()
        except Exception as e:
            logger.error(f"Ошибка ({url}) {e}", exc_info=True)
        await asyncio.sleep(0)
            
async def download_file(row):
    try:
        if len(row.split("<sep>")) == 2:
            file_name, url = row.split("<sep>")
            file_path = f"{books_path if 'getFiles' in url else images_path}{file_name}"
            if not os.path.exists(file_path) or True:
                await get_response(url, file_path)
    except KeyboardInterrupt:
        pass
    
async def main():
    global pbar
    
    max_tasks_count = -1
    
    if len(sys.argv) == 2:
        if sys.argv[1].isnumeric():
            max_tasks_count = int(sys.argv[1])
    
    if not os.path.exists(images_path):
        os.mkdir(images_path)
    if not os.path.exists(books_path):
        os.mkdir(books_path)
    if not os.path.exists(books_files_urls_file_path):
        open(books_files_urls_file_path, "w", encoding="utf-8").close()
    
    with tqdm(total=0, desc=f"Скачивание файлов") as pbar:
        tasks = set()
        old_row_index = -1
        while True:
            try:
                with open(books_files_urls_file_path, encoding="utf-8") as file:
                    for index, row in enumerate(file):
                        if index > old_row_index and "Пустая книга" not in row and (max_tasks_count == -1 or len(tasks) < max_tasks_count):
                            tasks.add(asyncio.create_task(download_file(row)))
                            old_row_index = index
                            pbar.total = index + 1
                for task in tasks:
                    if task.done():
                        tasks.remove(task)
                        pbar.update(1)
                        break
                pbar.refresh()
                await asyncio.sleep(0)
            except UnicodeDecodeError as e:
                logger.error(e, exc_info=True)
            
if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(e, exc_info=True)