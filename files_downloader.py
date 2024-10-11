import os
import sys
import configparser
import requests
import logging
import shutil

from tqdm import tqdm


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
        
def get_response(url, file_path):
    while True:
        try:
            with requests.get(url, headers=headers, stream=True) as response:
                if response.status_code == 404:
                    break
                if response.status_code != 200:
                    print(response.status_code, url)
                    continue
                
                if pbar and "ошибка сети" in pbar.desc:
                    pbar.desc = pbar.desc.removesuffix(" (ошибка сети)")
                
                with open(file_path, "wb") as file:
                    # file.write(response.content)
                    # return
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        pbar.refresh()
                        if chunk:
                            file.write(chunk)
                            print("YES")
                        else:
                            print(" ELSE")
        # except (ClientOSError, ClientPayloadError, asyncio.TimeoutError, aiohttp.ServerDisconnectedError):
        #     if pbar:
        #         if "ошибка сети" not in pbar.desc:
        #             pbar.desc = f"{pbar.desc} (ошибка сети)"
        #     else:
        #         logger.error("Ошибка сети")
        #     pbar.update(0)
        except Exception as e:
            logger.error(f"Ошибка ({url}) {e}", exc_info=True)
            
def download_file(row):
    try:
        if len(row.split("<sep>")) == 2:
            file_name, url = row.split("<sep>")
            file_path = f"{books_path if 'getFiles' in url else images_path}{file_name}"
            if not os.path.exists(file_path):
                get_response(url.removesuffix("\n"), file_path)
    except KeyboardInterrupt:
        quit()
    
def main():
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
        old_row_index = 0
        while True:
            try:
                with open(books_files_urls_file_path, encoding="utf-8") as file:
                    file_rows = file.readlines()
                    pbar.total = len(file_rows) - 1
                    pbar.refresh()
                    for index, row in enumerate(file_rows):
                        if index > old_row_index and "Пустая книга" not in row:
                            old_row_index = index
                            download_file(row)
                            pbar.update(1)
                pbar.refresh()
            except UnicodeDecodeError as e:
                logger.error(e, exc_info=True)
            
if __name__ == "__main__":
    while True:
        try:
            main()
        except KeyboardInterrupt:
            break
        except Exception as e:
            logger.error(e, exc_info=True)