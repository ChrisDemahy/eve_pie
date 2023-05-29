import asyncio
from concurrent.futures import ThreadPoolExecutor
import requests
import cchardet
from pprint import pprint
from bs4 import BeautifulSoup as bs
import eve_ref_helpers as er
from queue import Queue
import time
# Get the EVE Reference Data Homepage

directory_queue = Queue()
file_queue = Queue()


def main():
    soup: bs = er.get_soup(url='https://data.everef.net/killmails/')
    er.queue_directories(soup, directory_queue)
    pprint(directory_queue)
    print()
    with ThreadPoolExecutor(max_workers=50) as executor:
        try:
            while True:
                url = directory_queue.get(block=True, timeout=5)
                executor.submit(er.get_directories_and_files,
                                url, directory_queue, file_queue)
        except Exception as e:
            print(e)
            print('Directory Queue is empty')
            while file_queue.qsize() > 0:
                print(f"files left in file queue{file_queue.get()}")
                time.sleep(1)
        # list_of_urls_with_executor = list(
        #     map(lambda url: {'url': url, 'executor': executor}, directories))
        # test = list(executor.map(er.get_directories_and_files,
        #             list_of_urls_with_executor))
        # array = []
        # for x in test:
        #     array.extend(x)
        # pprint(array)

    # if directory_tasks:
    #     test = await asyncio.gather(*directory_tasks)
    #     array = []
    #     for x in test:
    #         array.extend(x)
    #     pprint(array)


if __name__ == "__main__":
    main()
