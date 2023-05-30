from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
import eve_ref_helpers as er
import os
from queue import Queue
directory_queue: Queue[tuple[str, bool]] = Queue()
file_queue = Queue()

temp_directory_name = os.path.join(os.path.pardir, 'data')


def work(url: str):
    soup = er.get_soup(url)
    directories = er.get_directories(soup)
    for link in directories or []:
        # executor.submit(work, link, executor)
        directory_queue.put((link, True))
    files = er.get_files(soup)

    for link in files or []:
        # er.download_files_to_database(link, os.path.join(
        #     '/home/chrisdemahy/projects/eve_pie/notebooks/data'))
        directory_queue.put((link, False))


def main():
    soup: bs = er.get_soup(url='https://data.everef.net/killmails/')
    directories_links = er.get_directories(soup)
    if directories_links:
        for link in directories_links:
            directory_queue.put((link, True))
    else:
        raise Exception('No directories found on initial page.')
    with ThreadPoolExecutor(max_workers=10) as executor:
        # for link in directories_links or []:
        #     executor.submit(work, link, executor)
        while True:
            # print(f'Approximate queue length {directory_queue.qsize()}')
            try:
                link, is_work = directory_queue.get(block=True, timeout=20)
            except Exception as e:
                executor.shutdown(wait=True)
                break
            if is_work:
                executor.submit(work, link)
            else:
                executor.submit(er.download_files_to_database, link)
        # while True:
        #     print(f'Approximate file queue length {directory_queue.qsize()}')


if __name__ == "__main__":
    main()
