from concurrent.futures import ThreadPoolExecutor
from bs4 import BeautifulSoup as bs
import eve_ref_helpers as er
import os
from queue import Queue
q: Queue[tuple[str, bool]] = Queue()
d_q = Queue()


def work(url: str):
    soup = er.get_soup(url)
    directories = er.get_directories(soup)
    for link in directories or []:
        # executor.submit(work, link, executor)
        q.put((link, True))
    files = er.get_files(soup)

    for link in files or []:
        # er.download_files_to_database(link, os.path.join(
        #     '/home/chrisdemahy/projects/eve_pie/notebooks/data'))
        q.put((link, False))


def main():
    soup: bs = er.get_soup(url='https://data.everef.net/killmails/')
    directories_links = er.get_directories(soup)
    if directories_links:
        for link in directories_links:
            q.put((link, True))
    else:
        raise Exception('No directories found on initial page.')
    with ThreadPoolExecutor(max_workers=100) as executor:
        # for link in directories_links or []:
        #     executor.submit(work, link, executor)
        while True:
            try:
                link, is_work = q.get(block=True, timeout=20)
            except Exception as e:
                executor.shutdown(wait=True)
                break
            if is_work:
                executor.submit(work, link)
            else:
                executor.submit(er.download_files_to_database, link)


if __name__ == "__main__":
    main()
