from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup as bs
import eve_ref_helpers as er
import os
from queue import Queue

from cache_helpers import check_cache, read_cache, write_cache

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
        file_queue.put((link, False))


def main(url='https://data.everef.net/killmails/'):
    if not check_cache():
        soup: bs = er.get_soup(url)
        directories_links = er.get_directories(soup)
        if not directories_links:
            raise Exception('Initial page has no directories')
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
        some_links: list[str] = []
        while True:
            try:
                link, is_work = file_queue.get(block=True, timeout=5)
            except:
                break
            some_links.append(link)
        # print(some_links)
        write_cache(some_links)

    print('loading from cache')
    for x in read_cache():
        # print(x)
        file_queue.put((x, False))

    # Second loop
    print('starting second loop')
    killmail_list: list = []
    victim_list: list = []
    attacker_list: list = []
    item_list: list = []
    #

    while True:
        print(f'Approximate file queue length {file_queue.qsize()}')
        try:
            link, is_work = file_queue.get(block=True, timeout=20)
            print(link)
        except Exception as e:
            print(e)
            break
        (killmails, victims, attackers, items) = er.download_files_to_database(link)

        killmail_list.extend(killmails)
        victim_list.extend(victims)
        attacker_list.extend(attackers)
        item_list.extend(items)
        print("\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n\n")
        print(
            f"killmails: {len(killmail_list)}, victims: {len(victim_list)}, attackers: {len(attacker_list)}, items: {len(item_list)}")
    er.load_items_into_database(
        killmail_list, victim_list, attacker_list, item_list)


if __name__ == "__main__":
    main()
