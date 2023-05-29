from concurrent.futures import ThreadPoolExecutor
from queue import Queue
import httpx
from bs4 import BeautifulSoup as bs
import asyncio
from typing import Union
import os
import zipfile
import tarfile
import tempfile
import msgspec
from process_killmails import extract_attacker_and_victim_and_items as extract
import pandas as pd
from dataframe_to_database import dataframe_to_database as df_to_db
import psycopg2

# conn_string = 'postgresql://postgres:eve_pie@localhost/eve_pie'
conn_string = 'postgresql://postgres:ZzIc2R5bO49ObfAmr7vX@containers-us-west-182.railway.app:7545/railway'


def get_directories_and_files(url, directory_queue: Queue, file_queue: Queue):
    soup = get_soup(url)
    file_list: list[str] = []
    # Make some soup and get the directories and files from the soup with a ladel
    directory_links = get_directories(soup)
    for link in directory_links or []:
        directory_queue.put(link)
    file_links = get_files(soup)
    for link in file_links or []:
        file_queue.put(link)


def make_request(url: str):
    print('making request')
    with httpx.Client() as client:
        response = client.get(url)
        return response


def get_soup(url: str) -> bs:
    response = make_request(url)
    soup = bs(response.content, 'lxml')
    return soup


def get_directories(soup: bs) -> Union[list[str], None]:
    directories = soup.find_all(class_='data-dir')
    # print(f'Getting directories: {directories}')
    if directories:
        directories_table_entries = list(map(lambda soup: soup.find(
            'a', href=True, class_='url'), directories))
        directory_links = list(map(
            lambda entry: f"https://data.everef.net{entry['href']}", directories_table_entries))
        return directory_links


def get_files(soup: bs) -> Union[list[str], None]:
    files = soup.find_all(class_='data-file-url')
    if files:
        file_links = list(
            map(lambda soup: f"https://data.everef.net{soup['href']}", files))
        # print(f'file links: {file_links}')
        return file_links


def download_files_to_database(url):
    with tempfile.TemporaryDirectory() as directory:
        response = make_request(url)
        filename: str = os.path.basename(url)
        directory_and_file = os.path.join(directory, filename)
        # save the file
        with open(os.path.join(directory, filename), 'wb') as f:
            f.write(response.content)
        # if the file is a zip or tar.bz2 file, extract it
        if filename.endswith('.zip'):
            with zipfile.ZipFile(directory_and_file, 'r') as zip_ref:
                zip_ref.extractall(directory)
        elif filename.endswith('.tar.bz2'):
            with tarfile.open(directory_and_file, 'r:bz2') as tar_ref:
                tar_ref.extractall(directory)
        load_files_into_database(directory)


def load_files_into_database(directory: str):
    killmails: list = []
    victims: list = []
    attackers: list = []
    items: list = []

    for file in os.listdir(directory):
        if not file.endswith('.json'):
            raise Exception('File is not a json file')
        with open(os.path.join(directory, file), 'rb') as f:
            decoded_json = decode_with_msgspec(f.read())
            decoded_killmail = extract(decoded_json)

            killmails.append(decoded_killmail['killmail'])
            if decoded_killmail['attackers']:
                attackers.extend(decoded_killmail['attackers'])
            if decoded_killmail['victims']:
                victims.append(decoded_killmail['victim'])
            if decoded_killmail['items']:
                items.extend(decoded_killmail['items'])
    killmail_dataframe = pd.json_normalize(attackers)
    victim_dataframe = pd.json_normalize(victims)
    attacker_dataframe = pd.json_normalize(attackers)
    item_dataframe = pd.json_normalize(items)
    pg_conn = psycopg2.connect(conn_string)
    df_to_db(killmail_dataframe, 'killmails', pg_conn)
    df_to_db(victim_dataframe, 'victims', pg_conn)
    df_to_db(attacker_dataframe, 'attackers', pg_conn)
    df_to_db(item_dataframe, 'items', pg_conn)
    pg_conn.close()


def decode_with_msgspec(json_data: Union[bytes, str]) -> dict:
    print('decoding json')
    return msgspec.json.decode(json_data)


def encode_with_msgspec(json_data: dict) -> bytes:
    return msgspec.json.encode(json_data)
