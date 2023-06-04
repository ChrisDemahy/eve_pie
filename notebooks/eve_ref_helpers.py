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
from sqlalchemy import create_engine
from process_killmails import extract_attacker_and_victim_and_items as extract
import pandas as pd
from dataframe_to_database import dataframe_to_database as df_to_db
from dataframe_to_database import no_truly_copy_dataframe_to_database as copy_df_to_db
import psycopg2
from dataframe_to_database import conn_string


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
    with httpx.Client() as client:
        response = client.get(url)
        return response


def get_soup(url: str) -> bs:
    response = make_request(url)
    soup = bs(response.content, 'lxml')
    return soup


def get_directories(soup: bs) -> Union[list[str], None]:
    directories = soup.find_all(class_='data-dir')
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

        return file_links


def download_files_to_database(url):
    with tempfile.TemporaryDirectory() as directory:
        response = make_request(url)
        filename: str = os.path.basename(url)
        directory_and_file = os.path.join(directory, filename)
        # save the file
        with open(directory_and_file, 'wb') as f:
            f.write(response.content)
        extract_archive(directory, filename, directory_and_file)

        (killmails, victims, attackers, items) = files_to_items(
            os.path.join(
                directory, 'killmails'))
    return (killmails, victims, attackers, items)


def extract_archive(directory, filename, directory_and_file):
    # if the file is a zip or tar.bz2 file, extract it
    if filename.endswith('.zip'):
        with zipfile.ZipFile(directory_and_file, 'r') as zip_ref:
            zip_ref.extractall(directory)
    elif filename.endswith('.tar.bz2'):
        with tarfile.open(directory_and_file, 'r:bz2') as tar_ref:
            tar_ref.extractall(directory)


def files_to_items(directory: str) -> tuple[list, list, list, list]:
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

        killmails.append(decoded_killmail.get('killmail'))
        if decoded_killmail.get('attackers'):
            attackers.extend(decoded_killmail['attackers'])
        if decoded_killmail.get('victim'):
            victims.append(decoded_killmail['victim'])
        # Items will be array of arrays [ [ item,item ], [ item,item ] ]
        # Upload it bactches of 'items per killmail' to make error resolution easier
        if decoded_killmail.get('items'):
            items.extend(decoded_killmail['items'])
    return ((killmails, victims, attackers, items))


def prime_database(killmails, victims, attackers, items):
    print(
        '##################!Prime that database!###################')
    killmail_dataframe = pd.json_normalize(killmails)
    victim_dataframe = pd.json_normalize(victims)
    attacker_dataframe = pd.json_normalize(attackers)
    item_dataframe = pd.json_normalize(items)

    ##
    # Crazy stuff to try to fix items not processing correctly
    # Making batches smaller (uploading very small dataframes to the database)
    ##

    # pg_conn = psycopg2.connect(conn_string)
    # uncomment to use pandas df_to_sql

    df_to_db(item_dataframe, 'items')
    df_to_db(killmail_dataframe, 'killmails')
    df_to_db(victim_dataframe, 'victims')
    df_to_db(attacker_dataframe, 'attackers')
    print(
        '##################!Done Priming LOL!###################')
    # pg_conn.close()


def load_items_into_database(killmails, victims, attackers, items, pg_conn):
    killmail_dataframe = pd.json_normalize(killmails)
    victim_dataframe = pd.json_normalize(victims)
    attacker_dataframe = pd.json_normalize(attackers)
    item_dataframe = pd.json_normalize(items)
    copy_df_to_db(item_dataframe, 'items', pg_conn)
    copy_df_to_db(killmail_dataframe, 'killmails', pg_conn)
    copy_df_to_db(victim_dataframe, 'victims', pg_conn)
    copy_df_to_db(attacker_dataframe, 'attackers', pg_conn)


def decode_with_msgspec(json_data: Union[bytes, str]) -> dict:
    return msgspec.json.decode(json_data)


def encode_with_msgspec(json_data: dict) -> bytes:
    return msgspec.json.encode(json_data)
