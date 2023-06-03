import uuid
import pandas as pd


def extract_attacker_and_victim_and_items(json: dict) -> dict[str, dict]:
    """Process a killmail into a dataframe"""

    def add_id_item(item: dict) -> dict:
        item['data_processing_id'] = str(uuid.uuid4())
        return item

    killmail_id = json['killmail_id']
    json = add_id_item(json)
    killmail_data_processing_id = json['data_processing_id']
    result: dict = {}

    def add_killmail_id(attacker):
        attacker['killmail_id'] = killmail_id
        return attacker

    def parent_id_item(item: dict) -> dict:
        item['killmail_data_processing_id'] = killmail_data_processing_id
        return item

    # Process attackers
    if json.get('attackers'):
        attackers: list[dict] = json.pop('attackers')
        attackers = list(map(add_killmail_id, attackers))
        attackers = list(map(add_id_item, attackers))
        attackers = list(map(parent_id_item, attackers))
        result['attackers'] = attackers

    # Process victim
    if json.get('victim'):
        victim: dict = json.pop('victim')
        victim = add_id_item(victim)
        victim = parent_id_item(victim)
        victim['killmail_id'] = killmail_id

    # Process Items
        if victim.get('items'):
            items: list[dict] = victim.pop('items')
            items = list(map(add_killmail_id, items))
            items = list(map(add_id_item, items))
            items = list(map(parent_id_item, items))
            new_items_list = []
            for i in items:
                if i.get('items'):
                    new_items = i.pop('items')
                    new_items = list(map(add_killmail_id, new_items))
                    new_items = list(map(add_id_item, new_items))
                    new_items = list(map(parent_id_item, new_items))
                    def add_parent_container_id(item):
                        item['parent_container_id'] = i['data_processing_id']
                        return item
                    new_items = list(map(add_parent_container_id, new_items))
                    new_items_list.extend(new_items)
                    
            items.extend(new_items_list)
            result['items'] = items
        else:
            print()
            victim.pop('items')
        result['victim'] = victim

    result['killmail'] = json

    return result


def extract_attackers(json: dict, killmail_id):
    if json['attackers']:
        attackers: list[dict] = json.pop('attackers')

        def add_killmail_id(attacker):
            attacker['killmail_id'] = killmail_id
            return attacker
        attackers = list(map(add_killmail_id, attackers))
        return attackers


def extract_items(victim, killmail_id):
    def add_killmail_id(attacker):
        attacker['killmail_id'] = killmail_id
        return attacker
    if victim['items']:
        items: list[dict] = victim.pop('items')
        items = list(map(add_killmail_id, items))
        return items
