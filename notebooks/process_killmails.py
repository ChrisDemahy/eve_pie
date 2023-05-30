import pandas as pd


def extract_attacker_and_victim_and_items(json: dict) -> dict[str, dict]:
    """Process a killmail into a dataframe"""

    killmail_id = json['killmail_id']

    result: dict = {}

    def add_killmail_id(attacker):
        attacker['killmail_id'] = killmail_id
        return attacker

    if json.get('attackers'):
        attackers: list[dict] = json.pop('attackers')
        attackers = list(map(add_killmail_id, attackers))
        result['attackers'] = attackers
    if json.get('victim'):
        victim: dict = json.pop('victim')
        victim['killmail_id'] = killmail_id
        if victim.get('items'):
            items: list[dict] = victim.pop('items')
            items = list(map(add_killmail_id, items))
            result['items'] = items
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
