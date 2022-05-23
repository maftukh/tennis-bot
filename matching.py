from itertools import combinations
import os
import pickle
import random

from airtable import Airtable
import pandas as pd

from config import BASE_ID, PARTICIPANTS_TABLE, PAIRS_TABLE

AT_TOKEN = os.environ.get('AT_TOKEN', None)
airtable_participants = Airtable(BASE_ID, PARTICIPANTS_TABLE, api_key=AT_TOKEN)
airtable_pairs = Airtable(BASE_ID, PAIRS_TABLE, api_key=AT_TOKEN)
BAN_WEEK_NUM = 8


def check_pairs(pairs, blacklist):
    for pair in pairs:
        if pair in blacklist:
            return False
        elif (pair[1], pair[0]) in blacklist:
            return False
    return True


def get_users():
    users = airtable_participants.get_all()
    users_dct = {}
    for user in users:
        uid = user['fields']['tg_id']
        users_dct[uid] = user['fields']
    df_users = pd.DataFrame(users_dct).transpose()
    return df_users


def format_user(name, info):
    result = f"{name} \n\nО себе: \n{info}"
    return result


def collect_descriptions(df_users, users):
    if isinstance(users, str):
        name = df_users.loc[users, 'name']
        info = df_users.loc[users, 'info']
        return format_user(name, info)
    else:
        result = ''
        for uid in users:
            result += collect_descriptions(df_users, uid) + '\n\n\b'
        return result


def collect_usernames(df_users, users):
    if isinstance(users, str):
        username = df_users.loc[users, 'tg_username']
        if username.startswith('@'):
            return username
        return '@' + username
    else:
        result = ''
        for uid in users:
            result += collect_usernames(df_users, uid) + ' '
        return result


def upload_pairs(pairs, df_users):
    for pair in pairs:
        for i in range(len(pair)):
            user_id = pair[i]
            companions = pair[: i] + pair[i + 1:]
            companion_username = collect_usernames(df_users, companions)
            companion_description = collect_descriptions(df_users, companions)
            fields = {
                'week_num': week,
                'pair_id': ', '.join(companions),
                'pair_username': companion_username,
                'pair_description': companion_description,
            }

            record = airtable_pairs.search('participant_id', user_id)
            if record:
                airtable_pairs.update(record[0]['id'], fields)
            else:
                fields['participant_id'] = user_id
                airtable_pairs.insert(fields)


def generate_pairs():
    df_users = get_users()

    try:
        pairs_history = pd.read_pickle('pairs.pickle')
    except FileNotFoundError:
        pairs_history = pd.DataFrame(columns=['user_1', 'user_2', 'week_num'])
    try:
        with open('blacklist.pickle', 'rb') as file:
            pairs_blacklist = pickle.load(file)
    except FileNotFoundError:
        pairs_blacklist = []

    global week
    week = pairs_history['week_num'].max() + 1
    if week != week:
        # only if week is nan
        week = 0

    relevant_bans = (pairs_history['week_num'] >= week - BAN_WEEK_NUM).sum()
    pairs_blacklist = pairs_blacklist[-relevant_bans:]

    ids = df_users.index.values.copy()
    while True:
        random.shuffle(ids)
        pairs = list(zip(ids[::2], ids[1::2]))
        print("tried pairing")
        if check_pairs(pairs, set(pairs_blacklist)):
            break
    if len(ids) % 2 == 1:
        for i in range(len(pairs)):
            print('tried adding a triplet')
            if check_pairs((*pairs[i], ids[-1]), pairs_blacklist):
                pairs[i] = (*pairs[i], ids[-1])
                break
        else:
            # We'll not return without break from a loop when >10 users, just for making sure
            pairs[0] = (*pairs[0], ids[-1])

    upload_pairs(pairs, df_users)

    for elem in pairs:
        for pair in combinations(elem, 2):
            pairs_blacklist.append(pair)
            pairs_history = pairs_history.append({'user_1': pair[0],
                                                  'user_2': pair[1],
                                                  'week_num': week},
                                                 ignore_index=True)

    with open('blacklist.pickle', 'wb') as file:
        pickle.dump(pairs_blacklist, file)

    pairs_history.to_pickle('pairs.pickle')


if __name__ == '__main__':
    generate_pairs()
