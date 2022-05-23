import asyncio
import concurrent.futures
import logging
import os
import random
import sys
import time
import typing as tp

from aiogram import Bot, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import Dispatcher
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, ReplyKeyboardRemove
from aiogram.utils import executor, exceptions
from aiogram.utils.emoji import emojize
from aiogram.utils.helper import Helper, HelperMode, ListItem

from airtable import Airtable
import numpy as np

from config import *

week = 0

# Bot initialization
TG_TOKEN = os.environ.get('TG_TOKEN', None)
bot = Bot(token=TG_TOKEN)
dp = Dispatcher(bot, storage=MemoryStorage())

AT_TOKEN = os.environ.get('AT_TOKEN', None)
airtable_participants = Airtable(BASE_ID, PARTICIPANTS_TABLE, api_key=AT_TOKEN)
# airtable_pairs = Airtable(BASE_ID, PAIRS_TABLE, api_key=AT_TOKEN)

hello_markup = ReplyKeyboardMarkup(resize_keyboard=True,
                                   one_time_keyboard=True).add(KeyboardButton(HELLO_MARKUP))
user_markup = ReplyKeyboardMarkup(resize_keyboard=True).add(*[KeyboardButton(button) for button in USER_MARKUP])

admin_list = os.environ.get("ADMINS", '').split(', ')


async def send_message(uid, text):
    try:
        await bot.send_message(uid, text)
    except Exception as e:
        print(f"{uid}:   {e}")


def check_user(id):
    search = airtable_participants.search('tg_id', id)
    if len(search) > 1:
        return 2
    return len(search)


def create_header(users):
    result = "Привет! Твой собеседник на этой неделе:\n"
    if len(users) == 2:
        result = "Привет!\nНа этой неделе у тебя в 2 раза больше крутых собеседников! " \
                 "Из-за нечетного количества участников, ты попал в состав тройки.\n" \
                 "Твои собеседники на этой неделе:\n"
    return result


def format_pair_message(fields):
    prefix = create_header(fields['pair_id'].split(', '))
    description = fields['pair_description']
    usernames = "\nКонтакты: " + fields['pair_username'] + '\n'
    suffix = "Не откладывай, скорее пиши и договаривайся о встрече. \n" \
             "Будут вопросы, пиши в общий чат или в личку @saydashtatar!"
    return prefix + description + usernames + suffix


@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message) -> None:
    search = check_user(message.from_user.id)
    if search:
        await message.answer("Привет, {}!\n".format(message.from_user.first_name) +
                             ALREADY_USER_TEXT,
                             reply_markup=user_markup
                             )
    else:
        await message.answer("Привет, {}!\n".format(message.from_user.first_name) +
                             HELLO_TEXT +
                             LINK_BASE +
                             f'prefill_tg_id={message.from_user.id}&prefill_tg_username={message.from_user.username}',
                             reply_markup=hello_markup
                             )


@dp.message_handler(lambda message: message.text.lower() == "сделано")
async def finish_registration(message: types.Message):
    search = check_user(message.from_user.id)
    if search:
        await message.answer(REGISTERED_TEXT,
                             reply_markup=user_markup)
    else:
        await message.answer(REG_STATUS[0], reply_markup=hello_markup)
    # await message.answer(REG_STATUS[search], reply_markup=user_markup)


def get_level_index(record):
    return LEVELS.index(record['fields']['level'])


def get_age(record):
    return record['fields']['age']


def get_id(record):
    return record['fields']['tg_id']


def format_record(record):
    result = f'{record["fields"]["name"]} (@{record["fields"]["tg_username"]})\n' \
             f'Возраст: {record["fields"]["age"]}\n' \
             f'Уровень: {record["fields"]["level"]}\n'
    return result


@dp.message_handler(lambda message: message.text.lower() == "найти партнеров для игры")
async def show_users(message: types.Message):
    record = airtable_participants.search('tg_id', message.from_user.id)[0]
    all_records = airtable_participants.get_all()
    cur_level = get_level_index(record)
    cur_age = get_age(record)

    all_records_sorted = sorted(all_records, key=lambda x: (abs(get_level_index(x) - cur_level) * 20 +
                                                            abs(get_age(x) - cur_age) +
                                                            (get_id(x) == str(message.from_user.id)) * 10000))

    n_rec = 5
    if len(all_records) <= 5:
        n_rec = len(all_records) - 1
    text = f"Вот топ-{n_rec} наиболее похожих пользователей:\n"
    for i, user in enumerate(all_records_sorted[:n_rec]):
        text += f"{i + 1}. {format_record(user)}\n"

    await message.answer(text, reply_markup=user_markup)


# @dp.message_handler(lambda message: message.text.lower() == 'разослать пользователей')
# async def send_pairs(message: types.Message):
#     if str(message.from_user.id) not in admin_list:
#         return
#     records = airtable_participants.get_all()
#     for record in records:
#         uid = record['fields']['tg_id']
#         text = format_pair_message(record['fields'])
#         await send_message(uid, text)
#
#
# @dp.message_handler(lambda message: message.text.lower().startswith('рассылка для участников:'))
# async def send_mailing(message: types.Message):
#     if str(message.from_user.id) not in admin_list:
#         return
#     records = airtable_participants.get_all()
#     for record in records:
#         uid = record['fields']['tg_id']
#         await send_message(uid, message.text)
#
#
# @dp.message_handler(lambda message: message.text.lower() == 'поменять данные о себе')
# async def send_pairs(message: types.Message):
#     await message.answer("Поменять данные станет возможно в следующей версии нашего бота.")


@dp.message_handler()
async def common_answer(message: types.Message):
    await message.answer("К сожалению, я пока не очень умный. Поэтому лучше пользоваться кнопками внизу. \n"
                         "А если есть идеи как сделать меня умнее, пиши в личку @saydashtatar, "
                         "он выслушает и скажет спасибо!",
                         reply_markup=user_markup)


if __name__ == '__main__':
    executor.start_polling(dp)
