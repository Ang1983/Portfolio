import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.utils import executor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram.types import ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton, KeyboardButton, InputFile
import datetime
import warnings
import json
from functools import wraps
import pandas as pd
from gspread_pandas import Spread
from gspread_pandas.conf import get_config, get_creds
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from io import BytesIO
from typing import Union, Optional, AnyStr
import aiosqlite
from aiogram.dispatcher.storage import BaseStorage

warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)
pd.set_option('future.no_silent_downcasting', True)

TOKEN =
bot = Bot(TOKEN)

class SQLiteStorage(BaseStorage):

    def __init__(self, db_path):
        self._path_db = db_path
        self._db = None

    async def close(self):
        if isinstance(self._db, aiosqlite.Connection):
            await self._db.close()

    async def get_db(self) -> aiosqlite.Connection:
        if isinstance(self._db, aiosqlite.Connection):
            return self._db

        self._db = await aiosqlite.connect(database=self._path_db)
        await self._db.execute("""CREATE TABLE IF NOT EXISTS "aiogram_state"(
                                        "user" BIGINT NOT NULL PRIMARY KEY,
                                        "chat" BIGINT NOT NULL,
                                        "state" TEXT NOT NULL)""")
        
        return self._db

    async def wait_closed(self):
        return True

    async def set_state(self, *, chat: Union[str, int, None] = None,
                        user: Union[str, int, None] = None,
                        state: Optional[AnyStr] = None):
        chat, user = map(int, self.check_address(chat=chat, user=user))
        db = await self.get_db()

        if state is not None:
            await db.execute("""INSERT INTO "aiogram_state" VALUES(?, ?, ?)"""
                             """ON CONFLICT ("user") DO UPDATE SET "state" = ?""",
                             (user, chat, self.resolve_state(state), self.resolve_state(state)))
            await db.commit()
        else:
            await db.execute("""DELETE FROM "aiogram_state" WHERE chat=? AND "user"=?""", (chat, user))
            await db.commit()

    async def get_state(self, *, chat: Union[str, int, None] = None, user: Union[str, int, None] = None,
                        default: Optional[str] = None) -> Optional[str]:
        chat, user = map(int, self.check_address(chat=chat, user=user))
        db = await self.get_db()
        async with db.execute("""SELECT "state" FROM "aiogram_state" WHERE "chat"=? AND "user"=?""", (chat, user)) as cursor:
            result = await cursor.fetchone()
        return result[0] if result else self.resolve_state(default)
    
storage = SQLiteStorage(db_path='data/database.db')
dp = Dispatcher(bot, storage=storage)
    
user_id_list = []
girls_user_id = {}
admin_id =
admin_id_1 =
admin_id_2 =
work_id = {}

pending_requests = {}

secret = get_config(
    conf_dir= '/app',
    file_name= ''
    )

creds = get_creds(config=secret)

spreadsheet_link = ''
spread = Spread(
spreadsheet_link,
creds=creds
)

df_1 = spread.sheet_to_df(sheet='График химии и ген.уборок')


spreadsheet_link_2 = ''
spread_2 = Spread(
spreadsheet_link_2,
creds=creds
)

spreadsheet_link_3 = ''
spread_3 = Spread(
spreadsheet_link_3,
creds=creds
)

spreadsheet_link_4 = ''
spread_4 = Spread(
    spreadsheet_link_4,
    creds=creds
)

back_button = types.KeyboardButton('Назад')

keyboard1 = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard1.add(
    
)

keyboard_change = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard_change.add(
    
)
keyboard_change.add(back_button)

keyboard2 = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard2.add(
    
)
keyboard2.add(back_button)

keyboard3 = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard3.add(
   
)
keyboard3.add(back_button)

keyboard4 = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard4.add(
    
)
keyboard4.add(back_button)

keyboard5 = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard5.add(
    
)
keyboard5.add(back_button)

keyboard6 = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard6.add(
    
)
keyboard6.add(back_button)

keyboard7 = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard7.add(
   
)
keyboard7.add(back_button)

keyboard8 = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard8.add(
    
)
keyboard8.add(back_button)

keyboard9 = ReplyKeyboardMarkup(resize_keyboard=True)
keyboard9.add(back_button)

keyboard10 = types.ReplyKeyboardMarkup(resize_keyboard= True)
button_loc = types.KeyboardButton('Отправить локацию', request_location= True)
keyboard10.add(button_loc, back_button)

keyboard11 = types.ReplyKeyboardMarkup(resize_keyboard= True)
keyboard11.add(
    
)
keyboard11.add(back_button)

kb_gen = types.InlineKeyboardMarkup()
gen_button = types.InlineKeyboardButton(text='Заполнить отчет', url='')
kb_gen.add(gen_button)

kb = types.InlineKeyboardMarkup()
b = types.InlineKeyboardButton(text='Заполнить форму', url='')
kb.add(b)

kb_chek =types.InlineKeyboardMarkup()
chek = types.InlineKeyboardButton(text='Чек лист', url= '')
kb_chek.add(chek)

class Form(StatesGroup):
    handle_message_st = State()
    authorized_new_user = State()
    get_location_st = State()
    questions_category_st = State()
    who_change = State()
    change_workers = State()
    change_workers_update_df = State()
    first_category_st = State()
    second_category_st = State()
    third_category_st = State()
    for_category_st = State()
    five_category_st = State()
    six_category_st = State()
    choose_point_st = State() 
    get_helper = State()
    point_name = State()
    get_loc_helper = State()

def update_cached_data_sun():
    global df_money, cached_data
    try:
        df_money = spread_2.sheet_to_df(sheet='Выплата сотрудникам за неделю')
        df_money['Сколько перевести'] = df_money['Сколько перевести'].astype(str).str.replace('\xa0', '', regex=False).str.replace(' ', '', regex=False).astype(int)
        df_money = df_money.iloc[:, 0:11]
        df_money['Имя'] = df_money['Имя'].str.lower()
        cached_data['df_money'] = df_money.copy()
    except Exception as e:
        asyncio.run(bot.send_message(admin_id,f"[ОШИБКА] Не удалось обновить данные из таблиц: {e}"))

async def update_cell(row_name: str, col_name: str, value: str):
    df_2 = cached_data['df_2']
    spread_worksheet = Spread('', sheet='Учет опозданий сотрудников', creds=creds, create_sheet=False)
    worksheet = spread_worksheet.sheet

    try:
        row_idx = df_2.index.get_loc(row_name) + 2
        col_idx = df_2.columns.get_loc(col_name) + 2
        worksheet.update_cell(row_idx, col_idx, value)
        return True
    except Exception as e:
        print(f"[ERROR] Не удалось обновить ячейку {row_name}, {col_name}: {e}")
        await bot.send_message(admin_id, f'[ОШИБКА] Не удалось обновить ячейку "{row_name}, {col_name}"')
        return False

def find_or_insert_helper_row(data_str: str, suffix: str):
    df_2 = cached_data['df_2']
    spread_worksheet = Spread('', sheet='Учет опозданий сотрудников', creds=creds, create_sheet=False)
    worksheet = spread_worksheet.sheet
    if df_2 is None:
        print("[ОШИБКА] Кэшированный DataFrame пуст")
        return None

    try:
        date_index = df_2.index.get_loc(data_str)
    except KeyError:
        print(f"[ОШИБКА] Дата {data_str} не найдена в таблице")
        return None

    helper_row_name = f"{data_str} {suffix}"

    if helper_row_name not in df_2.index:
        if suffix not in helper_row_name:
            try:
                worksheet.insert_row([""] * len(df_2.columns), index=date_index + 3)
                worksheet.update_cell(date_index + 3, 1, helper_row_name)

                new_row = pd.DataFrame([{col: None for col in df_2.columns}], index=[helper_row_name])
                df_2 = pd.concat([df_2.iloc[:date_index + 2], new_row, df_2.iloc[date_index + 2:]])
                cached_data['df_2'] = df_2
            except Exception as e:
                print(f"[ОШИБКА] Не удалось вставить строку '{helper_row_name}': {e}")
                return None
        else:
            try:
                worksheet.insert_row([""] * len(df_2.columns), index=date_index + 3)
                worksheet.update_cell(date_index + 3, 1, helper_row_name)

                new_row = pd.DataFrame([{col: None for col in df_2.columns}], index=[helper_row_name])
                df_2 = pd.concat([df_2.iloc[:date_index + 1], new_row, df_2.iloc[date_index + 1:]])
                cached_data['df_2'] = df_2
            except Exception as e:
                print(f"[ОШИБКА] Не удалось вставить строку '{helper_row_name}': {e}")
                return None

    return helper_row_name

async def update_df(row_name: str, col_name: str, value: str):
    df = cached_data['df']
    spread_worksheet = Spread('', sheet='График', creds=creds, create_sheet=False)
    worksheet = spread_worksheet.sheet

    try:
        row_idx = df.index.get_loc(row_name) + 2
        col_idx = df.columns.get_loc(col_name) + 2
        worksheet.update_cell(row_idx, col_idx, value)
        return True
    except Exception as e:
        print(f"[ERROR] Не удалось обновить ячейку {row_name}, {col_name}: {e}")
        await bot.send_message(admin_id, f'[ОШИБКА] Не удалось обновить ячейку "{row_name}, {col_name}"')
        return False
    
async def change_worker_update(row_date: str, col_name: str, user: str, point: str):
    df_3 = cached_data['df_3']
    spread_worksheet = Spread('', sheet='Лист3', creds=creds, create_sheet=False)
    worksheet = spread_worksheet.sheet
    if col_name in work_id.keys() or col_name in df_3.columns.tolist():
        try:
            row_idx = df_3.index.get_loc(row_date) + 2
            col_idx = df_3.columns.get_loc(user) + 2
            worksheet.update_cell(row_idx, col_idx, '')
            row_idx = df_3.index.get_loc(row_date) + 2
            col_idx = df_3.columns.get_loc(col_name) + 2
            worksheet.update_cell(row_idx, col_idx, point)
            return True
        except Exception as e:
            await bot.send_message(admin_id, f'[ОШИБКА]')
            return False
    else:
        try:
            row_idx = df_3.index.get_loc(row_date) + 2
            col_idx = df_3.columns.get_loc(user) + 2
            worksheet.update_cell(row_idx, col_idx, f'{col_name} {point}')
            return True
        except Exception as e:
            await bot.send_message(admin_id, f'[ОШИБКА]')
            return False

with open("config.json", "r", encoding="utf-8") as f:
    config = json.load(f)

async def get_girl_name(key):
    df = cached_data['df']
    data = (datetime.date.today() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
    try:
        chat_id = work_id[df.loc[data, key]]
        return girls_user_id[chat_id]
    except (KeyError, TypeError):
        return ''
    
def check_employee_exists(location_key):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            df = cached_data['df']
            try:
                data = (datetime.date.today() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
                employee = df.loc[data, location_key]
                if pd.isna(employee) or employee not in work_id:
                    return
                return await func(*args, **kwargs)
            except KeyError:
                return
        return wrapper
    return decorator

def dataframe_to_image(df: pd.DataFrame) -> BytesIO:
    max_width = sum(df.astype(str).applymap(len).max()) + len(df.columns)
    fig_width = min(max_width / 5, 12)
    fig_height = len(df) * 0.6 

    fig, ax = plt.subplots(figsize=(fig_width, fig_height))
    ax.axis('tight')
    ax.axis('off')

    table = ax.table(
        cellText=df.values,
        colLabels=df.columns,
        cellLoc='center',
        loc='center'
    )

    max_widths = []
    for j, col in enumerate(df.columns):
        max_content_length = max(
            df[col].astype(str).map(len).max(),
            len(str(col))
        )
        max_widths.append(max_content_length)

    total_width = sum(max_widths)
    if total_width > 0:
        col_widths = [w / total_width for w in max_widths]
    else:
        col_widths = [1 / len(df.columns)] * len(df.columns)

    table.auto_set_column_width(list(range(len(df.columns))))
    for j, width in enumerate(col_widths):
        table.auto_set_column_width(j)
        for i in range(len(df) + 1):
            cell = table[(i, j)]
            cell.set_width(width)

    table.set_fontsize(9)
    table.scale(1, 2) 

    for (i, j), cell in table.get_celld().items():
        if i == 0:
            cell.set_text_props(weight='bold', color='black')
            cell.set_facecolor('#d5d5d5')
        else:
            cell.set_facecolor('#f8f9fa')

    plt.tight_layout(pad=0) 
    fig.subplots_adjust(left=0, right=1, top=1, bottom=0)

    buf = BytesIO()
    fig.savefig(buf, format='png', dpi=150, bbox_inches='tight', pad_inches=0)
    buf.seek(0)
    plt.close(fig)

    return buf

def tables(name: str):
    df_name = df_money[df_money['Имя'] == name].copy().reset_index(drop=True)
    sum_value = sum(int(elem) for elem in df_name[''])
    new_row = pd.DataFrame([{'Дата':'','Имя':'','Точка':'','Выручка':'','Взяли из кассы':'','Ставка':'','Процент':'','ЗП':'','Штраф': 'ИТОГО', 'Сколько перевести': sum_value, 'Комментарии':''}])
    df_name = pd.concat([df_name, new_row], ignore_index=True)
    return dataframe_to_image(df_name)

cached_data = {
    'df': None,
    'df_money': None,
    'df_2': None,
    'df_3':None,
    'df_4': None,
    'last_update': None
}


LOCATIONS = {
    '': ''
}

@dp.message_handler(commands= ['start'])
async def start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await Form.handle_message_st.set()
    if user_id == admin_id or user_id == admin_id_1 or user_id == admin_id_2:
        await message.reply(f'Привет {girls_user_id[user_id]}, я буду помогать тебе следить за опозданиями сотрудников ;)!', reply_markup=keyboard1)
    elif user_id not in user_id_list:
        await message.reply(f'Приветствую, я твой бот помощник!\nЧем могу помочь?', reply_markup=keyboard1)
    else:
        await message.reply(f'Привет {girls_user_id[user_id]}, я твой бот помощник!\nЧем могу помочь?', reply_markup=keyboard1)

@dp.message_handler(state=Form.handle_message_st)
async def handle_message(message: types.Message, state: FSMContext):
    global user_id, username
    user_id = message.from_user.id
    if message.text == 'Начать смену':
        await Form.get_location_st.set()
        await message.reply('Отправьте вашу геолокацию, для согласования с админом', reply_markup=keyboard10)
    elif message.text == 'Задать вопрос':
        await Form.questions_category_st.set()
        await message.reply('Выберите категорию вашего вопроса', reply_markup=keyboard2)
    elif message.text == 'Кто сегодня на смене?':
        try:
            '' = await get_girl_name('')

            await Form.handle_message_st.set()

            await message.reply(
                f'"" - {''}\n'
                reply_markup=keyboard9
            )

        except Exception as e:
            await message.reply(f'Произошла ошибка: {str(e)}', reply_markup=keyboard9)
    elif message.text == 'Назад':
        await Form.handle_message_st.set()
        await message.reply('Чем могу помочь?)', reply_markup=keyboard1)
    elif message.text == 'Начать смену ПОМОЩНИКИ/СМЕНЩИКИ':
        await Form.get_helper.set()
        await message.reply('Смена помощника или сменщика?', reply_markup=keyboard11)
    elif message.text == 'Уведомить о замене':
        await Form.who_change.set()
        await message.reply('Выберете', reply_markup=keyboard_change)
    else:
        await Form.handle_message_st.set()
        await message.reply('Я тебя не понял, выбери из предложенных вариантов(', reply_markup=keyboard1)

@dp.message_handler(state=Form.who_change)
async def who_change(message: types.Message, state: FSMContext):
    global mes_text
    if message.text == 'Назад':
        await Form.handle_message_st.set()
        await message.reply('Чем могу помочь?)', reply_markup=keyboard1)
    elif message.text != 'Меня заменяют' and message.text != 'Я заменяю':
        await Form.handle_message_st.set()
        await message.reply('Я тебя не понял, выбери из предложенных вариантов(', reply_markup=keyboard1)
    else:
        if message.text == 'Меня заменяют':
            mes_text = 'На какую дату вы нашли себе замену? (Как в графике, пример: 29 июля)'
        elif message.text == 'Я заменяю':
            mes_text = 'Какого числа вы выходите на замену? (Как в графике, пример: 29 июля)'
        await Form.change_workers.set()
        await message.reply(mes_text, reply_markup=keyboard9)

@dp.message_handler(state=Form.change_workers)
async def change(message: types.Message, state: FSMContext):
    global change_date, pref
    change_date = message.text.strip()
    if message.text == 'Назад':
        await Form.handle_message_st.set()
        await message.reply('Чем могу помочь?)', reply_markup=keyboard1)
    else:
        if mes_text == 'На какую дату вы нашли себе замену? (Как в графике, пример: 29 июля)':
            pref = 'Кто'
            await message.reply(f'{pref} вас заменяет? Имя сотрудника должно быть написано как в графике', reply_markup=keyboard9)
            await Form.change_workers_update_df.set()
        elif mes_text == 'Какого числа вы выходите на замену? (Как в графике, пример: 29 июля)':
            pref = 'Кого'
            await message.reply(f'{pref} вы заменяете? Имя сотрудника должно быть написано как в графике', reply_markup=keyboard9)
            await Form.change_workers_update_df.set()

@dp.message_handler(state=Form.change_workers_update_df)
async def change_update_df(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    usern = message.from_user.username
    user = None
    for key, value in work_id.items():
        if user_id == value:
            user = key

    if message.text == 'Назад':
        await Form.handle_message_st.set()
        await message.reply('Чем могу помочь?)', reply_markup=keyboard1)
    else:
        worker = message.text.strip().capitalize()
        point = df_3.loc[change_date, user]
        if pref == 'Кто':
            await change_worker_update(change_date, worker, user, point)
            if worker in work_id.keys():
                await bot.send_message(work_id[worker], f'Сотрудник {user} сообщил, что отдал вам смену {change_date} по адресу {point}')
                await bot.send_message(admin_id_1, f'Вместо сотрудника {user}, {change_date} на точку {point} выйдет {worker}\nДанные в таблице график обновлены, проверьте', reply_markup=keyboard1)
            elif worker in df_3.columns.tolist() and worker not in work_id.keys():
                await bot.send_message(admin_id_1, f'Вместо сотрудника {user}, {change_date} на точку {point} выйдет {worker}\nДанные в таблице график обновлены, проверьте', reply_markup=keyboard1)
            else:                    
                await bot.send_message(admin_id_1, f'Вместо сотрудника {user}, {change_date} на точку {point} выйдет {worker}\nСотрудник {worker} не найден в таблице, данные изменены в ячейке, проверьте', reply_markup=keyboard1)
        elif pref == 'Кого':
            await change_worker_update(change_date, user, worker, point)
            if worker in work_id.keys():
                await bot.send_message(work_id[worker], f'Сотрудник {user} сообщил, что вы поменялись и он выйдет вместо вас {change_date} на точку {point}')
                await bot.send_message(admin_id_1, f'Вместо сотрудника {worker}, {change_date} на точку {point} выйдет {user}\nДанные в таблице график обновлены, проверьте', reply_markup=keyboard1)
            elif worker in df_3.columns.tolist() and worker not in work_id.keys():
                await bot.send_message(admin_id_1, f'Вместо сотрудника {worker}, {change_date} на точку {point} выйдет {user}\nДанные в таблице график обновлены, проверьте', reply_markup=keyboard1)
                await bot.send_message(admin_id_1, f'Сотрудник {user}, выходит на замену вместо {worker} {change_date} на точке {point}')
            else:
                await bot.send_message(admin_id_1, f'Вместо сотрудника {worker}, {change_date} на точку {point} выйдет {user}\nСотрудник {worker} не найден в таблице, данные изменены в ячейке, проверьте', reply_markup=keyboard1)
        await bot.send_message(user_id, 'Благодарю, администратору отправлено уведомление о вашей замене')

    await Form.handle_message_st.set()

@dp.message_handler(state=Form.get_helper)
async def helper(message: types.Message, state: FSMContext):
    global log_pref
    if message.text == 'Помощник':
        log_pref = f'Помощник'
        keyb = ReplyKeyboardMarkup(resize_keyboard=True)
        keyb.add(*[KeyboardButton(p) for p in []])
        keyb.add(KeyboardButton('Назад'))
        await Form.point_name.set()
        await message.reply('На какой точке вы работаете?', reply_markup=keyb)
    elif message.text == 'Сменщик':
        log_pref = f'Сменщик'
        keyb = ReplyKeyboardMarkup(resize_keyboard=True)
        keyb.add(*[KeyboardButton(p) for p in []])
        keyb.add(KeyboardButton('Назад'))
        await Form.point_name.set()
        await message.reply('На какой точке вы работаете?', reply_markup=keyb)
    elif message.text == 'Назад':
        await Form.handle_message_st.set()
        return await message.reply('Чем могу помочь?)', reply_markup=keyboard1)
    
@dp.message_handler(state=Form.point_name)
async def helper(message: types.Message, state: FSMContext):
    text = message.text.strip().lower()
    global point_help
    location = {
        "":""
    }

    point_help = location.get(text)
    await Form.get_loc_helper.set()
    await message.reply('Отправьте вашу геолокацию, для согласования с админом', reply_markup=keyboard10)

@dp.message_handler(state=Form.get_loc_helper, content_types=['location', 'text'])
async def get_loc_helper(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = girls_user_id.get(user_id, message.from_user.username)

    if message.text == 'Назад':
        await Form.handle_message_st.set()
        return await message.reply('Чем могу помочь?)', reply_markup=keyboard1)

    if not message.location:
        await Form.get_location_st.set()
        return await bot.send_message(user_id, 'Отправьте локацию(')

    try:
        await bot.send_location(admin_id, message.location.latitude, message.location.longitude)
    except Exception as loc_err:
        await bot.send_message(admin_id, f"[ЛОК] Не удалось отправить локацию от {user_id}: {loc_err}")

    data = (datetime.date.today() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
    time_send = (datetime.datetime.now() + datetime.timedelta(hours=3)).strftime('%H:%M')

    log_message = "Неизвестная ошибка"

    if not point_help:
        await bot.send_message(user_id, 'Ошибка: точка не выбрана')
        return

    if log_pref == 'Помощник' and user_id not in user_id_list:
        log_message = f'{log_pref} @{username} {user_id} отправил геолокацию в {time_send}'
    elif log_pref == 'Помощник' and user_id in user_id_list:
        log_message = f'{log_pref} {username} отправил геолокацию в {time_send}'
    elif log_pref == 'Сменщик' and user_id in user_id_list:
        log_message = f'{log_pref} {username} отправил геолокацию в {time_send}'
        for key,value in work_id.items():
            if user_id == value:
                user_name = key
        await update_df(data, point_help, user_name)
    elif log_pref == 'Сменщик' and user_id not in user_id_list:
        log_message = f'{log_pref} @{username} {user_id} отправил геолокацию в {time_send}'
    else:
        log_message = f'{log_pref} не определён'
        await bot.send_message(user_id, 'Ошибка!')
        return

    helper_row_name = find_or_insert_helper_row(data, 'ПОМОЩНИКИ' if log_pref == 'Помощник' else 'СМЕНЩИКИ')

    if helper_row_name:
        success = await update_cell(helper_row_name, point_help, f'{username} в {time_send}')
        if not success:
            await bot.send_message(admin_id, f"[ОШИБКА] Не удалось записать данные в таблицу для {helper_row_name}, {point_help}")
    else:
        await bot.send_message(admin_id, "[ОШИБКА] Не удалось найти/вставить строку для записи")

    try:
        await bot.send_message(user_id, 'Ваша геолокация отправлена админу, хорошей смены!', reply_markup=keyboard1)
        await bot.send_message(admin_id, log_message)
    finally:
        await Form.handle_message_st.set()

@dp.message_handler(state=Form.get_location_st, content_types=['location', 'text'])
async def get_location(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = girls_user_id.get(user_id, message.from_user.username)
    log_prefix = f'Сотрудник'

    if message.text == 'Назад':
        await Form.handle_message_st.set()
        return await message.reply('Чем могу помочь?)', reply_markup=keyboard1)

    if not message.location:
        await Form.get_location_st.set()
        return await bot.send_message(user_id, 'Отправьте локацию(')

    try:
        await bot.send_location(admin_id, message.location.latitude, message.location.longitude)
    except Exception as loc_err:
        await bot.send_message(admin_id, f"[ЛОК] Не удалось отправить локацию от {user_id}: {loc_err}")

    data = (datetime.date.today() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
    time_send = (datetime.datetime.now() + datetime.timedelta(hours=3)).strftime('%H:%M')

    location_map = {}
    locations_to_check = []

    for point in locations_to_check:
        try:
            user_id_from_sheet = work_id[df.loc[data, point]]
            location_map[user_id_from_sheet] = point
        except Exception:
            continue

    if user_id in location_map:
        point_name = location_map[user_id]
        value_to_set = f'{username} в {time_send}'
        await update_cell(data, point_name, value_to_set)
        log_message = f'{log_prefix} {point_name} {username} отправил геолокацию в {time_send}'
    elif user_id not in user_id_list:
        log_message = f'Неизвестный сотрудник @{username} {user_id} отправил геолокацию в {time_send}'
    elif user_id in user_id_list and user_id not in location_map:
        keyb = ReplyKeyboardMarkup(resize_keyboard=True)
        keyb.add(*[KeyboardButton(p) for p in []])
        keyb.add(KeyboardButton('Назад'))
        await bot.send_message(user_id, 'Сотрудника какой точки вы заменяете? Выберете', reply_markup=keyb)
        await Form.choose_point_st.set()
        return
    else:
        log_message = f'{log_prefix} не определён'
        await bot.send_message(user_id, 'Ошибка!')
        return

    await bot.send_message(user_id, 'Ваша геолокация отправлена админу, хорошей смены!', reply_markup=keyboard1)
    await bot.send_message(admin_id, log_message)
    await Form.handle_message_st.set()

@dp.message_handler(state=Form.choose_point_st)
async def process_menu(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    text = message.text.strip().lower()
    data = (datetime.date.today() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
    time_send = (datetime.datetime.now() + datetime.timedelta(hours=3)).strftime('%H:%M')
    username = girls_user_id.get(user_id, message.from_user.username)

    valid_points = []
    if text.lower() == 'назад':
        await Form.handle_message_st.set()
        await bot.send_message(user_id, 'Выберите действие:', reply_markup=keyboard1)
        return

    if text not in valid_points:
        await bot.send_message(user_id, 'Ошибка: неверная точка. Пожалуйста, выберите из предложенных вариантов.')
        return

    point = text
    value_to_set = f'{username} в {time_send}'
    log_message = f'Сотрудник {point} {username} отправил геолокацию в {time_send}'

    for key, value in work_id.items():
        if value == user_id:
            user_name = key

    await update_df(data, point, user_name)

    try:
        await update_cell(data, point, value_to_set)
        await bot.send_message(admin_id, log_message)
        await bot.send_message(user_id, 'Ваша геолокация отправлена админу, хорошей смены!', reply_markup=keyboard1)
    except Exception as e:
        await bot.send_message(user_id, f'Произошла ошибка: {str(e)}')
    finally:
        await Form.handle_message_st.set()

@dp.message_handler(state=Form.questions_category_st)
async def questions_category(message: types.Message, state: FSMContext):
    text = message.text

    if text == "Назад":
        await Form.handle_message_st.set()
        await message.reply('Чем могу помочь?)', reply_markup=keyboard1)
        return

    category = config["categories"].get(text)
    if not category:
        await message.reply('Я тебя не понял, выбери из предложенных вариантов(')
        return
    
    if "photo" in category:
        keyboard = types.ReplyKeyboardMarkup(resize_keyboard=True).add(types.KeyboardButton("Назад"))
        await message.reply_photo(photo=open(category["photo"], 'rb'), reply_markup=keyboard)
        return

    if "buttons" in category:
        keyboard = InlineKeyboardMarkup()
        for btn in category["buttons"]:
            keyboard.add(InlineKeyboardButton(text=btn["text"], url=btn["url"]))
        keyboard.add(InlineKeyboardButton(text="↩️ Назад", callback_data="back_to_menu"))
        await message.reply('Выберите нужный вариант:', reply_markup=keyboard)
        return

    state_name = category.get("state")
    await getattr(Form, state_name).set()

    subcategories = category.get("subcategories", None)
    if subcategories:
        keyboard_name = category.get("keyboard", "keyboard9")
        keyboard = globals().get(keyboard_name, None)

        await message.reply(
            'Уточните какой именно вопрос из этой категории вас интересует',
            reply_markup=keyboard
        )

async def handle_subcategory(message: types.Message, category_name: str, state_attr: str):
    text = message.text

    if text == "Назад":
        await Form.questions_category_st.set()
        await message.reply('Чем могу помочь?)', reply_markup=keyboard2)
        return

    category = config["categories"].get(category_name)
    if not category or text not in category.get("subcategories", {}):
        await message.reply('Я тебя не понял, выбери из предложенных вариантов(', reply_markup=keyboard2)
        return

    subcategory = category["subcategories"][text]
    state_name = subcategory.get("state")
    await getattr(Form, state_name).set()

    if "text_file" in subcategory:
        if "buttons" in subcategory:
            keyboard = InlineKeyboardMarkup()
            for btn in subcategory["buttons"]:
                keyboard.add(InlineKeyboardButton(text=btn["text"], url=btn["url"]))
        else:
            keyboard_name = subcategory.get("keyboard", "keyboard9")
            keyboard = globals().get(keyboard_name, None)
        file_path = subcategory["text_file"]
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
        await message.reply(content, reply_markup=keyboard)

    elif "buttons" in subcategory:
        keyboard = InlineKeyboardMarkup()
        for btn in subcategory["buttons"]:
            keyboard.add(InlineKeyboardButton(text=btn["text"], url=btn["url"]))
        if "text" in subcategory:
            await message.reply(subcategory["text"], reply_markup=keyboard)
        else:
            await message.reply('Выберите нужный вариант:', reply_markup=keyboard)

@dp.message_handler(state=Form.first_category_st)
async def first_category(message: types.Message, state: FSMContext):
    await handle_subcategory(message)

@dp.message_handler(state=Form.second_category_st)
async def second_category(message: types.Message, state: FSMContext):
    await handle_subcategory(message)

@dp.message_handler(state=Form.third_category_st)
async def third_category(message: types.Message, state: FSMContext):
    await handle_subcategory(message)

@dp.message_handler(state=Form.for_category_st)
async def for_category(message: types.Message, state: FSMContext):
    await handle_subcategory(message)

@dp.message_handler(state=Form.five_category_st)
async def five_category(message: types.Message, state: FSMContext):
    await handle_subcategory(message)

@dp.message_handler(state=Form.six_category_st)
async def six_category(message: types.Message, state: FSMContext):
    await handle_subcategory(message)

def make_send_message(key):
    @check_employee_exists(key)
    async def handler():
        df = cached_data['df']
        data = (datetime.date.today() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
        chat_id = work_id[df.loc[data, key]]
        msg = 'Доброе утро! Через 10 минут начало рабочего дня\nНе забудь начать смену)'
        await bot.send_message(chat_id, msg)

        if datetime.datetime.now().weekday() in (0, 3): 
            await bot.send_message(chat_id, 'Сегодня чек лист', reply_markup=kb_chek)
        elif datetime.datetime.now().weekday() in (1, 4):
            await bot.send_message(chat_id, 'Сегодня зазказ молока ')
    return handler


def make_end_message(key):
    @check_employee_exists(key)
    async def handler():
        df = cached_data['df']
        data = (datetime.date.today() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
        chat_id = work_id[df.loc[data, key]]
        kb = types.InlineKeyboardMarkup()
        b = types.InlineKeyboardButton(
            text='Итоги дня',
            url=''
        )
        kb.add(b)
        await bot.send_message(chat_id, 'Через 10 минут конец рабочего дня, не забудь заполнить отчет по выручке, хорошего вечера)', reply_markup=kb)
    return handler

def get_cleaning_dates(df_1, column_name):
    return df_1[column_name].dropna().astype(str).tolist()

def make_gen_message(key):
    @check_employee_exists(key)
    async def handler():
        data = (datetime.date.today() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
        chat_id = work_id[df.loc[data, key]]

        chem_dates = get_cleaning_dates(df_1, 'мытье с химией')
        clean_dates = get_cleaning_dates(df_1, 'ген.уборка')

        if data in chem_dates:
            await bot.send_message(chat_id, 'Сегодня по графику мытье кофемашины с химией')

        if data in clean_dates:
            await bot.send_message(chat_id, 'Сегодня по графику генеральная уборка')
    
    return handler

def make_cleaning_reminder(key):
    @check_employee_exists(key)
    async def handler():
        data = (datetime.date.today() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
        chat_id = work_id[df.loc[data, key]]

        chem_dates = get_cleaning_dates(df_1, 'мытье с химией')
        clean_dates = get_cleaning_dates(df_1, 'ген.уборка')

        if data in chem_dates:
            await bot.send_message(chat_id, 'Сегодня по графику было мытье кофемашины с химией, не забудьте заполнить отчет', reply_markup=kb_gen)

        if data in clean_dates:
            await bot.send_message(chat_id, 'Сегодня по графику была генеральная уборка, не забудьте заполнить отчет', reply_markup=kb_gen)
    return handler

def make_out_work(key, location_name):
    @check_employee_exists(key)
    async def handler():
        df = cached_data['df']
        data = (datetime.date.today() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
        employee = df.loc[data, key]
        value = df_2.loc[data, key]

        if pd.isna(value) or value == '':
            message = (
                f'Прошло 10 минут с начала рабочего дня сотрудник {employee} не отправил геолокацию\n'
                f'Адрес кофейни: {location_name}'
            )
            await bot.send_message(admin_id, message)
            await bot.send_message(admin_id_1, message)
            await bot.send_message(admin_id_2, message)
        elif pd.isna(employee):
            return
    return handler

async def tables_every_sun():
    names = set(df_money['Имя'])
    for name in names:
        image =  tables(name)
        caption = name
        await bot.send_photo(chat_id=admin_id, photo=InputFile(image, filename="report.png"), caption=caption)

async def schedule_nk_jobs(scheduler):
    if datetime.datetime.now().weekday() in (5, 6):
        morning_time = ('8', '50')
        evening_time = ('20', '50')
        gen_time = ('9', '30')
        gen_end = ('20', '30')
        out_time = ('9', '10')
    else:
        morning_time = ('8', '20')
        evening_time = ('20', '20')
        gen_time = ('9', '30')
        gen_end = ('20', '00')
        out_time = ('8', '40')

    scheduler.add_job(make_send_message('нк'), 'cron', hour=morning_time[0], minute=evening_time[1], timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)
    scheduler.add_job(make_end_message('нк'), 'cron', hour=evening_time[0], minute=evening_time[1], timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)
    scheduler.add_job(make_gen_message('нк'), 'cron', hour=gen_time[0], minute=gen_time[1], timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)
    scheduler.add_job(make_cleaning_reminder('нк'), 'cron', hour=gen_end[0], minute=gen_end[1], timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)
    scheduler.add_job(make_out_work('нк', 'Новокосино'), 'cron', hour=out_time[0], minute=out_time[1], timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)

async def schedule_messages():
    scheduler = AsyncIOScheduler(timezone='Europe/Moscow')
    scheduler.add_job(update_cached_data_sun, 'cron', day_of_week='sun', hour=22, minute=18, timezone='Europe/Moscow', coalesce=True, misfire_grace_time=300)
    scheduler.add_job(tables_every_sun, 'cron', day_of_week='sun', hour='22', minute='20', timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)

    location = {
    }

    await schedule_nk_jobs(scheduler)

    morning_times = [('9', '20'), ('9', '50'), ('9', '50'), ('7', '50'), ('8', '50'), ('8', '50'), ('8', '50')]
    evening_times = [('21', '20'), ('21', '50'), ('21', '50'), ('20', '50'), ('20', '50'), ('20', '50'), ('20', '50')]
    gen_time = [('10', '00'), ('10', '30'), ('10', '30'), ('8', '30'), ('9', '30'), ('9', '30'), ('9', '30')]
    gen_end = [('21', '00'), ('21', '30'), ('21', '30'), ('20', '30'), ('20', '30'), ('20', '30'), ('20', '30')]
    out_time = [('9', '40'), ('10', '10'), ('10', '10'), ('8', '10'), ('9', '10'), ('9', '10'), ('9', '10')]

    for (mh, mm), (eh, em), (gh, gm), (geh, gem), (oh, om), key in zip(morning_times, evening_times, gen_time, gen_end, out_time, location.keys()):
        scheduler.add_job(make_send_message(key), 'cron', hour=mh, minute=mm, timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)
        scheduler.add_job(make_end_message(key), 'cron', hour=eh, minute=em, timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)
        scheduler.add_job(make_gen_message(key), 'cron', hour=gh, minute=gm, timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)
        scheduler.add_job(make_cleaning_reminder(key), 'cron', hour=geh, minute=gem, timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)
        scheduler.add_job(make_out_work(key, location[key]), 'cron', hour=oh, minute=om, timezone='Europe/Moscow', coalesce=True, misfire_grace_time=420)

    scheduler.start()

async def cache_updater():
    while True:
        update_cached_data()
        await asyncio.sleep(60 * 180)

async def on_startup(_):
    asyncio.create_task(cache_updater())
    await schedule_messages()

if __name__ == '__main__':
    executor.start_polling(dp, on_startup=on_startup)
