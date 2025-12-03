"""
Telegram-бот для автоматизации операционных процессов в небольшой сети точек

Версия: 2.1  
Автор: Петрухина А.Д.  
Дата: 2025-10-25  

Описание:
- Автоматический сбор геолокации при начале смены
- Учёт посещаемости и отклонений через интеграцию с Google Sheets
- Генерация финансовых ведомостей (PNG) из табличных данных
- Расписание напоминаний (начало/окончание смены, техобслуживание, контроль сроков годности)
- Поддержка замен, обменов сменами, динамическое управление персоналом

Стек:
- Python 3.10+, aiogram 2.x, apscheduler, gspread_pandas, pandas, matplotlib
- Google Sheets API (чтение/запись), Redis (FSM, кэширование)
- Telegram Bot API

Безопасность:
- Все конфиденциальные данные заменены на заглушки (YOUR_*)
- Нет упоминаний компаний, точек, продуктов — только абстракции (A–H, Product Group)
- Для запуска замените `YOUR_*` в секции конфигурации
"""

# === ИМПОРТЫ ===
import asyncio
import json
import logging
import math
import datetime
from functools import wraps
from io import BytesIO
from typing import Optional, Dict, Any, Union

import pandas as pd
import matplotlib.pyplot as plt
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InputFile,
)
from aiogram.utils import executor
from aiogram.utils.exceptions import BadRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from gspread_pandas import Spread
from gspread_pandas.conf import get_config, get_creds

# Локальные модули (SQLite/Redis-интеграция)
from sqlite import (
    init_db, db_on, add_log_pref, edit_location, edit_point, get_time_send,
    get_locations_by_date, clear_locations_by_date, add_user, get_id,
    get_state_form, update_status, get_work_username, get_username,
    get_user_id, get_admin, get_all_work_username, get_all_users,
    update_work_username, delet_user, user_verification, db_message,
    edit_message, edit_location_messages, edit_sent_messages,
    get_message_id, get_location_messages, get_sent_messages,
    init_changes_db, add_change, get_all_changes_by_user
)
import redis
from aiogram.dispatcher.storage import BaseStorage

# === НАСТРОЙКА ЛОГГИРОВАНИЯ ===
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === КОНФИГУРАЦИЯ (ОБЕЗЛИЧЕНА) ===
TOKEN = 'YOUR_TELEGRAM_BOT_TOKEN'  # ← os.getenv("BOT_TOKEN") рекомендуется
bot = Bot(TOKEN)

# Redis-хранилище для FSM (безопасно для production)
class RedisStorage(BaseStorage):
    def __init__(self, host='redis', port=6379, db=0, password=None, prefix='fsm'):
        self.pool = redis.ConnectionPool(
            host=host,
            port=port,
            db=db,
            password=password,
            decode_responses=True
        )
        self.prefix = prefix

    def _get_key(self, chat: int, user: int, key: str) -> str:
        return f"{self.prefix}:{key}:{chat}:{user}"

    @staticmethod
    def check_address(chat: Union[str, int, None], user: Union[str, int, None]) -> (int, int):
        chat = int(chat) if chat else 0
        user = int(user) if user else 0
        return chat, user

    @staticmethod
    def resolve_state(state) -> Optional[str]:
        if state is None:
            return None
        return state.name if hasattr(state, 'name') else str(state)

    async def set_state(self, *, chat=None, user=None, state=None):
        chat, user = self.check_address(chat, user)
        key = self._get_key(chat, user, 'state')
        r = redis.Redis(connection_pool=self.pool)
        if state is None:
            r.delete(key)
        else:
            r.set(key, self.resolve_state(state))

    async def get_state(self, *, chat=None, user=None, default=None):
        chat, user = self.check_address(chat, user)
        key = self._get_key(chat, user, 'state')
        r = redis.Redis(connection_pool=self.pool)
        value = r.get(key)
        return value if value is not None else default

    async def set_data(self, *, chat=None, user=None, data=None):
        chat, user = self.check_address(chat, user)
        key = self._get_key(chat, user, 'data')
        r = redis.Redis(connection_pool=self.pool)
        r.set(key, json.dumps(data or {}))

    async def get_data(self, *, chat=None, user=None, default=None):
        chat, user = self.check_address(chat, user)
        key = self._get_key(chat, user, 'data')
        r = redis.Redis(connection_pool=self.pool)
        value = r.get(key)
        if value is not None:
            try:
                return json.loads(value)
            except:
                return {}
        return default or {}

    async def update_data(self, *, chat=None, user=None, data=None, **kwargs):
        current = await self.get_data(chat=chat, user=user)
        current.update(data or {})
        current.update(kwargs)
        await self.set_data(chat=chat, user=user, data=current)

    async def reset_state(self, *, chat=None, user=None, with_ :bool = True):
        await self.set_state(chat=chat, user=user, state=None)
        if with_:
            await self.set_data(chat=chat, user=user, data={})

    async def close(self):
        pass

    async def wait_closed(self):
        pass

# Подключение Redis (по умолчанию — локальный хост; для Docker/Amvera — свой endpoint)
storage = RedisStorage(host="localhost", port=6379, db=0)
dp = Dispatcher(bot, storage=storage)
dp.middleware.setup(LoggingMiddleware())

#Администраторы (можно несколько)
ADMIN_IDS = ['ID_1', 'ID_2', 'ID_3']  # ← заменить на int или использовать os.getenv()

#Google Sheets — замените 'YOUR_*' 
SECRET_CONFIG = get_config(
    conf_dir='/app',
    file_name='YOUR_CREDENTIALS.json'  # ← имя вашего credentials.json
)
CREDS = get_creds(config=SECRET_CONFIG)

#Ссылки на таблицы (оставлены как заглушки — замените ID на свои)
SPREADSHEET_LINKS = {
    'maintenance': 'https://docs.google.com/spreadsheets/d/YOUR_MAINTENANCE_ID/edit',
    'attendance': 'https://docs.google.com/spreadsheets/d/YOUR_ATTENDANCE_ID/edit',
    'payroll': 'https://docs.google.com/spreadsheets/d/YOUR_PAYROLL_ID/edit',
}
df_maintenance_spread = Spread(SPREADSHEET_LINKS['maintenance'], creds=CREDS)
df_maintenance = df_maintenance_spread.sheet_to_df(sheet='Schedule')

# === ИНТЕРФЕЙС ПОЛЬЗОВАТЕЛЯ: КЛАВИАТУРЫ ===
keyboard_user = InlineKeyboardMarkup(row_width=1)
keyboard_user.add(
    InlineKeyboardButton(text='Начать смену', callback_data='start_work'),
    InlineKeyboardButton(text='Задать вопрос', callback_data='question'),
    InlineKeyboardButton(text='Кто сегодня на смене?', callback_data='who_work'),
    InlineKeyboardButton(text='Начать смену ПОМОЩНИКИ/СМЕНЩИКИ', callback_data='work_change'),
    InlineKeyboardButton(text='Уведомить о замене', callback_data='notification')
)

keyboard_admin = InlineKeyboardMarkup(row_width=2)
keyboard_admin.add(
    *keyboard_user.inline_keyboard,  # наследуем user-кнопки
    InlineKeyboardButton(text='Табели ЗП', callback_data='payroll_sheets'),
    InlineKeyboardButton(text='Список сотрудников', callback_data='worker_list'),
    InlineKeyboardButton(text='Геолокации за сегодня', callback_data='worker_locations')
)

# Другие клавиатуры (сокращены для краткости)
keyboard_request_access = InlineKeyboardMarkup().add(
    InlineKeyboardButton(text='Запросить доступ', callback_data='allow')
)
keyboard_send_location = ReplyKeyboardMarkup(resize_keyboard=True).add(
    KeyboardButton('Отправить локацию', request_location=True)
)
# ... и т.д.

# === ВНЕШНИЕ ССЫЛКИ (ОБЕЗЛИЧЕНЫ) ===
kb_report = InlineKeyboardMarkup().add(
    InlineKeyboardButton(text='Заполнить отчёт', url='https://docs.google.com/forms/d/e/YOUR_FORM_ID/viewform'),
    InlineKeyboardButton(text='ОК', callback_data='OK')
)
kb_revenue = InlineKeyboardMarkup().add(
    InlineKeyboardButton(text='Итоги выручки', url='https://docs.google.com/forms/d/e/YOUR_FORM_ID/viewform'),
    InlineKeyboardButton(text='ОК', callback_data='OK')
)
kb_checklist = InlineKeyboardMarkup().add(
    InlineKeyboardButton(text='Чеклист', url='https://docs.google.com/spreadsheets/d/YOUR_SHEET_ID/edit'),
    InlineKeyboardButton(text='ОК', callback_data='OK')
)

# === FSM-СОСТОЯНИЯ ===
class Form(StatesGroup):
    get_location_st = State()           # Ожидание геолокации
    notification_st = State()           # Выбор типа уведомления
    notification_name_st = State()      # Уточнение деталей уведомления
    verification = State()              # Регистрация нового пользователя
    work_name_user = State()            # Имя в графике
    add_user_st = State()               # Добавление сотрудника
    del_user_st = State()               # Удаление сотрудника
    update_user_st = State()            # Обновление имени
    ask_question = State()              # Вопрос в FAQ

# === МАППИНГ ТОЧЕК (ОБЕЗЛИЧЕН) ===
LOCATION_CODES = {
    'a': 'Location A',
    'b': 'Location B',
    'c': 'Location C',
    'd': 'Location D',
    'e': 'Location E',
    'f': 'Location F',
    'g': 'Location G',
    'h': 'Location H',
}
LOCATION_ALIASES = {k: k.upper() for k in LOCATION_CODES}

# === КЭШ ДАННЫХ (ДЛЯ ПРОИЗВОДИТЕЛЬНОСТИ) ===
cached_data = {
    'schedule_df': None,      # Основной график смен
    'attendance_df': None,    # Учёт отклонений
    'payroll_df': None,       # Финансовые данные
    'changes_df': None,       # График замен
    'expiry_df': None,        # Сроки годности (Product Group monitoring)
    'last_update': None
}

# === ЗАГРУЗКА FAQ (БЕЗ КОНФИДЕНЦИАЛЬНОСТИ) ===
with open('config.json', 'r', encoding='utf-8') as f:
    CATEGORIES = json.load(f)['categories']
ITEMS_PER_PAGE = 5
CATEGORY_LIST = list(CATEGORIES.keys())

# === ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ===

#Обновление кэша из Google Sheets
async def update_cached_data():
    """Обновить кэш данных из Google Sheets."""
    global cached_data
    try:
        # Пример: основной график
        spread = Spread('YOUR_SCHEDULE_SPREADSHEET_ID', config=SECRET_CONFIG, sheet='Main Schedule')
        cached_data['schedule_df'] = spread.sheet_to_df().copy()
        # ... аналогично для attendance_df, payroll_df и expiry_df
        logger.info("Кэш данных обновлён")
    except Exception as e:
        logger.error(f"[ОШИБКА] Не удалось обновить кэш: {e}")
        for admin_id in ADMIN_IDS:
            try:
                await bot.send_message(admin_id, f"Ошибка обновления данных: {e}")
            except:
                pass

#Преобразование DataFrame → PNG
def dataframe_to_image(df: pd.DataFrame) -> BytesIO:
    """Генерирует изображение таблицы из pandas DataFrame."""
    # ... реализация без изменений — безопасна и обезличена
    return buf

#Генерация индивидуального табеля (финансы)
def generate_payroll_sheet(name: str) -> BytesIO:
    """Формирует PNG-ведомость для сотрудника по имени."""
    df = cached_data['payroll_df']
    if df is None:
        raise ValueError("Финансовые данные не загружены")
    df_user = df[df['Employee'] == name].copy()
    # Обобщённые колонки: 'Amount', 'Adjustments', 'Total'
    total = df_user['Amount'].sum()
    summary_row = pd.DataFrame([{
        'Date': '', 'Employee': '', 'Location': '', 'Revenue': '',
        'Adjustments': 'ИТОГО', 'Amount': total, 'Comments': ''
    }])
    df_user = pd.concat([df_user, summary_row], ignore_index=True)
    return dataframe_to_image(df_user)

#Проверка существования сотрудника (декоратор)
def check_employee_exists(location_key: str):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            schedule_df = cached_data.get('schedule_df')
            if schedule_df is None:
                return
            try:
                today = (datetime.datetime.now() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
                employee = schedule_df.loc[today, location_key]
                if pd.isna(employee) or employee not in get_all_work_username():
                    return
                return await func(*args, **kwargs)
            except (KeyError, TypeError):
                return
        return wrapper
    return decorator

#Утилиты для расписания
def get_dates_from_column(df: pd.DataFrame, column_name: str) -> list:
    """Извлекает список дат из колонки таблицы."""
    return df[column_name].dropna().astype(str).tolist()

# === ХЕНДЛЕРЫ: ВХОД И ДОСТУП ===

@dp.message_handler(commands=['start'], state="*")
async def start(message: types.Message, state: FSMContext):
    """Обработка команды /start."""
    user_id = message.from_user.id
    if user_verification(user_id):
        username = get_username(user_id)
        if get_admin(user_id):
            await message.reply(f'Привет, {username}! Я помогаю отслеживать операционные процессы.', reply_markup=keyboard_admin)
        else:
            await message.reply(f'Привет, {username}! Чем могу помочь?', reply_markup=keyboard_user)
    else:
        await message.reply('Доступ временно ограничен.', reply_markup=keyboard_request_access)

# === ХЕНДЛЕРЫ: ГЕОЛОКАЦИЯ И СМЕНЫ ===

@dp.callback_query_handler(lambda c: c.data.startswith('start_work'), state="*")
async def start_work(callback_query: types.CallbackQuery, state: FSMContext):
    """Инициация смены: запрос геолокации."""
    user_id = callback_query.from_user.id
    if not user_verification(user_id):
        return await callback_query.answer("Доступ запрещён", show_alert=True)
    sent = await bot.send_message(user_id, 'Отправьте вашу геолокацию для подтверждения начала смены', reply_markup=keyboard_send_location)
    edit_message(user_id, sent.message_id)
    await Form.get_location_st.set()

# (остальные хендлеры — location, point_name, who_work_today — оставлены без изменений в логике,
# но строки сообщений обезличены: вместо "Новокосино" → "Location A", вместо "вафли" → "Product Group")

# === ХЕНДЛЕРЫ: УПРАВЛЕНИЕ ПЕРСОНАЛОМ ===

@dp.callback_query_handler(lambda c: c.data.startswith('worker_list'), state="*")
async def manage_staff(callback_query: types.CallbackQuery, state: FSMContext):
    """Меню управления сотрудниками."""
    # ... реализация без упоминания реальных имён/точек

# === ХЕНДЛЕРЫ: ФИНАНСОВАЯ ОТЧЁТНОСТЬ ===

@dp.callback_query_handler(lambda c: c.data.startswith('payroll_sheets'), state="*")
async def send_payroll_sheets(callback_query: types.CallbackQuery, state: FSMContext):
    """Генерация и отправка PNG-табелей."""
    # ... использует generate_payroll_sheet(name) → обезличена

# === ХЕНДЛЕРЫ: FAQ И СПРАВОЧНАЯ СИСТЕМА ===

@dp.callback_query_handler(lambda c: c.data.startswith('question'), state="*")
async def faq_menu(callback_query: types.CallbackQuery, state: FSMContext):
    """Открытие категорий FAQ."""
    # ... категории обезличены: вместо "Штрафы" → "Operational Rules"

# === ПЛАНИРОВЩИК ЗАДАЧ (CRON) ===

#Утренние напоминания
def make_start_shift_reminder(location_key: str):
    @check_employee_exists(location_key)
    async def handler():
        schedule_df = cached_data['schedule_df']
        today = (datetime.datetime.now() + datetime.timedelta(hours=3)).strftime('%d.%m.%Y')
        chat_id = get_user_id(schedule_df.loc[today, location_key])
        msg = "Доброе утро! Начало смены через 10 минут.\nНе забудьте отправить геолокацию."
        await bot.send_message(chat_id, msg)
        # Условные напоминания (без бизнес-деталей)
        if datetime.datetime.now().weekday() in (0, 3):
            await bot.send_message(chat_id, "Сегодня: заполнить чек-лист до 13:00", reply_markup=kb_checklist)
    return handler

#Вечерние напоминания
def make_end_shift_reminder(location_key: str):
    @check_employee_exists(location_key)
    async def handler():
        # ... аналогично
    return handler

#Техобслуживание (maintenance)
def make_maintenance_reminder(location_key: str):
    @check_employee_exists(location_key)
    async def handler():
        # ... обобщённые напоминания о графике техобслуживания
    return handler

#Контроль отклонений (опоздания)
def make_absence_alert(location_key: str, location_name: str):
    @check_employee_exists(location_key)
    async def handler():
        # ... оповещает админа, если геолокация не получена через 10 минут
    return handler

#Контроль сроков годности (Product Group)
def product_expiry_monitor():
    @check_employee_exists()
    async def handler():
        # ... обобщённые сообщения: "Через 3 дня истекает срок годности Product Group 1"
    return handler

#Регистрация задач в планировщике
async def schedule_jobs():
    """Настройка всех cron-задач для точек A–H."""
    scheduler = AsyncIOScheduler(timezone='Europe/Moscow')
    scheduler.add_job(product_expiry_monitor(), 'cron', hour=18, minute=0)
    for key in LOCATION_ALIASES.keys():
        scheduler.add_job(make_start_shift_reminder(key), 'cron', hour=8, minute=50)
        scheduler.add_job(make_end_shift_reminder(key), 'cron', hour=20, minute=50)
        scheduler.add_job(make_maintenance_reminder(key), 'cron', hour=9, minute=30)
        scheduler.add_job(make_absence_alert(key, LOCATION_CODES[key]), 'cron', hour=9, minute=10)
    scheduler.start()

#Фоновое обновление кэша
async def cache_updater():
    """Обновляет кэш каждые 3 часа."""
    while True:
        await update_cached_data()
        await asyncio.sleep(60 * 180)

# === ЗАПУСК ПРИЛОЖЕНИЯ ===

async def on_startup(_):
    """Действия при старте бота."""
    init_db()
    db_on()
    db_message()
    init_changes_db()
    asyncio.create_task(cache_updater())
    await schedule_jobs()

if __name__ == '__main__':
    logger.info("Запуск Telegram-бота...")
    executor.start_polling(dp, on_startup=on_startup, skip_updates=True)
