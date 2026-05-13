import asyncio, sqlite3, logging, random, time, tired, injured, types, recovery, io, clubs, academy, os
from collections import deque
from datetime import timedelta, datetime
from clubs import CLUBS
from typing import Union # Чтобы не было ошибок с типами
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram import types
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup
from aiogram.exceptions import TelegramBadRequest
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery
from typing import Callable, Dict, Any, Awaitable
from database import get_db
from recovery import process_recovery
from aiogram.filters import StateFilter
from aiogram.types import TelegramObject, CallbackQuery
from typing import Callable, Dict, Any, Awaitable
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram import Router
from aiogram.exceptions import TelegramRetryAfter, TelegramForbiddenError
from balances import TEAM_BALANCES
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter, IS_NOT_MEMBER, MEMBER
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import re
import datetime as dt
from leaks import generate_locker_room_action
from itertools import combinations
from academy import restore_training_tasks
from admin_awards import router as awards_route
from interviews import router as interviews_router, start_interview, check_scandal_event
from pytz import timezone
from interviews import router as interview_router

scheduler = AsyncIOScheduler()
last_messages = {}
spam_tracker = {}

processing_catches = set()
already_caught = set()
router = Router()
broadcast_active = set() 

class AdminMoney(StatesGroup):
    waiting_for_amount = State()

class AwardStates(StatesGroup):
    choosing_type = State()
    choosing_user = State()  
    entering_data = State()

class ManualMatch(StatesGroup):
    waiting_for_title = State()
    selecting_t1 = State()
    selecting_t2 = State()  

class MarketStates(StatesGroup):
    waiting_for_sell_price = State() 
    waiting_for_bid_price = State()  
    waiting_for_trade_player = State() 
    waiting_for_trade_cash = State()
    waiting_for_loan_type = State()
    waiting_for_buyout_price = State()
    waiting_for_loan_duration = State()
    waiting_for_loan_fee = State()
    waiting_for_exchange_money = State()  
    setting_price = State()

class CasinoStates(StatesGroup):
    waiting_for_bet = State()

class AdminMarketStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_rating = State()
    waiting_for_pos = State()
    waiting_for_price = State()

class AdminEditStates(StatesGroup):
    target_p_id = State()
    waiting_for_new_name = State()
    waiting_for_new_rat = State()
    waiting_for_new_pos = State() 

class LoanStates(StatesGroup):
    waiting_for_type = State()
    waiting_for_buyout_price = State()
    waiting_for_duration = State()
    waiting_for_loan_fee = State()

class AdminUpgrade(StatesGroup):
    waiting_for_club = State()
    waiting_for_player = State()
    waiting_for_amount = State()

class AdminStates(StatesGroup):
    waiting_for_fa_name = State()
    waiting_for_fa_rat = State()
    waiting_for_fa_pos = State()
    waiting_for_fa_price = State()
    target_id = State()
    player_data = State()
    waiting_for_season_name = State()

class GameStates(StatesGroup):
    choosing_club = State()
    setting_price = State()

class TradeStates(StatesGroup):
    waiting_for_cash = State()
    waiting_for_trade_player = State()

class MatchStates(StatesGroup):
    live = State()
    half_time = State()
    waiting_for_loan_price = State()


def increment_season(season_str):
    try:
        # Разбиваем "25/26" на ["25", "26"]
        start, end = map(int, season_str.split('/'))
        # Превращаем в "26/27"
        return f"{start + 1}/{end + 1}"
    except:
        # Если вдруг формат сбился, вернем дефолт
        return "26/27"

async def check_ownership(cb: types.CallbackQuery, player_id):
    """Универсальная проверка: если игрок не твой — вернет False и покажет алерт"""
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id FROM squad WHERE id = ?', (player_id,))
    row = c.fetchone()
    conn.close()
    if not row or int(row[0]) != cb.from_user.id:
        await cb.answer("🚫 Это не твой игрок! Нельзя трогать чужой контент.", show_alert=True)
        return False
    return True

def init_db():   
    # Подключаемся с таймаутом и включаем WAL для стабильной работы
    conn = sqlite3.connect('game.db', timeout=30)
    c = conn.cursor()
    c.execute('PRAGMA journal_mode=WAL;')

    # --- ВСПОМОГАТЕЛЬНАЯ ФУНКЦИЯ ДЛЯ МИГРАЦИЙ ---
    def add_column(table, col_name, col_type):
        try:
            c.execute(f"PRAGMA table_info({table})")
            columns = [col[1] for col in c.fetchall()]
            if col_name not in columns:
                c.execute(f'ALTER TABLE {table} ADD COLUMN {col_name} {col_type}')
                print(f"✅ Колонка {col_name} добавлена в {table}")
        except Exception as e:
            print(f"❌ Ошибка миграции {col_name} в {table}: {e}")

    # 1. ОСНОВНЫЕ ТАБЛИЦЫ (USERS & SQUAD)
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        full_name TEXT,
        club TEXT,
        balance INTEGER DEFAULT 100,
        formation TEXT DEFAULT "4-3-3",
        wins INTEGER DEFAULT 0, draws INTEGER DEFAULT 0, losses INTEGER DEFAULT 0,
        goals_scored INTEGER DEFAULT 0, assists INTEGER DEFAULT 0,
        yellow_cards INTEGER DEFAULT 0, red_cards INTEGER DEFAULT 0,
        daily_catch INTEGER DEFAULT 0,
        last_match TEXT, last_recovery TEXT,
        chat_id INTEGER,
        league_wins INTEGER DEFAULT 0, league_draws INTEGER DEFAULT 0, 
        league_losses INTEGER DEFAULT 0, league_goals INTEGER DEFAULT 0,
        casino_loss INTEGER DEFAULT 0,
        tactic TEXT DEFAULT "Тики-така",
        captain_id INTEGER DEFAULT NULL,
        penalty_id INTEGER DEFAULT NULL,
        freekick_id INTEGER DEFAULT NULL,
        league_group TEXT DEFAULT "A"
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS squad (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        player_name TEXT,
        rating INTEGER,
        pos TEXT,
        status TEXT DEFAULT "bench",
        slot_id INTEGER DEFAULT NULL,
        market_price INTEGER DEFAULT 0,
        goals INTEGER DEFAULT 0, assists INTEGER DEFAULT 0,
        yellow_cards INTEGER DEFAULT 0, red_cards INTEGER DEFAULT 0,
        is_banned INTEGER DEFAULT 0,
        stamina INTEGER DEFAULT 100, 
        fatigue INTEGER DEFAULT 0,
        injury_type TEXT DEFAULT NULL,
        injury_remaining INTEGER DEFAULT 0,
        chat_id INTEGER,
        club TEXT,
        original_owner_id INTEGER DEFAULT NULL,
        loan_expires_window INTEGER DEFAULT 0,
        loan_to INTEGER DEFAULT NULL,
        loan_term INTEGER DEFAULT 0,
        training_until TEXT DEFAULT NULL,
        mvp_stats INTEGER DEFAULT 0
    )''')

    # 2. ТУРНИРНЫЕ ТАБЛИЦЫ (CUP & LEAGUE)
    c.execute("DROP TABLE IF EXISTS cup_bracket")
    c.execute('''CREATE TABLE cup_bracket (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stage TEXT, 
        t1_id INTEGER, t1_name TEXT,
        t2_id INTEGER, t2_name TEXT,
        winner_id INTEGER DEFAULT NULL,
        winner_name TEXT DEFAULT "Неизвестно",
        h_score INTEGER DEFAULT 0, a_score INTEGER DEFAULT 0,
        h_pen INTEGER DEFAULT NULL, a_pen INTEGER DEFAULT NULL,
        first_leg_score TEXT DEFAULT NULL,
        status TEXT DEFAULT "pending"
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS league_schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        home_id INTEGER, away_id INTEGER, 
        tour_number INTEGER, 
        status TEXT DEFAULT "pending",
        is_cup_break INTEGER DEFAULT 0,
        league_group TEXT DEFAULT "A"
    )''')

    # Таблица для Лиги Чемпионов
    c.execute('''CREATE TABLE IF NOT EXISTS ucl_bracket (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        t1_id INTEGER,
        t1_name TEXT,
        t2_id INTEGER,
        t2_name TEXT,
        winner_id INTEGER,
        res1 TEXT,
        res2 TEXT,
        h_p INTEGER,
        a_p INTEGER,
        stage TEXT
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS ucl_stats (
        player_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        goals INTEGER DEFAULT 0,
        assists INTEGER DEFAULT 0,
        yellow_cards INTEGER DEFAULT 0,
        red_cards INTEGER DEFAULT 0
    )''')

    # Таблица для выставленных на рынок игроков
    c.execute('''CREATE TABLE IF NOT EXISTS market_lots (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_id INTEGER,
        player_name TEXT,
        rating INTEGER,
        price INTEGER,
        club TEXT,
        user_id INTEGER,
        pos TEXT,
        status TEXT DEFAULT "sale"
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS cup_schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stage TEXT, match_date TEXT,
        status TEXT DEFAULT "pending"
    )''')

    c.execute('CREATE TABLE IF NOT EXISTS league_participants (user_id INTEGER PRIMARY KEY)')
    c.execute('CREATE TABLE IF NOT EXISTS cup_participants (user_id INTEGER PRIMARY KEY)')

    # 3. СТАТИСТИКА И ЖУРНАЛЫ
    c.execute('''CREATE TABLE IF NOT EXISTS league_stats (
        player_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        goals INTEGER DEFAULT 0, assists INTEGER DEFAULT 0,
        yellow_cards INTEGER DEFAULT 0, red_cards INTEGER DEFAULT 0
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS academy_stats (
    user_id INTEGER PRIMARY KEY, 
    stars_sold INTEGER DEFAULT 0
    )''')

    try:
        c.execute('ALTER TABLE academy_candidates ADD COLUMN c4_data TEXT')
    except:
        pass 

    c.execute('''CREATE TABLE IF NOT EXISTS transfer_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        player_name TEXT, buyer_club TEXT,
        price INTEGER, date TEXT
    )''')

    c.execute("DROP TABLE IF EXISTS msg_stats")
    c.execute('''CREATE TABLE msg_stats (
        user_id INTEGER PRIMARY KEY, 
        full_name TEXT, msg_count INTEGER, 
        last_reset DATE
    )''')

    # 4. СИСТЕМНЫЕ (SETTINGS, ACADEMY, PUNISHMENTS)
    c.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT)')

    c.execute('''CREATE TABLE IF NOT EXISTS academy_players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, name TEXT, position TEXT,
        ovr INTEGER, potential INTEGER,
        trainings_completed INTEGER DEFAULT 0,
        next_training_finish DATETIME, spawn_date DATETIME
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS punishments (
        user_id INTEGER, full_name TEXT, 
        type TEXT, reason TEXT, 
        until_date DATETIME, admin_id INTEGER
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS hall_of_fame (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, player_name TEXT,
        achievement_type TEXT, date_awarded DATETIME
    )''')

    c.execute('''CREATE TABLE IF NOT EXISTS sold_originals (
        club_name TEXT, player_name TEXT,
        UNIQUE(club_name, player_name)
    )''')

    # --- ИНИЦИАЛИЗАЦИЯ НАСТРОЕК ---
    default_settings = [
        ("current_season", "25/26"),
        ("transfer_window", "0"),
        ("current_half", "1"),
        ("window_counter", "1"),
        ("main_chat_id", "0")
    ]
    for key, val in default_settings:
        c.execute('INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)', (key, val))

    # --- МИГРАЦИИ (Если база уже создана) ---
    # Squad миграции
    for col in ['club', 'original_owner_id', 'is_banned', 'injury_type', 'training_until']:
        add_column('squad', col, 'TEXT' if 'type' in col or 'until' in col or 'club' in col else 'INTEGER')
    
    add_column('squad', 'fatigue', 'INTEGER DEFAULT 0')
    add_column('squad', 'mvp_stats', 'INTEGER DEFAULT 0')
    
    # Users миграции
    add_column('users', 'casino_loss', 'INTEGER DEFAULT 0')
    add_column('users', 'tactic', 'TEXT DEFAULT "Тики-така"')
    add_column('users', 'league_group', 'TEXT DEFAULT "A"')

    add_column('academy_players', 'potential', 'INTEGER DEFAULT 80')
    add_column('academy_players', 'start_ovr', 'INTEGER DEFAULT 0')
    add_column('academy_players', 'trainings_left', 'INTEGER DEFAULT 5')
    add_column('academy_players', 'last_spawn_date', 'DATETIME')
    add_column('squad', 'block_offers', 'INTEGER DEFAULT 0')
    add_column('squad', 'price', 'INTEGER DEFAULT 0')

    conn.commit()
    conn.close()
    print("✅ БАЗА ДАННЫХ РАБОТАЕТ!")

def manual_migration():
    conn = sqlite3.connect('game.db') 
    c = conn.cursor()
    try:
        c.execute('ALTER TABLE users ADD COLUMN casino_loss INTEGER DEFAULT 0')
        conn.commit()        
    except sqlite3.OperationalError:
        conn.close()

# Вызови это перед запуском бота
manual_migration()

academy.init_academy_db()

# --- КЛАВИАТУРЫ ---
def get_main_kb(user_id: int):
    b = ReplyKeyboardBuilder()
    
    # 4 главных хаба
    b.button(text="👤 Мой Клуб")
    b.button(text="🏟 Игровой Центр")
    b.button(text="🏆 Турниры")
    b.button(text="💰 Рынок & Баланс")
    
    # Кнопка админа снизу, если нужно
    if user_id in ADMINS: 
        b.button(text="🛠 Админка")
        
    b.adjust(2, 2, 1) # Сетка 2-2 и админка снизу во всю ширину
    return b.as_markup(resize_keyboard=True)
    
# --- МЕХАНИКА ЖЕСТКОГО ЛИМИТА ---
class CatchLimitMiddleware(BaseMiddleware):
    def __init__(self):
        # Список тех, кто в процессе или уже забрал
        self.already_caught = set()

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        if isinstance(event, CallbackQuery) and event.data.startswith("catch_"):
            user_id = event.from_user.id
            
            # 1. Проверяем мгновенно
            if user_id in self.already_caught:
                return await event.answer("🚫 Лимит: 1 игрок за выброс!", show_alert=True)
            
            # 2. БЛОКИРУЕМ СРАЗУ (до выполнения логики)
            self.already_caught.add(user_id)
            
            try:
                result = await handler(event, data)
                # Если handler вернул что-то, что означает "не купил" 
                # (например, False), можно здесь сделать remove, но лучше 
                # оставить жесткий лимит на попытку нажатия.
                return result
            except Exception as e:
                # Если произошла системная ошибка, даем шанс нажать еще раз
                if user_id in self.already_caught:
                    self.already_caught.remove(user_id)
                raise e
            
        return await handler(event, data)
    
class ThrottlingMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: TelegramObject, data):
        try:
            return await handler(event, data)
        except TelegramRetryAfter as e:
            # Если словили флуд, бот спит столько, сколько просит сервер
            await asyncio.sleep(e.retry_after)
            return await handler(event, data) # Повторная попытка

# @router.callback_query()
# async def handle_all_callbacks(callback: types.CallbackQuery):
#     # Проверяем, есть ли двоеточие (для тактики и прочего)
#     if ":" in callback.data:
#         data_parts = callback.data.split(":")
#         owner_id_str = data_parts[-1]
#         if owner_id_str.isdigit():
#             if callback.from_user.id != int(owner_id_str):
#                 await callback.answer("Это не твой состав! ❌", show_alert=True)
#                 return

# Создаем экземпляр, чтобы к нему можно было обращаться из админки
limit_manager = CatchLimitMiddleware()

# --- КОНФИГ ---
TOKEN = "8784991908:AAGdZ5mcIfc1nW0u77-HuwkN-Ym9eJpaR5U"
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())
dp.callback_query.outer_middleware(limit_manager)
ADMINS = [5611356552]
MODERS = []
SET_CHAT_ID = -1003513118924  
CHAT_ID = 5611356552    
# -1003556034012, - тест чат
# -1003345980096 -нищ лига
# -5137303209 - моя лига
# 5611356552 - Я

from aiogram.client.session.aiohttp import AiohttpSession

# Создаем сессию с указанием прокси PythonAnywhere
session = AiohttpSession(proxy="http://proxy.server:3128")

# Инициализируем бота с этой сессией
bot = Bot(token=TOKEN, session=session)

matches_data = {}

def get_club_kb():
    b = ReplyKeyboardBuilder()
    b.button(text="📋 Состав"); b.button(text="📋 Весь состав")
    b.button(text="📦 Вне состава"); b.button(text="🏋️‍♂️ Отправить на тренировку")
    b.button(text="⬅️ Назад")
    b.adjust(2, 2, 1)
    return b.as_markup(resize_keyboard=True)

def get_games_kb():
    b = ReplyKeyboardBuilder()
    b.button(text="⚽️ Играть (Бот)"); b.button(text="📝 Записаться в Лигу")
    b.button(text="📅 Мои матчи"); b.button(text="🏫 Академия")
    b.button(text="⬅️ Назад")
    b.adjust(2, 2, 1)
    return b.as_markup(resize_keyboard=True)

def get_tournaments_kb():
    b = ReplyKeyboardBuilder()
    b.button(text="🏆 Таблица"); b.button(text="📊 Сетки Турниров"); b.button(text="📜 История сезонов")
    b.button(text="📊 Статистика"); b.button(text="🏛 Зал Славы")
    b.button(text="⬅️ Назад")
    b.adjust(3, 2, 1)
    return b.as_markup(resize_keyboard=True)

@dp.message(F.text == "👤 Мой Клуб")
async def open_club(m: types.Message):
    await m.answer("Управление клубом:", reply_markup=get_club_kb())

@dp.message(F.text == "🏟 Игровой Центр")
async def open_games(m: types.Message):
    await m.answer("Игровой центр:", reply_markup=get_games_kb())

@dp.message(F.text == "🏆 Турниры")
async def open_tourneys(m: types.Message):
    await m.answer("Турнирные таблицы:", reply_markup=get_tournaments_kb())

@dp.message(F.text == "⬅️ Назад")
async def go_back_main(m: types.Message):
    await m.answer("Главное меню:", reply_markup=get_main_kb(m.from_user.id))

@dp.message(F.text == "📊 Сетки Турниров")
async def show_tournament_choice(m: types.Message):
    builder = InlineKeyboardBuilder()
    # ВАЖНО: callback_data должна быть указана!
    builder.button(text="🏆 Кубок", callback_data="show_grid_cup")
    builder.button(text="🇪🇺 Лига Чемпионов", callback_data="show_grid_ucl")
    builder.adjust(2)
    
    await m.answer(
        "<b>Выберите турнир для просмотра сетки:</b>", 
        reply_markup=builder.as_markup(), 
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "show_grid_ucl")
async def show_ucl_grid(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    
    c.execute('SELECT value FROM settings WHERE key = "current_season"')
    season_label = c.fetchone()[0]

    # Тянем данные из твоей новой таблицы ЛЧ
    c.execute('''SELECT stage, t1_name, t2_name, winner_id, res1, res2 
                 FROM ucl_bracket 
                 ORDER BY CASE stage 
                    WHEN '1/8' THEN 1 WHEN '1/4' THEN 2 
                    WHEN '1/2' THEN 3 WHEN 'Финал' THEN 4 END''')
    rows = c.fetchall()
    conn.close()

    if not rows:
        return await cb.answer("🇪🇺 ЛЧ еще не сформирована или начнется со 2-го сезона.", show_alert=True)

    res = f"🇪🇺 <b>ЛИГА ЧЕМПИОНОВ | СЕЗОН {season_label}</b>\n"
    res += "————————————————————\n"
    
    current_stage = ""
    for r in rows:
        stage, t1, t2, w_id, r1, r2 = r
        if stage != current_stage:
            res += f"\n🔹 <b>{stage.upper()}</b>\n"
            current_stage = stage

        # Логика отображения счета ЛЧ (двухматчевая)
        if w_id:
            score_str = f"({r1} | {r2})" if r2 else f"{r1}"
            res += f"🏁 {t1} <b>{score_str}</b> {t2}\n"
        else:
            res += f"⏳ {t1} vs {t2}\n"

    await cb.message.edit_text(res, parse_mode="HTML")
    await cb.answer()

# --- ОБНОВЛЕННАЯ, КРАСИВАЯ РАССЫЛКА С ФОТО ---
# Мы используем F.text, чтобы поймать именно команду "!рассылка_вик"
# Можешь изменить "!рассылка_вик" на любую удобную команду.
@dp.message(lambda m: m.text and m.text.strip() == "!рассылка_вик")
async def send_pretty_quiz_broadcast(m: types.Message):
    if m.from_user.id not in ADMINS:
        return

    # Путь к файлу
    photo_path = "quiz_image.png"

    # Проверяем наличие файла сразу, чтобы не начинать рассылку впустую
    if not os.path.exists(photo_path):
        return await m.answer(f"❌ Файл {photo_path} не найден!")

    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(
        text="Да ✅, конечно!", 
        url="https://t.me/NorthDivisionLeague")
    )
    builder.row(types.InlineKeyboardButton(
        text="Нет ❌, в другой раз", 
        url="https://t.me/NorthDivisionLeague")
    )

    broadcast_text = (
        "⚽ Хотите викторину на топ-клуб?\n\n"
        "Готовы проверить свои знания?"
    )

    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT user_id FROM users')
    users = c.fetchall()
    conn.close()

    if not users:
        return await m.answer("✅ База данных пользователей пуста.")

    await m.answer(f"🚀 Начинаю рассылку для {len(users)} чел...")

    count = 0
    for user in users:
        try:
            # ВАЖНО: Создаем объект фото ВНУТРИ цикла для каждого юзера
            # Это решает проблему ClientOSError [Errno 2]
            current_photo = types.FSInputFile(photo_path)
            
            await bot.send_photo(
                chat_id=user[0],
                photo=current_photo,
                caption=broadcast_text,
                reply_markup=builder.as_markup(),
                parse_mode="HTML"
            )
            count += 1
            await asyncio.sleep(0.06) 
        except Exception as e:
            print(f"DEBUG: Не удалось отправить {user[0]}: {e}")

    await m.answer(f"✅ Рассылка завершена! Получили: {count} чел.", parse_mode="HTML")

# Отдельно для Баланса и Рынка (сразу результат)
@dp.message(F.text == "💰 Рынок & Баланс")
async def show_balance_market(m: types.Message):
    uid = m.from_user.id
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT balance FROM users WHERE user_id = ?", (uid,))
    res = c.fetchone()
    balance = res[0] if res else 0
    conn.close()
    
    # ТУТ ДЕЛИМ НА ЛЯМ
    balance_in_m = balance / 1_000_000
    
    kb = ReplyKeyboardBuilder()
    kb.button(text="🚀 Рынок")
    kb.button(text="⬅️ Назад")
    kb.adjust(1)
    
    await m.answer(
        f"💰 Ваш баланс: <b>{balance_in_m:.1f} млн €</b>\n"
        f"Чтобы зайти на рынок, жми кнопку ниже",
        reply_markup=kb.as_markup(resize_keyboard=True),
        parse_mode="HTML"
    )



@dp.message(F.new_chat_members)
async def welcome_new_member_service(message: types.Message):
    if message.chat.id != SET_CHAT_ID:
        return
    
    for user in message.new_chat_members:
        if user.username:
            mention = f"@{user.username}"
        else:
            mention = f"<a href='tg://user?id={user.id}'>{user.first_name}</a>"
        
        text = (
            f"⚡️ <b>НОВОЕ ПОПОЛНЕНИЕ: {mention}</b>\n"
            f"————————————————————\n"
            f"Добро пожаловать в <b>NORTH DIVISION</b>. Здесь не играют в футбол — здесь за него сражаются. "
            f"Твой путь начинается с этого момента.\n\n"
            f"📍 <b>ПЕРВЫМ ДЕЛОМ:</b>\n"
            f"Напиши в чат команду <code>!хелп</code> — там собраны все инструменты управления твоим штабом и составом. "
            f"Изучи её внимательно, чтобы не остаться на скамейке запасных.\n\n"
            f"🛡 <b>ПРАВИЛО ДИВИЗИОНА:</b>\n"
            f"Дисциплина — твой главный союзник. Рынок не прощает ошибок, а лига не терпит слабых.\n\n"
            f"🤝 <b>Связь с командованием:</b> @North_Officail\n"
            f"————————————————————\n"
            f"<i>Вводи <code>!хелп</code> и приступай к работе. Удачи.</i>"
        )
        
        await message.answer(text, parse_mode="HTML")

# 1. Главное меню академии (по команде или кнопке)
@dp.message(F.text == "🏫 Академия")
async def open_academy_main_handler(m: types.Message):
    text, kb = await academy.get_academy_main(m.from_user.id)
    await m.answer(text, reply_markup=kb, parse_mode="HTML")

# 2. Обработка кнопки "Назад" в главное меню академии
@dp.callback_query(F.data == "open_academy_main")
async def back_to_academy_handler(cb: types.CallbackQuery):
    text, kb = await academy.get_academy_main(cb.from_user.id)
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

# 3. Список твоих игроков (Академия -> Моя Академия)
@dp.callback_query(F.data == "acad_list")
async def show_academy_list_handler(cb: types.CallbackQuery):
    text, kb = await academy.get_academy_list(cb.from_user.id)
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

# 4. Поиск регенов (Показывает 3-х кандидатов с учетом Репутации)
@dp.callback_query(F.data == "acad_search")
async def search_regens_handler(cb: types.CallbackQuery):
    text, kb = await academy.get_search_menu(cb.from_user.id)
    if kb:
        await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    else:
        await cb.answer(text, show_alert=True)

# 5. Выбор игрока (Когда нажал на кнопку с фамилией в поиске)
@dp.callback_query(F.data.startswith("acad_select_"))
async def select_candidate_handler(cb: types.CallbackQuery):
    await academy.process_select_candidate(cb)

# 6. Тренировка (Запуск таймера и расчет роста OVR)
@dp.callback_query(F.data.startswith("acad_train_"))
async def train_callback_handler(cb: types.CallbackQuery):
    await academy.process_start_train(cb)

# 7. Перевод в основу (Когда попытки тренировок закончились)
@dp.callback_query(F.data.startswith("acad_promote_"))
async def promote_to_squad_handler(cb: types.CallbackQuery):
    await academy.process_promote_to_squad(cb)

@dp.message(F.text == "!составы")
async def show_all_clubs_menu(m: types.Message):
    conn = get_db(); c = conn.cursor()
    # Берем только те клубы, у которых есть хотя бы один игрок
    c.execute('SELECT DISTINCT club FROM users WHERE club IS NOT NULL')
    clubs = [row[0] for row in c.fetchall()]
    conn.close()

    if not clubs:
        return await m.answer("📭 Пока ни один клуб не создан.")

    builder = InlineKeyboardBuilder()
    for club_name in clubs:
        builder.button(text=f"🏟 {club_name}", callback_data=f"view_club_{club_name}")
    
    builder.adjust(2) # Кнопки в два столбца
    await m.answer("🔍 <b>Выберите клуб для просмотра состава:</b>", 
                   reply_markup=builder.as_markup(), parse_mode="HTML")
    
@dp.callback_query(F.data.startswith("view_club_"))
async def view_other_club_squad(cb: types.CallbackQuery):
    club_name = cb.data.replace("view_club_", "")
    
    conn = get_db(); c = conn.cursor()
    # Находим владельца клуба
    c.execute('SELECT user_id, username FROM users WHERE club = ?', (club_name,))
    user_data = c.fetchone()
    
    if not user_data:
        conn.close()
        return await cb.answer("❌ Клуб не найден.", show_alert=True)
    
    target_uid, owner_name = user_data
    
    # Получаем игроков в старте (slot_id NOT NULL)
    c.execute('''SELECT slot_id, player_name, rating, stamina 
                 FROM squad WHERE user_id = ? AND slot_id IS NOT NULL 
                 ORDER BY slot_id ASC''', (target_uid,))
    players = c.fetchall()
    conn.close()

    if not players:
        return await cb.answer(f"⚠️ У клуба «{club_name}» пока нет игроков в стартовом составе.", show_alert=True)

    # Формируем текст как на скриншоте
    text = f"🏟 <b>{club_name}</b>\n"
    text += f"👤 Владелец: {owner_name}\n"
    text += "————————————————\n"
    text += "📋 <b>Стартовый состав:</b>\n"
    
    for slot, name, rat, stam in players:
        text += f"{slot}. {name} ({rat}) 🔋 {stam}%\n"

    await cb.message.edit_text(text, parse_mode="HTML", 
                               reply_markup=InlineKeyboardBuilder().button(text="⬅️ К списку", callback_data="show_clubs_back").as_markup())

# Доп. хендлер для кнопки назад
@dp.callback_query(F.data == "show_clubs_back")
async def back_to_clubs(cb: types.CallbackQuery):
    await cb.message.delete()
    await show_all_clubs_menu(cb.message)

# --- ГЛОБАЛЬНАЯ ОТМЕНА ---
# Ставим в самое начало, чтобы работала из любого состояния
@dp.message(StateFilter("*"), F.text.casefold() == "отмена")
async def global_cancel_handler(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        return await message.answer("❌ Сейчас нечего отменять.")
    
    await state.clear()
    await message.answer("🚫 Действие успешно отменено.", reply_markup=types.ReplyKeyboardRemove())

# --- ГЛАВНОЕ МЕНЮ !ИНФО ---
@dp.message(F.text.casefold() == "!инфо")
async def admin_info_clubs(m: types.Message):
    if m.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT DISTINCT club, user_id FROM users WHERE club IS NOT NULL')
    clubs = c.fetchall()
    conn.close()

    if not clubs:
        return await m.answer("📭 В лиге пока нет клубов.")

    kb = InlineKeyboardBuilder()
    for club_name, u_id in clubs:
        kb.button(text=f"🏟 {club_name}", callback_data=f"adm_edit_c_{u_id}")
    
    kb.adjust(2)
    await m.answer("🛠 <b>Админ-панель:</b> Выберите клуб:", 
                   reply_markup=kb.as_markup(), parse_mode="HTML")

# Назад к списку клубов
@dp.callback_query(F.data == "admin_info_back")
async def back_to_clubs_list(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await admin_info_clubs(cb.message)
    await cb.message.delete()
    await cb.answer()

@dp.message(F.text.lower() == "!казиктоп")
async def show_casino_top(m: types.Message):
    conn = get_db()
    c = conn.cursor()
    
    # 1. Берем топ-10 игроков
    c.execute('''SELECT username, casino_loss, club 
                 FROM users 
                 WHERE casino_loss > 0 
                 ORDER BY casino_loss DESC 
                 LIMIT 10''')
    rows = c.fetchall()
    
    # 2. Считаем общую сумму всех проигрышей в лиге (Total Depths)
    c.execute('SELECT SUM(casino_loss) FROM users')
    total_loss_raw = c.fetchone()[0] or 0
    
    conn.close()

    if not rows:
        return await m.answer("🎰 В казино пока пусто. Все ушли с деньгами!")

    text = "🎰 <b>ТОП СПОНСОРОВ NORTH CASINO</b>\n"
    text += "<i>Главные меценаты нашего заведения:</i>\n"
    text += "————————————————————\n"

    for i, (name, loss, club) in enumerate(rows, 1):
        # Если username пустой, пишем название клуба
        display_name = name if name else f"Клуб {club}"
        loss_mln = loss / 1_000_000
        
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        text += f"{medal} <b>{display_name}</b> — <code>{loss_mln:.1f} млн €</code>\n"

    # Добавляем подвал с общим итогом
    total_mln = total_loss_raw / 1_000_000
    text += "————————————————————\n"
    text += f"📉 <b>ВСЕГО ПРОИГРАНО:</b> <code>{total_mln:.1f} млн €</code>\n"
    text += "<i>Хочешь в топ? Крути !казик [сумма]</i>"

    await m.answer(text, parse_mode="HTML")

@dp.message(F.text.lower().startswith("!казик"))
async def gamble_game(m: types.Message):
    # 1. Разбираем аргументы
    args = m.text.split()
    uid = m.from_user.id
    
    if len(args) < 2:
        return await m.answer(
            "🎰 <b>NORTH CASINO</b>\n"
            "————————————————————\n"
            "Введите сумму ставки после команды.\n"
            "Пример: <code>!казик 10</code>", 
            parse_mode="HTML"
        )

    # 2. Проверка корректности числа
    try:
        # Убираем лишние символы, если юзер ввел "10млн" вместо "10"
        amount_raw = "".join(filter(str.isdigit, args[1]))
        if not amount_raw:
            return await m.answer("🚫 Введите сумму числом!")
            
        bet_input = int(amount_raw)
        bet = bet_input * 1_000_000  # Перевод в миллионы
        
        if bet <= 0:
            return await m.answer("🚫 Ставка должна быть больше нуля!")
    except ValueError:
        return await m.answer("🚫 Ошибка в формате суммы!")

    # 3. База данных
    conn = get_db()
    c = conn.cursor()
    
    try:
        c.execute('SELECT balance FROM users WHERE user_id = ?', (uid,))
        row = c.fetchone()
        
        if not row:
            return await m.answer("🚫 Вы не зарегистрированы!")
        
        balance = row[0]
        if balance < bet:
            return await m.answer(
                f"🚫 Недостаточно средств!\n"
                f"💰 Баланс: <b>{balance / 1_000_000:.1f} млн €</b>", 
                parse_mode="HTML"
            )

        # 4. Анимация (Твои шансы и твои анимации)
        status_msg = await m.answer("🎰 Ставка принята! Крутим барабаны...\n[ 🟥 🟥 🟥 ]")
        await asyncio.sleep(0.7)
        await status_msg.edit_text("🎰 Ставка принята! Крутим барабаны...\n[ 🟥 🟧 🟥 ]")
        await asyncio.sleep(0.7)
        await status_msg.edit_text("🎰 Ставка принята! Крутим барабаны...\n[ 🟧 🟨 🟧 ]")
        await asyncio.sleep(0.7)

        # 5. Шансы (20% на победу)
        is_win = random.choices([True, False], weights=[20, 80])[0]

        if is_win:
            c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (bet, uid))
            conn.commit()
            
            c.execute('SELECT balance FROM users WHERE user_id = ?', (uid,))
            new_balance = c.fetchone()[0]
            
            result_text = (
                f"💎 <b>ДЖЕКПОТ!</b> 💎\n"
                f"————————————————————\n"
                f"🎰 Результат: [ 🔔 🔔 🔔 ]\n"
                f"💰 Выигрыш: <b>+{bet_input} млн €</b>\n"
                f"🏦 Баланс: <b>{new_balance / 1_000_000:.1f} млн €</b>\n\n"
                f"<i>Норс поздравляет с выйгрышам</i>"
            )
        else:
            # ОБЯЗАТЕЛЬНО добавляем casino_loss = casino_loss + ?
            c.execute('''UPDATE users SET 
                         balance = balance - ?, 
                         casino_loss = casino_loss + ? 
                         WHERE user_id = ?''', (bet, bet, uid))
            conn.commit()
            
            # Получаем обновленный баланс для вывода
            c.execute('SELECT balance FROM users WHERE user_id = ?', (uid,))
            new_balance = c.fetchone()[0]
            
            result_text = (
                f"💀 <b>ПРОИГРЫШ</b> 💀\n"
                f"————————————————————\n"
                f"🎰 Результат: [ 🍋 🍒 🍫 ]\n"
                f"📉 Ты потерял: <b>-{bet / 1_000_000:.1f} млн €</b>\n"
                f"🏦 Остаток: <b>{new_balance / 1_000_000:.1f} млн €</b>\n\n"
                f"<i>Норс не прощает азарта...</i>"
            )

        await status_msg.edit_text(result_text, parse_mode="HTML")

    except Exception as e:
        print(f"Ошибка казика: {e}")
        await m.answer("⚠️ Техническая ошибка в казино.")
    finally:
        conn.close()

# --- СПИСОК ИГРОКОВ КЛУБА ---
@dp.callback_query(F.data.startswith("adm_edit_c_"))
async def adm_list_players(cb: types.CallbackQuery):
    owner_id = int(cb.data.split("_")[3])
    
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT id, player_name, rating, pos FROM squad WHERE user_id = ?', (owner_id,))
    players = c.fetchall()
    conn.close()

    kb = InlineKeyboardBuilder()
    if not players:
        text = "❌ В этом клубе пусто."
    else:
        text = "👤 <b>Выберите игрока для редактирования:</b>"
        for p_id, name, rat, pos in players:
            kb.button(text=f"{name} ({rat}) [{pos}]", callback_data=f"adm_mod_p_{p_id}")
    
    kb.button(text="⬅️ Назад к клубам", callback_data="back_to_clubs_list")
    kb.adjust(1)
    await cb.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    await cb.answer()

# --- МЕНЮ ДЕЙСТВИЙ С ИГРОКОМ ---
@dp.callback_query(F.data.startswith("adm_mod_p_"))
async def adm_player_actions(cb: types.CallbackQuery):
    p_id = int(cb.data.split("_")[3])
    
    # Ищем владельца, чтобы кнопка "Назад" вела к списку игроков этого клуба
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id FROM squad WHERE id = ?', (p_id,))
    res = c.fetchone()
    conn.close()
    owner_id = res[0] if res else 0

    kb = InlineKeyboardBuilder()
    kb.button(text="📝 Сменить Имя", callback_data=f"adm_field_name_{p_id}")
    kb.button(text="⭐ Докинуть Рейтинг", callback_data=f"adm_field_rat_{p_id}")
    kb.button(text="🏃 Изменить Позиции", callback_data=f"adm_field_pos_{p_id}")
    kb.button(text="🗑 Удалить игрока", callback_data=f"adm_del_p_{p_id}")
    
    # Возвращает к составу того же клуба
    kb.button(text="⬅️ К составу", callback_data=f"adm_edit_c_{owner_id}") 
    
    kb.adjust(1)
    await cb.message.edit_text("⚙️ <b>Редактирование:</b>\nВыберите параметр:", 
                               reply_markup=kb.as_markup(), parse_mode="HTML")
    await cb.answer()

# --- ЛОГИКА РЕДАКТИРОВАНИЯ (FSM) ---

# Имя
@dp.callback_query(F.data.startswith("adm_field_name_"))
async def edit_name_start(cb: types.CallbackQuery, state: FSMContext):
    p_id = int(cb.data.split("_")[3])
    await state.update_data(target_p_id=p_id)
    await state.set_state(AdminEditStates.waiting_for_new_name)
    await cb.message.answer("📝 Введите новое Имя и Фамилию (или напишите 'отмена'):")
    await cb.answer()

@dp.message(AdminEditStates.waiting_for_new_name)
async def edit_name_finish(m: types.Message, state: FSMContext):
    data = await state.get_data(); p_id = data['target_p_id']
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE squad SET player_name = ? WHERE id = ?', (m.text, p_id))
    conn.commit(); conn.close()
    await m.answer(f"✅ Имя изменено на: <b>{m.text}</b>", parse_mode="HTML")
    await state.clear()

# Рейтинг
@dp.callback_query(F.data.startswith("adm_field_rat_"))
async def edit_rat_start(cb: types.CallbackQuery, state: FSMContext):
    p_id = int(cb.data.split("_")[3])
    await state.update_data(target_p_id=p_id)
    await state.set_state(AdminEditStates.waiting_for_new_rat)
    await cb.message.answer("⭐ Введите новый рейтинг (1-99):")
    await cb.answer()

@dp.message(AdminEditStates.waiting_for_new_rat)
async def edit_rat_finish(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Введите число!")
    data = await state.get_data(); p_id = data['target_p_id']
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE squad SET rating = ? WHERE id = ?', (int(m.text), p_id))
    conn.commit(); conn.close()
    await m.answer(f"✅ Рейтинг обновлен до: <b>{m.text}</b>", parse_mode="HTML")
    await state.clear()

# --- ПОЗИЦИИ ---
@dp.callback_query(F.data.startswith("adm_field_pos_"))
async def edit_pos_start(cb: types.CallbackQuery, state: FSMContext):
    p_id = int(cb.data.split("_")[3])
    await state.set_state(AdminEditStates.waiting_for_new_pos)
    await state.update_data(target_p_id=p_id, adm_positions=[]) 
    await show_edit_position_selection(cb, state)
    await cb.answer()

async def show_edit_position_selection(message, state: FSMContext):
    data = await state.get_data()
    selected = data.get("adm_positions", [])
    kb = InlineKeyboardBuilder()
    for p in ["GK", "DEF", "MID", "FWD"]:
        kb.button(text=f"✅ {p}" if p in selected else p, callback_data=f"edit_toggle_{p}")
    if selected:
        kb.button(text=f"💾 Сохранить ({len(selected)})", callback_data="edit_pos_confirm")
    kb.adjust(2)
    current = "/".join(selected) if selected else "не выбраны"
    text = f"⚙️ <b>Позиции</b>\nТекущие: <b>{current}</b>"
    if isinstance(message, types.CallbackQuery):
        await message.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("edit_toggle_"), AdminEditStates.waiting_for_new_pos)
async def admin_edit_toggle_pos(cb: types.CallbackQuery, state: FSMContext):
    pos = cb.data.split("_")[2]
    data = await state.get_data(); selected = data.get("adm_positions", [])
    if pos in selected: selected.remove(pos)
    elif len(selected) < 3: selected.append(pos)
    else: return await cb.answer("🚨 Максимум 3!", show_alert=True)
    await state.update_data(adm_positions=selected)
    await show_edit_position_selection(cb, state)
    await cb.answer()

@dp.callback_query(F.data == "edit_pos_confirm", AdminEditStates.waiting_for_new_pos)
async def admin_edit_confirm_pos(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data(); p_id = data['target_p_id']
    new_pos = "/".join(data.get("adm_positions"))
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE squad SET pos = ? WHERE id = ?', (new_pos, p_id))
    conn.commit(); conn.close()
    await cb.message.edit_text(f"✅ Позиции изменены на: <b>{new_pos}</b>", parse_mode="HTML")
    await state.clear()
    await cb.answer()

# Удаление
@dp.callback_query(F.data.startswith("adm_del_p_"))
async def adm_delete_player(cb: types.CallbackQuery):
    p_id = int(cb.data.split("_")[3])
    conn = get_db(); c = conn.cursor()
    c.execute('DELETE FROM squad WHERE id = ?', (p_id,))
    conn.commit(); conn.close()
    await cb.message.edit_text("🗑 Игрок удален.")
    await cb.answer("Удалено", show_alert=True)

@dp.message(F.text == "!починить_трени")
async def fix_stuck_training(message: types.Message):
    # Если ты не уверен в списке ADMINS, временно закомментируй проверку ниже
    # if message.from_user.id not in ADMINS: return

    try:
        conn = get_db(); c = conn.cursor()
        
        # Используем наш импорт dt
        now_str = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Находим всех, у кого время вышло или зависло
        c.execute('SELECT id, player_name FROM squad WHERE training_until IS NOT NULL')
        players = c.fetchall()

        if not players:
            conn.close()
            return await message.answer("✅ В базе нет игроков на тренировке.")

        count = 0
        for pid, name in players:
            # Сбрасываем тренировку принудительно
            c.execute('UPDATE squad SET training_until = NULL, status = "bench" WHERE id = ?', (pid,))
            count += 1
        
        conn.commit(); conn.close()
        await message.answer(f"🛠 <b>Успешно!</b>\nВернул в строй игроков: {count} шт.", parse_mode="HTML")

    except Exception as e:
        await message.answer(f"❌ Произошла ошибка: {e}")

@dp.message(F.text.casefold() == "!правила")
async def cmd_rules(m: types.Message):
    rules = (
        "📜 <b>РЕГЛАМЕНТ NORTH DIVISION</b>\n"
        "————————————————————\n"
        "1. <b>Оскорбление родных</b> — Мут от 2 до 8 часов.\n"
        "2. <b>Спам/Флуд командами</b> — Мут от 30 мин до 2 часов (бот лагает!).\n"
        "3. <b>Абуз кнопок в чате</b> — Авто-мут 10 мин (юзай рынок в личке).\n"
        "4. <b>Оскорбление админов</b> — Мут на 12 часов или бан.\n"
        "5. <b>Реклама сторонних ресурсов</b> — Мут от 2 часов или бан.\n"
        "6. <b>Махинации с трансферами</b> — Обнуление состава или бан.\n"
        "7. <b>Неадекватное поведение/Токсичность</b> — Мут от 1 часа.\n"
        "8. <b>Мат в избыточном количестве</b> — Предупреждение, затем мут.\n"
        "9. <b>Попытки взлома/Багоюз</b> — Бан по ID навсегда.\n"
        "10. <b>Продажа аккаунтов/игроков</b> — Бан обеих сторон.\n"
        "11. <b>Контент 18+ (Порно/Шок)</b> — Бан или мут на 24 часа.\n"
        "12. <b>Разглашение личной информации</b> — Бан навсегда (доксинг запрещен).\n"
        "13. <b>Пропаганда/Дискриминация</b> — Мут от 12 часов.\n"
        "14. <b>Дезинформация/Клевета</b> — Мут от 2 часов.\n"
        "————————————————————\n"
        "<i>Незнание правил не освобождает от ответственности!</i>"
    )
    await m.answer(rules, parse_mode="HTML")
@dp.message(F.text == "!клубы")
async def show_all_clubs(message: types.Message):
    conn = get_db(); c = conn.cursor()
    
    c.execute('''
        SELECT DISTINCT u.username, u.user_id, u.club 
        FROM users u 
        WHERE u.club IS NOT NULL AND u.club != ''
        ORDER BY u.club ASC
    ''')
    rows = c.fetchall()
    conn.close()

    if not rows:
        return await message.answer("<b>🏟 Клубы еще не зарегистрированы.</b>", parse_mode="HTML")

    text = "<b>🏆 СПИСОК ВСЕХ КЛУБОВ:</b>\n\n"
    
    for username, uid, club_name in rows:
        if username:
            owner_display = f"@{username}"
        else:
            owner_display = f"Владелец клуба"
            
        mention = f'<a href="tg://user?id={uid}">{owner_display}</a>'
        
        text += f"⚽️ <b>{club_name}</b> — {mention}\n"

    await message.answer(text, parse_mode="HTML", disable_web_page_preview=True)

@dp.callback_query(F.data == "show_grid_cup")
async def show_cup_grid_callback(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    
    c.execute('SELECT value FROM settings WHERE key = "current_season"')
    res_season = c.fetchone()
    season_label = res_season[0] if res_season else "25/26"

    # Твой оригинальный SQL-запрос
    c.execute('''SELECT stage, t1_name, t2_name, winner_id, h_score, a_score, h_pen, a_pen 
                 FROM cup_bracket 
                 WHERE t1_name IS NOT NULL
                 ORDER BY CASE stage 
                    WHEN 'Play-In' THEN 1 WHEN '1/16' THEN 2 WHEN '1/8' THEN 3 
                    WHEN '1/4' THEN 4 WHEN '1/2' THEN 5 WHEN 'Финал' THEN 6 END''')
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        return await cb.answer(f"⏳ Сетка Кубка на сезон {season_label} еще не создана.", show_alert=True)
    
    res = f"🏆 <b>ТУРНИРНАЯ СЕТКА КУБКА | СЕЗОН {season_label}</b>\n"
    res += "————————————————————\n"
    
    current_stage = ""
    for r in rows:
        stage, t1, t2, w_id, h_s, a_s, h_p, a_p = r
        if stage != current_stage:
            res += f"\n🔹 <b>{stage.upper()}</b>\n"
            current_stage = stage
            
        if t1 and not t2:
            res += f"🏆 {t1} — <b>Авто-проход</b> ⏭\n"
            continue

        name2 = t2 if t2 else "???"
        if w_id: 
            score = f"{h_s}:{a_s}"
            if h_p is not None: score += f" ({h_p}:{a_p} пен.)"
            res += f"🏁 {t1} {score} {name2}\n"
        else:
            res += f"⏳ {t1} vs {name2}\n"
    
    # Кнопки навигации под сеткой
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🇪🇺 Перейти к ЛЧ", callback_query_data="show_grid_ucl")],
        [InlineKeyboardButton(text="⬅️ Назад к выбору", callback_query_data="back_to_tourney_choice")]
    ])
    
    await cb.message.edit_text(res, reply_markup=kb, parse_mode="HTML")

@dp.message(F.text == "📜 История сезонов")
async def show_season_history(m: types.Message):
    conn = get_db(); c = conn.cursor()
    
    c.execute('SELECT * FROM season_history ORDER BY id DESC')
    history = c.fetchall()
    conn.close()

    if not history:
        return await m.answer("📜 История сезонов пока пуста.\nСтаньте первым, кто впишет своё имя в летопись!")

    res = "📜 <b>ЛЕТОПИСЬ ЧЕМПИОНОВ</b>\n"
    res += "————————————————————\n"

    for h in history:
        s_name = h[1] if h[1] else "???"
        l_club = h[2] if h[2] else "Неизвестно"
        l_manager = h[3] if h[3] else "Аноним"
        c_club = h[4] if h[4] else "Неизвестно"
        c_manager = h[5] if h[5] else "Аноним"

        res += f"📅 <b>СЕЗОН {h[1]}</b>\n"
        res += f"🏆 <b>Лига:</b> {h[2]} (рук. {h[3]})\n"
        res += f"🎫 <b>Кубок:</b> {h[4]} (рук. {h[5]})\n"
        res += f"⚽️ <b>Бомбардир:</b> {h[7] if len(h)>7 else '—'}\n"
        res += f"🅰️ <b>Ассистент:</b> {h[8] if len(h)>8 else '—'}\n"
        res += "————————————————————\n"

    await m.answer(res, parse_mode="HTML")

async def training_done_callback(bot, user_id, player_id, old_rating):
    try:
        conn = get_db(); c = conn.cursor()
        # Проверяем, существует ли игрок
        c.execute('SELECT player_name, rating FROM squad WHERE id = ?', (player_id,))
        player = c.fetchone()
        
        if player:
            name, current_rating = player
            new_rating = current_rating + 1
            
            # ВАЖНО: Ставим status = NULL (или "bench"), очищаем training_until и ПОЛНОСТЬЮ убираем из training
            c.execute('''UPDATE squad 
                         SET rating = ?, 
                             training_until = NULL, 
                             status = "bench", 
                             slot_id = NULL 
                         WHERE id = ?''', (new_rating, player_id))
            conn.commit()
            
            text = (f"✅ <b>Тренировка завершена!</b>\n\n"
                    f"👤 <b>{name}</b> прибавил в мастерстве!\n"
                    f"📈 Рейтинг: {current_rating} ➡️ <b>{new_rating}</b>\n"
                    f"🏃 Игрок вернулся в распоряжение клуба.")
            try:
                await bot.send_message(user_id, text, parse_mode="HTML")
            except: pass
        conn.close()
    except Exception as e:
        print(f"Ошибка завершения тренировки: {e}")

async def restore_training_tasks(bot):
    conn = get_db(); c = conn.cursor()
    now = datetime.now()
    
    # Ищем всех, кто еще тренируется
    c.execute('SELECT id, user_id, player_name, training_until, rating FROM squad WHERE training_until IS NOT NULL')
    training_players = c.fetchall()
    
    for pid, uid, name, until_str, old_rat in training_players:
        try:
            # Важно: убедись, что формат совпадает с тем, что в confirm_tr
            until_dt = datetime.strptime(until_str, "%Y-%m-%d %H:%M:%S")
            
            if until_dt <= now:
                # Время уже вышло — завершаем немедленно
                await training_done_callback(bot, uid, pid, old_rat)
            else:
                # Время еще не вышло — переназначаем задачу
                # Проверяем, нет ли уже такой задачи в планировщике, чтобы не дублировать
                job_id = f"train_{pid}"
                if not scheduler.get_job(job_id):
                    scheduler.add_job(
                        training_done_callback,
                        'date',
                        run_date=until_dt,
                        args=[bot, uid, pid, old_rat],
                        id=job_id,
                        replace_existing=True
                    )
        except Exception as e:
            print(f"Ошибка при восстановлении игрока {pid}: {e}")
            
    conn.close()

@dp.message(F.text == "🏋️‍♂️ Отправить на тренировку")
async def training_selection_list(message: types.Message):

    if message.chat.id == -1003513118924:
        try:
            await message.delete() # Удаляем команду игрока
        except:
            pass
            
        warn = await message.answer(
            f"⚠️ {message.from_user.first_name}, тренировки проводятся только в личке бота!\n"
            f"Не забивай чат техническими меню."
        )
        
        # Удаляем предупреждение через 5-7 секунд
        await asyncio.sleep(7)
        try:
            await warn.delete()
        except:
            pass
        return

    user_id = message.from_user.id
    conn = get_db(); c = conn.cursor()
    
    # Берем тех, кто в клубе, не травмирован, не на рынке и не на тренировке
    c.execute('''SELECT id, player_name, rating, pos 
                 FROM squad 
                 WHERE user_id = ? AND training_until IS NULL AND injury_remaining = 0 AND status != 'on_sale'
                 ORDER BY rating DESC''', (user_id,))
    players = c.fetchall()
    conn.close()

    if not players:
        return await message.answer("📭 У вас нет доступных для тренировки игроков (все заняты или на рынке).")

    b = InlineKeyboardBuilder()
    for p in players:
        b.button(text=f"{p[1]} ({p[2]})", callback_data=f"train_pl_{p[0]}")
    
    b.adjust(1)
    await message.answer("🏋️‍♂️ <b>Выберите игрока для тренировки:</b>", reply_markup=b.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("train_pl_"))
async def confirm_training(cb: types.CallbackQuery):
    pid = cb.data.replace("train_pl_", "")
    conn = get_db(); c = conn.cursor()
    # Используем rating
    c.execute('SELECT player_name, rating FROM squad WHERE id = ?', (pid,))
    res = c.fetchone()
    conn.close()
    
    if not res: return
    name, rat = res

    # Считаем параметры для текста
    price = rat * 50000
    if rat < 60: hours = 0.5
    elif rat < 70: hours = 1
    elif rat < 75: hours = 2
    elif rat < 85: hours = 5
    elif rat < 90: hours = 9
    else: hours = 24

    text = (
        f"🏋️‍♂️ <b>ПОДТВЕРЖДЕНИЕ ТРЕНИРОВКИ</b>\n\n"
        f"👤 Игрок: <b>{name}</b>\n"
        f"📊 Улучшение: {rat} ➡️ {rat+1}\n"
        f"💰 Стоимость: <code>{price:,}</code> монет\n"
        f"⏳ Длительность: <b>{hours} ч.</b>\n\n"
        f"⚠️ <i>Игрок будет временно удален из состава и перемещен в лазарет!</i>"
    )
    
    b = InlineKeyboardBuilder()
    # Передаем данные. Важно: hours может быть float (0.5), поэтому передаем как строку
    b.row(types.InlineKeyboardButton(
        text="✅ Подтвердить и оплатить", 
        callback_data=f"confirm_tr_{pid}_{hours}_{price}")
    )
    b.row(types.InlineKeyboardButton(text="❌ Отмена", callback_data="back_to_field"))
    
    await cb.message.edit_text(text, reply_markup=b.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("confirm_tr_"))
async def process_training_payment(cb: types.CallbackQuery):
    data = cb.data.split("_")
    pid, hours, price = data[2], float(data[3]), int(data[4])
    uid = cb.from_user.id

    conn = get_db(); c = conn.cursor()
    
    # ЛИМИТ: 3 игрока. Считаем тех, у кого заполнено поле training_until
    c.execute('SELECT COUNT(*) FROM squad WHERE user_id = ? AND training_until IS NOT NULL', (uid,))
    count = c.fetchone()[0]
    
    if count >= 3:
        conn.close()
        return await cb.answer("❌ Лимит! Нельзя тренировать больше 3-х игроков одновременно.", show_alert=True)

    c.execute('SELECT balance FROM users WHERE user_id = ?', (uid,))
    user_bal = c.fetchone()
    if not user_bal or user_bal[0] < price:
        conn.close()
        return await cb.answer("❌ Недостаточно средств!", show_alert=True)

    # Устанавливаем время
    finish_dt = datetime.now() + timedelta(hours=hours)
    finish_time = finish_dt.strftime("%Y-%m-%d %H:%M:%S")

    # Списываем баланс
    c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (price, uid))
    
    # ОБНОВЛЯЕМ ИГРОКА: 
    # status = 'training' выкинет его из всех обычных списков состава
    # slot_id = NULL выкинет его из активной 11-ки (с поля)
    c.execute('''UPDATE squad SET 
                 status = "training", 
                 slot_id = NULL, 
                 training_until = ? 
                 WHERE id = ?''', (finish_time, pid))
    conn.commit(); conn.close()

    # Запускаем таймер
    scheduler.add_job(
        training_done_callback, 
        'date', 
        run_date=finish_dt, 
        args=[cb.bot, uid, pid, 0],
        id=f"train_{pid}", 
        replace_existing=True
    )

    await cb.message.edit_text(f"✅ Тренировка началась!\n🏃 Игрок покинул состав и вернется через {hours} ч.")

@dp.message(F.text.lower() == "!выйти")
async def quit_request(m: types.Message):
    uid = m.from_user.id
    conn = get_db(); c = conn.cursor()
    
    c.execute('SELECT club FROM users WHERE user_id = ?', (uid,))
    res = c.fetchone()
    
    if not res or not res[0]:
        conn.close()
        return await m.answer("❌ Ты не состоишь в клубе.")

    club_name = res[0]
    username = f"@{m.from_user.username}" if m.from_user.username else m.from_user.full_name
    conn.close()

    # ВАЖНО: Тут исправлено callback_data (было callback_mode)
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Разрешить", callback_data=f"confirm_exit:{uid}"),
            InlineKeyboardButton(text="❌ Запретить", callback_data=f"deny_exit:{uid}")
        ]
    ])

    try:
        # Если ADMINS — это список [123, 456], берем первого ADMINS[0]
        # Если ADMINS — это просто число, убери [0]
        admin_to_send = ADMINS[0] if isinstance(ADMINS, list) else ADMINS
        
        await m.bot.send_message(
            admin_to_send, 
            f"🔔 <b>ЗАЯВКА НА ВЫХОД</b>\n\n"
            f"Юзер: {username} (ID: {uid})\n"
            f"Клуб: <b>{club_name}</b>\n"
            f"Хочет покинуть проект.",
            parse_mode="HTML",
            reply_markup=kb
        )
        await m.answer("⏳ Ваша заявка отправлена администратору на рассмотрение.")
    except Exception as e:
        print(f"Ошибка отправки админу: {e}") # Посмотришь в консоли, если упадет
        await m.answer("❌ Ошибка при отправке заявки. Убедись, что админ запустил бота.")

@dp.callback_query(F.data.startswith("confirm_exit:"))
async def approve_exit(cb: types.CallbackQuery):
    user_id = int(cb.data.split(":")[1])
    conn = get_db(); c = conn.cursor()
    
    # 1. Узнаем какой клуб покидает юзер
    c.execute('SELECT club, balance FROM users WHERE user_id = ?', (user_id,))
    res = c.fetchone()
    if not res:
        conn.close()
        return await cb.answer("Юзер не найден в базе.")
    
    club_name = res[0]
    # Сохраняем баланс (просто для инфы в лог, в базе мы его НЕ обнуляем теперь)
    current_balance = res[1]

    # 2. Просто снимаем юзера с должности тренера
    c.execute('UPDATE users SET club = NULL WHERE user_id = ?', (user_id,))
    
    # 3. Важный момент по игрокам:
    
    # Очищаем только расписание матчей, так как тренера больше нет
    c.execute('DELETE FROM league_schedule WHERE (home_id = ? OR away_id = ?) AND status = "pending"', (user_id, user_id))

    conn.commit(); conn.close()

    # Уведомляем бывшего тренера
    try:
        await cb.bot.send_message(
            user_id, 
            f"✅ Админ одобрил ваш выход.\n\n"
            f"Вы покинули <b>{club_name}</b>.\n"
            f"Все активы (игроки и баланс) остались закреплены за клубом. "
            f"Вы теперь свободный агент.", 
            parse_mode="HTML"
        )
    except: pass

    await cb.message.edit_text(f"✅ Выход разрешен. {club_name} теперь свободен. Баланс и состав сохранены для преемника.")
    await cb.answer()

@dp.callback_query(F.data.startswith("deny_exit:"))
async def decline_exit(cb: types.CallbackQuery):
    user_id = int(cb.data.split(":")[1])
    
    try:
        await cb.bot.send_message(user_id, "❌ Администратор отклонил ваш запрос на выход из клуба. Продолжайте играть!")
    except: pass

    await cb.message.edit_text(f"❌ Ты запретил выход юзеру {user_id}.")
    await cb.answer()

@dp.message(F.text == "!хелп")
async def help_command(m: types.Message):
    help_text = (
        "📖 <b>СПИСОК КОМАНД БОТА</b>\n\n"
        "⚽️ <b>Игровые:</b>\n"
        "├ <code>!клубы</code> — Посмотреть список клубов\n"
        "├ <code>!выйти</code> — Покинуть текущий клуб\n"
        "├ <code>!составы</code> — Стартовые составы всей лиги\n"
        "└ <code>!топ</code> — Самые активные игроки за 24 часа\n"
        "└ <code>!казик</code> — Испытай свою удачу!\n"
         "└ <code>!казиктоп</code> — Посмотри соклько денег депнуто за сезон\n"
        "└ <code>!банан</code> — Забананить другого тренера\n"
        "└ <code>!правила</code> — ⚠️ <b>Читать всем обязательно!</b>\n\n"
        "🛠 <b>Управление:</b>\n"
        "├ Используй кнопку 🏃 <b>/start</b> для выбора клуба\n"
        "├ Используй кнопку 🚀 <b>Рынок</b> для торговли (в личке)\n"
        "└ Используй кнопку 📋 <b>Состав</b> для управления командой\n\n"
    )
    await m.answer(help_text, parse_mode="HTML")

async def send_temp_msg(ctx, text, reply_markup=None, delay=15):
    """Отправляет сообщение и вешает на него таймер удаления"""
    # ctx может быть как message, так и callback.message
    msg = await ctx.answer(text, reply_markup=reply_markup)
    asyncio.create_task(delete_after(msg, delay))
    return msg

async def delete_after(msg, delay):
    await asyncio.sleep(delay)
    try:
        await msg.delete()
    except:
        pass

# --- СОСТОЯНИЯ ---
# --- БАЗА ДАННЫХ ---
# Убедись, что эта функция стоит ВЫШЕ всех остальных, где она используется
def apply_real_injury_to_db(uid, player_name):
    import injured
    # Убедись, что в injured.py переменная MAX_STAMINA определена
    name, duration = injured.get_random_injury()
    
    conn = get_db()
    c = conn.cursor()
    # Игрок получает травму, уходит в запас и освобождает слот на поле
    c.execute('''UPDATE squad 
                 SET injury_type = ?, injury_remaining = ?, status = "bench", slot_id = NULL 
                 WHERE user_id = ? AND player_name = ?''', 
              (name, duration, uid, player_name))
    conn.commit()
    conn.close()

# @dp.message(Command("clear_league")) # Не забудь добавить Command в импорты из aiogram.filters
# async def clear_league_db(m: types.Message):
#     if m.from_user.id not in ADMINS: return
    
#     conn = get_db()
#     c = conn.cursor()
#     c.execute('DELETE FROM league_participants')
#     conn.commit()
#     conn.close()
    
#     await m.answer("🧹 Таблица участников очищена! Теперь багов с '3 юзерами' не будет.")

def check_squad_size(user_id):
    conn = get_db()
    c = conn.cursor()
    # Считаем только тех, кто не забанен и не на рынке (опционально)
    # Или просто общее количество игроков в клубе:
    c.execute('SELECT COUNT(*) FROM squad WHERE user_id = ?', (user_id,))
    count = c.fetchone()[0]
    conn.close()
    return count

def get_bot_club_ovr(club_name):
    if club_name not in CLUBS:
        return 75  # Запасной вариант
    
    # Берем первых 11 игроков (это обычно основа в твоем списке)
    players = CLUBS[club_name]["players"][:11]
    ratings = [p["rating"] for p in players]
    
    return sum(ratings) / len(ratings)

def get_squad_text(uid):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT player_name, pos, rating, stamina, injury_type FROM squad WHERE user_id = ?', (uid,))
    players = c.fetchall()
    conn.close()

    if not players:
        return "У вас пока нет игроков в составе."

    text = "📋 Ваш состав:\n\n"
    for p_name, pos, rat, stam, inj in players:
        # Проверяем, не травмирован ли игрок
        status_icon = "🚑" if inj else "✅"
        # Стамина (берем 0, если данных нет)
        s_val = stam if stam is not None else 0
        
        text += f"{status_icon} {pos} | {p_name} ({rat}) — 🔋 {s_val}/50\n"
    
    return text

def get_actual_squad_from_db(uid):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT player_name, rating, pos FROM squad WHERE user_id = ? AND status = "main"', (uid,))
    rows = c.fetchall()
    conn.close()
    return [{"name": r[0], "rating": r[1], "pos": r[2], "yc": 0} for r in rows]


TACTIC_DOMINANCE = {
    "Автобус": "Прессинг",       
    "Прессинг": "Тики-така",     
    "Тики-така": "Контратака",   
    "Контратака": "Бей-беги",    
    "Бей-беги": "Автобус"        
}

FORMATION_MODS = {
    "4-4-2": {"DEF": 1.0, "MID": 1.1, "FWD": 1.0},
    "4-3-3": {"DEF": 1.0, "MID": 1.0, "FWD": 1.2},
    "3-4-3": {"DEF": 0.8, "MID": 1.1, "FWD": 1.2},
    "5-3-2": {"DEF": 1.3, "MID": 0.9, "FWD": 0.9},
    "3-5-2": {"DEF": 0.9, "MID": 1.3, "FWD": 0.9}
}

def get_weighted_scorer(players_list):
    # Задаем веса для позиций: Напы забивают чаще всего, вратари — почти никогда
    SCORER_WEIGHTS = {'FWD': 10, 'MID': 5, 'DEF': 1, 'GK': 0.1}
    
    # Собираем веса для текущего состава
    weights = [SCORER_WEIGHTS.get(p.get('pos', 'MID'), 1) for p in players_list]
    
    # random.choices выбирает одного игрока с учетом этих весов
    return random.choices(players_list, weights=weights, k=1)[0]

def get_weighted_assister(players_list, scorer_id):
    others = []
    for p in players_list:
        # 1. Достаем ID (если словарь — .get, если кортеж — p[0])
        if isinstance(p, dict):
            p_id = p.get('db_id') or p.get('id')
        else:
            p_id = p[0] # В fetchall ID обычно первый
            
        # 2. Исключаем автора гола
        if p_id != scorer_id:
            others.append(p)
            
    if not others:
        return None
        
    # Веса для ассиста: MID (x3), FWD (x2), DEF (x1)
    weights = []
    for p in others:
        pos = p.get('pos') if isinstance(p, dict) else p[2]
        pos = str(pos).upper()
        
        if pos == 'MID': weights.append(3)
        elif pos == 'FWD': weights.append(2)
        else: weights.append(1)
        
    return random.choices(others, weights=weights, k=1)[0]

def get_squad_rating(user_id):
    conn = get_db()
    c = conn.cursor()
    # Берем ТОЛЬКО 11 игроков, которые стоят в слотах (status = 'active' или slot_id IS NOT NULL)
    c.execute('SELECT rating FROM squad WHERE user_id = ? AND slot_id IS NOT NULL LIMIT 11', (user_id,))
    ratings = [r[0] for r in c.fetchall()]
    conn.close()

    if not ratings:
        return 40.0
    
    # Считаем среднее строго по 11 позициям
    return round(sum(ratings) / 11, 1)

async def update_match_message(msg: types.Message, uid: int):
    data = matches_data[uid]
    
    # Заголовок теперь всегда показывает "Твой Клуб vs Соперник"
    match_title = f"🏟 <b>{data['my_club']} vs {data['opp_name']}</b>"
    
    log_v = "\n".join(data["match_log"][-3:]) # Последние 3 события
    text = (f"{match_title}\n"
            f"⏱ {data['minute']}' | Счет: <b>{data['score_me']}:{data['score_opp']}</b>\n"
            f"Тактика: {data['tactic']}\n"
            f"————————————————\n{log_v if log_v else 'Идет плотная борьба...'}")
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⚙️ Руководство", callback_data="manage_team")]
    ])
    
    try:
        await msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except: 
        pass # Игнорируем, если текст не изменился

def get_formation_inline():
    builder = InlineKeyboardBuilder()
    forms = ["4-4-2", "4-3-3", "3-4-3", "5-3-2", "3-5-2"]
    for f in forms:
        # Исправлено на callback_data
        builder.button(text=f, callback_data=f"set_formation:{f}")
    builder.adjust(2)
    return builder.as_markup()

@dp.message(F.text.startswith("!id"))
async def get_user_id(m: types.Message):
    # Если это ответ на сообщение (реплей)
    if m.reply_to_message:
        target = m.reply_to_message.from_user
        await m.reply(
            f"👤 <b>Данные игрока:</b>\n"
            f"🆔 ID: <code>{target.id}</code>\n"
            f"🏷 Имя: {target.full_name}\n"
            f"🔗 Юзернейм: @{target.username if target.username else 'нет'}",
            parse_mode="HTML"
        )
    # Если просто команда !id
    else:
        await m.reply(
            f"🆔 Ваш ID: <code>{m.from_user.id}</code>", 
            parse_mode="HTML"
        )

@dp.callback_query(F.data.startswith("set_formation:")) 
async def set_formation_callback(cb: types.CallbackQuery):
    uid = cb.from_user.id
    new_form = cb.data.split(":")[1] 

    if uid in matches_data:
        # В матче меняем только временную схему
        matches_data[uid]["formation"] = new_form
        # Сообщаем об успехе, но оставляем меню схем открытым
        await cb.answer(f"Тактика изменена на {new_form}!")
        await open_forms_cb(cb) 
    else:
        # Вне матча сохраняем в БД и сбрасываем состав
        conn = get_db(); c = conn.cursor()
        c.execute('UPDATE users SET formation = ? WHERE user_id = ?', (new_form, uid))
        c.execute('UPDATE squad SET slot_id = NULL, status = "bench" WHERE user_id = ?', (uid,))
        conn.commit(); conn.close()
        await cb.answer(f"Основная схема: {new_form}")
        await show_formation_menu_inline(cb)


# Вспомогательная функция для обновления меню схем без нового сообщения
async def show_formation_menu_inline(cb: types.CallbackQuery):
    uid = cb.from_user.id
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT formation FROM users WHERE user_id = ?', (uid,))
    res = c.fetchone()
    current_form = res[0] if res else "4-3-3"
    conn.close()

    builder = InlineKeyboardBuilder()
    forms = ["4-4-2", "4-3-3", "3-4-3", "5-3-2", "3-5-2"]
    for f in forms:
        btn_text = f"✅ {f}" if f == current_form else f
        builder.button(text=btn_text, callback_data=f"set_formation:{f}")
    builder.adjust(2)
    
    await cb.message.edit_text(
        f"<b>Управление тактикой</b> 📐\nТекущая схема: <b>{current_form}</b>\n\n"
        f"<i>При смене схемы состав сбрасывается!</i>",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )

@dp.message(F.text == "📅 Мои матчи")
@dp.message(F.text == "!игры")
async def show_fixtures(m: types.Message):
    user_id = m.from_user.id
    conn = get_db(); c = conn.cursor()

    c.execute('SELECT club FROM users WHERE user_id = ?', (user_id,))
    user_club = c.fetchone()
    if not user_club or not user_club[0]:
        conn.close(); return await m.answer("❌ Сначала создай клуб!")

    user_club_name = user_club[0]
    
    # 1. Получаем матчи Лиги
    c.execute('''
        SELECT s.tour_number as priority, u1.club, u2.club, s.home_id, '🏆 ЛИГА' as type, s.tour_number as stage_name
        FROM league_schedule s
        JOIN users u1 ON s.home_id = u1.user_id
        JOIN users u2 ON s.away_id = u2.user_id
        WHERE (s.home_id = ? OR s.away_id = ?) AND s.status = "pending"
    ''', (user_id, user_id))
    league_fixtures = c.fetchall()

    # 2. Получаем матчи Кубка (БЕЗ 1/16)
    # Исключаем 1/16 из запроса, чтобы она не тянулась из старых записей
    c.execute('''
        SELECT 
            CASE 
                WHEN stage = 'Play-In' THEN 2.5 
                WHEN stage = '1/8' THEN 5.5
                WHEN stage = '1/4' THEN 8.5
                WHEN stage = '1/2' THEN 12.5
                ELSE 20 
            END as priority,
            t1_name, t2_name, t1_id, '🎫 КУБОК' as type, stage
        FROM cup_bracket
        WHERE (t1_id = ? OR t2_id = ?) 
          AND winner_id IS NULL 
          AND stage != '1/16'
    ''', (user_id, user_id))
    cup_fixtures = c.fetchall()

    conn.close()

    all_fixtures = sorted(league_fixtures + cup_fixtures, key=lambda x: x[0])

    if not all_fixtures:
        return await m.answer(f"🏟 <b>{user_club_name}</b>\n\n✅ Все текущие матчи сыграны.")

    text = f"📅 <b>КАЛЕНДАРЬ ИГР: {user_club_name.upper()}</b>\n"
    text += "————————————————————\n"

    for i, (priority, home_name, away_name, h_id, t_type, stage_label) in enumerate(all_fixtures):
        role = "🏠 Дома" if h_id == user_id else "✈️ В гостях"
        display_stage = f"Тур {stage_label}" if "ЛИГА" in t_type else stage_label

        if i == 0:
            text += f"🆕 <b>БЛИЖАЙШАЯ ИГРА:</b>\n"
            text += f"🔘 <b>{t_type}</b> — {display_stage}\n"
            text += f"📍 {role}\n"
            text += f"👉 <code>{home_name} — {away_name}</code>\n\n"
            if len(all_fixtures) > 1:
                text += "<b>Дальнейшие игры:</b>\n"
        else:
            text += f"▫️ {t_type} | {display_stage}: <code>{home_name} — {away_name}</code>\n"

    text += "————————————————————\n"
    text += "<i>Подготовь состав:</i> /squad"

    await m.answer(text, parse_mode="HTML")

@dp.callback_query(F.data == "open_formations")
async def open_forms_cb(cb: types.CallbackQuery):
    # Просто вызываем функцию меню схем, но редактируя текущее сообщение
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT formation FROM users WHERE user_id = ?', (cb.from_user.id,))
    res = c.fetchone()
    current_form = res[0] if res else "4-4-3"
    conn.close()

    builder = InlineKeyboardBuilder()
    forms = ["4-4-2", "4-3-3", "3-4-3", "5-3-2", "3-5-2"]
    for f in forms:
        btn_text = f"✅ {f}" if f == current_form else f
        builder.button(text=btn_text, callback_data=f"set_formation:{f}")
    builder.adjust(2)
    
    await cb.message.edit_text("Выберите тактическую схему:", reply_markup=builder.as_markup())

async def update_match_message(msg: types.Message, uid: int):
    if uid not in matches_data: return
    data = matches_data[uid]
    
    # Используем .get() чтобы не было KeyError
    m_club = data.get('my_club', 'Мой Клуб')
    o_club = data.get('opp_name', 'Соперник')
    
    log_v = "\n".join(data.get("match_log", [])[-3:])
    text = (f"🏟 <b>{m_club} vs {o_club}</b>\n"
            f"⏱ {data['minute']}' | Счет: <b>{data['score_me']}:{data['score_opp']}</b>\n"
            f"Тактика: {data.get('tactic', 'Тики-така')}\n"
            f"————————————————\n{log_v if log_v else 'Идет плотная борьба...'}")
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⚙️ Руководство", callback_data="manage_team")]
    ])
    
    try:
        await msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except:
        pass

@dp.message(F.text == "📐 Схемы")
async def show_formation_menu(message: types.Message):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT formation FROM users WHERE user_id = ?', (message.from_user.id,))
    res = c.fetchone()
    current_form = res[0] if res else "4-4-3"
    conn.close()

    builder = InlineKeyboardBuilder()
    forms = ["4-4-2", "4-3-3", "3-4-3", "5-3-2", "3-5-2"]
    for f in forms:
        btn_text = f"✅ {f}" if f == current_form else f
        # ИСПОЛЬЗУЙ ТОЛЬКО callback_data
        builder.button(text=btn_text, callback_data=f"set_formation:{f}")
    
    builder.adjust(2)
    
    await message.answer(
        f"<b>Управление тактикой</b> 📐\n\nТекущая схема: <b>{current_form}</b>", 
        reply_markup=builder.as_markup(), 
        parse_mode="HTML"
    )

# --- ЛОГИКА ---
async def edit_squad_message(message: types.Message, user_id: int, chat_id: int, viewer_id: int = None):
    # --- ТВОЙ БЛОК ПРОВЕРКИ ГРУППЫ (БЕЗ ИЗМЕНЕНИЙ) ---
    chat_obj = message.message.chat if isinstance(message, types.CallbackQuery) else message.chat
    
    if chat_obj.type != "private":
        text = "❌ Управление составом доступно только в личных сообщениях с ботом!"
        if isinstance(message, types.CallbackQuery):
            return await message.answer(text, show_alert=True)
        
        warning_msg = await message.answer(text)
        await asyncio.sleep(6)
        try:
            await warning_msg.delete()
            await message.delete()
        except: pass
        return

    # --- ЛОГИКА ДЛЯ ЛИЧКИ ---
    if viewer_id is None: viewer_id = user_id
    is_owner = (user_id == viewer_id)

    if is_owner:
        try:
            tired.process_stamina_recovery(user_id) 
        except Exception as e:
            print(f"Ошибка восстановления стамины: {e}")

    # --- РАБОТА С БАЗОЙ ДАННЫХ ---
    conn = get_db()
    c = conn.cursor()
    
    # Соединяем два твоих запроса в один для оптимизации
    c.execute('''SELECT club, formation, tactic, captain_id, penalty_id, freekick_id 
                 FROM users WHERE user_id = ?''', (user_id,))
    user_data = c.fetchone()
    
    if not user_data or not user_data[0]:
        conn.close()
        text = "❌ Клуб не найден. Начните /start"
        if isinstance(message, types.CallbackQuery):
            return await message.answer(text, show_alert=True)
        return await message.answer(text)

    club_name, formation_name, tactic, cap_id, pen_id, fk_id = user_data

    # Получаем игроков (нужно сделать ДО формирования текста настроек)
    c.execute('''SELECT id, player_name, rating, pos, slot_id, stamina, injury_type 
                 FROM squad 
                 WHERE user_id = ? AND slot_id IS NOT NULL 
                 ORDER BY slot_id ASC''', (user_id,))
    
    # Создаем словарь слотов
    slots_dict = {row[4]: row for row in c.fetchall()}
    conn.close()

    # Вспомогательная функция (теперь slots_dict уже существует)
    def get_pl_name(pid):
        if not pid: return "Не назначен"
        for s in slots_dict.values():
            if s[0] == pid: return s[1]
        return "Не в составе"

    # --- ФОРМИРОВАНИЕ ТЕКСТА ---
    current_rating = get_squad_rating(user_id)

    # Собираем настройки (Твои roles_text)
    roles_text = (
        f"<b>⚙️ Настройки:</b>\n"
        f"• Тактика: <b>{tactic}</b>\n"
        f"• Капитан: <b>{get_pl_name(cap_id)}</b>\n"
        f"• Пенальти: <b>{get_pl_name(pen_id)}</b>\n"
        f"• Штрафные: <b>{get_pl_name(fk_id)}</b>\n\n"
    )

    # Основной текст сообщения
    text = (
        f"🏟 <b>{club_name}</b>\n"
        f"⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n"
        f"📐 Схема: <b>{formation_name}</b> | ⭐ РТГ: <b>{current_rating}</b>\n\n"
        f"{roles_text}"
        f"📋 <b>Стартовый состав:</b>\n"
    )

    # --- ЛОГИКА ПОСТРОЕНИЯ КНОПОК ПОЛЯ ---
    try:
        f_parts = [int(x) for x in formation_name.split('-')]
        formation_layout = [1] + f_parts
    except:
        formation_layout = [1, 4, 3, 3]

    builder = InlineKeyboardBuilder()
    current_slot = 1
    pos_names = ["GK", "DEF", "MID", "FWD"]
    
    for i, count in enumerate(formation_layout):
        line_pos = pos_names[i]
        line_buttons = []
        
        for _ in range(count):
            player_in_slot = slots_dict.get(current_slot)            
            if player_in_slot:
                pid, name, rat, pos, _, stam, inj = player_in_slot
                icon = "🚑" if inj else "✅"
                cb_data = f"pl_{pid}" if is_owner else "view_only_info"
                line_buttons.append(types.InlineKeyboardButton(text=icon, callback_data=cb_data))
                
                inj_info = f" [🤕 {inj}]" if inj else ""
                text += f"<code>{current_slot}.</code> {name} ({rat}) 🔋{stam}%{inj_info}\n"
            else:
                cb_data = f"selectpos_{line_pos}_{current_slot}" if is_owner else "view_only_info"
                btn_text = "➕" if is_owner else "▫️"
                line_buttons.append(types.InlineKeyboardButton(text=btn_text, callback_data=cb_data))
                text += f"<code>{current_slot}.</code> ——— <i>Пусто ({line_pos})</i> ———\n"
            
            current_slot += 1
        builder.row(*line_buttons)
            
    # --- НИЖНИЕ КНОПКИ УПРАВЛЕНИЯ ---
    if is_owner:
        # Добавляем твою новую кнопку настроек
        builder.row(types.InlineKeyboardButton(text="⚙️ Настройки и Тактика", callback_data="squad_settings"))
        
        builder.row(
            types.InlineKeyboardButton(text="⚡️ Автосбор", callback_data="autofill"),
            types.InlineKeyboardButton(text="🗑 Очистить", callback_data="clear_squad")
        )
        builder.row(
            types.InlineKeyboardButton(text="📐 Схемы", callback_data="open_formations"),
            types.InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_main")
        )
    else:
        builder.row(
            types.InlineKeyboardButton(text="⬅️ Назад в профиль", callback_data=f"view_profile_{user_id}")
        )

    # --- ОТПРАВКА ИЛИ РЕДАКТИРОВАНИЕ ---
    try:
        if isinstance(message, types.CallbackQuery):
            await message.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
        else:
            await message.answer(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    except Exception as e:
        if "message is not modified" not in str(e):
            print(f"Ошибка отрисовки состава: {e}")

@dp.callback_query(F.data == "back_to_squad")
async def back_to_squad_handler(cb: types.CallbackQuery):
    await edit_squad_message(cb, cb.from_user.id, cb.message.chat.id)

# --- ОБРАБОТЧИКИ ---

@dp.message(Command("start"))
async def start(m: types.Message, state: FSMContext):
    # init_db() лучше вызывать один раз при запуске бота, а не в каждом сообщении
    uid = m.from_user.id
    uname = m.from_user.username
    
    conn = get_db()
    c = conn.cursor()
    
    # 1. Проверяем наличие игрока
    c.execute('SELECT club FROM users WHERE user_id = ?', (uid,))
    user = c.fetchone()
    
    # 2. Если игрока нет — создаем запись
    if not user:
        # Теперь колонка username точно есть в базе
        c.execute('INSERT OR IGNORE INTO users (user_id, username, balance) VALUES (?, ?, 1000)', (uid, uname))
        conn.commit()
        # Перезапрашиваем данные после вставки
        c.execute('SELECT club FROM users WHERE user_id = ?', (uid,))
        user = c.fetchone()

    # 3. Если клуб уже выбран — пускаем в игру
    if user and user[0]:
        conn.close()
        return await m.answer("Вы уже в игре!", reply_markup=get_main_kb(uid))
    
    # 4. Собираем список занятых клубов для выбора
    c.execute('SELECT DISTINCT club FROM users WHERE club IS NOT NULL')
    taken_clubs = [row[0] for row in c.fetchall()]
    conn.close() # Закрываем базу перед асинхронными ответами
    
    b = InlineKeyboardBuilder()
    for n in CLUBS:
        if n not in taken_clubs:
            b.button(text=f"{CLUBS[n]['emoji']} {n}", callback_data=f"club_{n}")
    
    b.adjust(1)
    await m.answer("Выберите свободный клуб:", reply_markup=b.as_markup())
    await state.set_state(GameStates.choosing_club)

@dp.callback_query(F.data == "squad_settings")
async def squad_settings_menu(cb: types.CallbackQuery):
    builder = InlineKeyboardBuilder()
    
    # 1. Роли (Капитан, Пенальтист и т.д.)
    builder.row(types.InlineKeyboardButton(text="© Капитан", callback_data="setrole_captain"))
    builder.row(
        types.InlineKeyboardButton(text="🎯 Пенальти", callback_data="setrole_penalty"),
        types.InlineKeyboardButton(text="☄️ Штрафные", callback_data="setrole_freekick")
    )
    
    # 2. РЕАЛЬНЫЕ ТАКТИКИ (Вместо "Атак/Защ")
    # Группируем кнопки для красоты
    t_btns = [
        types.InlineKeyboardButton(text="🚌 Автобус", callback_data="settactic_Автобус"),
        types.InlineKeyboardButton(text="⚡️ Прессинг", callback_data="settactic_Прессинг"),
        types.InlineKeyboardButton(text="🪄 Тики-така", callback_data="settactic_Тики-така"),
        types.InlineKeyboardButton(text="🏹 Контратака", callback_data="settactic_Контратака"),
        types.InlineKeyboardButton(text="🏃‍♂️ Бей-беги", callback_data="settactic_Бей-беги")
    ]
    
    builder.row(t_btns[0], t_btns[1]) # Автобус и Прессинг
    builder.row(t_btns[2])           # Тики-така по центру
    builder.row(t_btns[3], t_btns[4]) # Контра и Бей-беги
    
    builder.row(types.InlineKeyboardButton(text="⬅️ Назад к составу", callback_data="back_to_squad"))
    
    await cb.message.edit_text(
        "⚙️ <b>ТАКТИЧЕСКИЙ ШТАБ</b>\n\n"
        "Выбери стиль игры. Помни: <b>каждая тактика бьет другую!</b>\n"
        "🚌 > ⚡️ | ⚡️ > 🪄 | 🪄 > 🏹 | 🏹 > 🏃‍♂️ | 🏃‍♂️ > 🚌", 
        reply_markup=builder.as_markup(), 
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("setslot_"))
async def set_player_to_slot(cb: types.CallbackQuery):
    _, pid, slot_id = cb.data.split("_")
    uid = cb.from_user.id

    if not await check_ownership(cb, pid): return
    
    conn = get_db()
    c = conn.cursor()
    
    # ПРОВЕРКА: Не травмирован ли игрок и нет ли бана?
    c.execute('''SELECT player_name, injury_remaining, is_banned 
                 FROM squad WHERE id = ? AND user_id = ?''', (pid, uid))
    player = c.fetchone()
    
    if player and (player[1] > 0 or player[2] > 0):
        conn.close()
        return await cb.answer(f"❌ {player[0]} недоступен (травма/бан)!", show_alert=True)
    
    # 1. Убираем того, кто уже сидел в этом слоте
    c.execute('UPDATE squad SET slot_id = NULL, status = "bench" WHERE user_id = ? AND slot_id = ?', (uid, slot_id))
    
    # 2. Убираем нового игрока из его старого слота (если он был)
    c.execute('UPDATE squad SET slot_id = NULL, status = "bench" WHERE id = ? AND user_id = ?', (pid, uid))
    
    # 3. Ставим игрока в слот
    c.execute('UPDATE squad SET slot_id = ?, status = "active" WHERE id = ? AND user_id = ?', (slot_id, pid, uid))
    
    conn.commit()
    conn.close()
    
    await cb.answer("✅ Игрок выставлен в состав!")
    await edit_squad_message(cb.message, uid, cb.message.chat.id)

@dp.callback_query(F.data.startswith("settactic_"))
async def set_tactic_handler(cb: types.CallbackQuery):
    new_tactic = cb.data.split("_")[1]
    with get_db() as conn:
        conn.execute('UPDATE users SET tactic = ? WHERE user_id = ?', (new_tactic, cb.from_user.id))
        conn.commit()
    
    await cb.answer(f"✅ Тактика изменена на: {new_tactic}")
    await squad_settings_menu(cb) # Возвращаемся в меню настроек

@dp.callback_query(F.data.startswith("setrole_"))
async def list_players_for_role(cb: types.CallbackQuery):
    role = cb.data.split("_")[1] # captain, penalty или freekick
    user_id = cb.from_user.id
    
    conn = get_db(); c = conn.cursor()
    # Берем тех, кто в старте
    c.execute('SELECT id, player_name, rating, pos FROM squad WHERE user_id = ? AND slot_id IS NOT NULL', (user_id,))
    players = c.fetchall()
    conn.close()
    
    if not players:
        return await cb.answer("❌ Сначала соберите состав!", show_alert=True)
        
    builder = InlineKeyboardBuilder()
    
    for pid, name, rat, pos in players:
        # ПРОВЕРКА: Если роль - пенальти или штрафной, а позиция - GK, пропускаем игрока
        if role in ["penalty", "freekick"] and pos == "GK":
            continue 
            
        builder.row(types.InlineKeyboardButton(
            text=f"{name} ({rat})", 
            callback_data=f"confirmrole_{role}_{pid}"
        ))
    
    builder.row(types.InlineKeyboardButton(text="⬅️ Отмена", callback_data="squad_settings"))
    
    titles = {"captain": "Капитана", "penalty": "Пенальтиста", "freekick": "Исполнителя штрафных"}
    await cb.message.edit_text(
        f"🎯 Выберите <b>{titles[role]}</b>:\n"
        f"<i>(Вратари не могут бить пенальти и штрафные)</i>", 
        reply_markup=builder.as_markup(), parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("confirmrole_"))
async def confirm_role_handler(cb: types.CallbackQuery):
    _, role, pid = cb.data.split("_")
    column = f"{role}_id" # captain_id, penalty_id и т.д.
    
    with get_db() as conn:
        conn.execute(f'UPDATE users SET {column} = ? WHERE user_id = ?', (pid, cb.from_user.id))
        conn.commit()
    
    await cb.answer("✅ Назначено!")
    await squad_settings_menu(cb)

@dp.callback_query(F.data.startswith("selectpos_"))
async def list_players_for_slot(cb: types.CallbackQuery):
    _, pos_needed, slot_id = cb.data.split("_")
    uid = cb.from_user.id

    conn = get_db(); c = conn.cursor()
    
    # ИСПРАВЛЕНИЕ: Добавляем маску поиска % (например, %MID%)
    search_pattern = f"%{pos_needed}%"
    
    # ИСПРАВЛЕНИЕ: Меняем "pos = ?" на "pos LIKE ?"
    c.execute('''SELECT id, player_name, rating, stamina, pos 
                 FROM squad 
                 WHERE user_id = ? 
                 AND pos LIKE ? 
                 AND status = "bench" 
                 AND injury_remaining = 0
                 ORDER BY rating DESC''', (uid, search_pattern))
    
    players = c.fetchall(); conn.close()
    
    if not players:
        return await cb.answer(f"❌ У вас нет свободных игроков на позицию {pos_needed}", show_alert=True)

    b = InlineKeyboardBuilder()
    for pid, name, rat, stam, p_pos in players:
        # Добавил отображение позиции [p_pos], чтобы ты видел, что универсалы подтянулись
        b.button(text=f"[{p_pos}] {name} ({rat}) 🔋{stam}%", callback_data=f"setslot_{pid}_{slot_id}")
    
    b.adjust(1)
    b.row(types.InlineKeyboardButton(text="⬅️ Назад к составу", callback_data="edit_squad_message"))
    
    await cb.message.edit_text(f"📥 <b>Выберите {pos_needed} для слота №{slot_id}:</b>", 
                               reply_markup=b.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("club_"), GameStates.choosing_club)
async def choose_club(cb: types.CallbackQuery, state: FSMContext):
    await cb.answer()
    uid = cb.from_user.id
    uname = cb.from_user.username
    selected_club = cb.data.split("_")[1]
    
    if selected_club not in CLUBS: 
        return

    conn = get_db()
    c = conn.cursor()
    
    try:
        # 1. Проверка занятости клуба
        c.execute('SELECT username FROM users WHERE club = ? AND user_id != ?', (selected_club, uid))
        owner = c.fetchone()
        if owner:
            return await cb.message.answer(f"❌ Клуб {selected_club} уже занят менеджером @{owner[0]}!")

        # 2. НОВОЕ: Получаем список проданных игроков этого клуба
        c.execute('SELECT player_name FROM sold_originals WHERE club_name = ?', (selected_club,))
        sold_list = [row[0] for row in c.fetchall()]

        # 3. Баланс
        start_balance = TEAM_BALANCES.get(selected_club, 20_000_000) 

        # 4. Чистим старое (смена клуба)
        c.execute('DELETE FROM users WHERE user_id = ?', (uid,))
        c.execute('DELETE FROM squad WHERE user_id = ?', (uid,))
        
        # 5. Регистрация
        c.execute('INSERT INTO users (user_id, username, club, balance) VALUES (?, ?, ?, ?)', 
                  (uid, uname, selected_club, start_balance))

        # 6. Заполнение состава (С ФИЛЬТРАЦИЕЙ ПРОДАННЫХ)
        for p in CLUBS[selected_club]["players"]:
            # Если игрок был продан навсегда — пропускаем его
            if p['name'] in sold_list:
                print(f"⚠️ Игрок {p['name']} продан ранее, пропускаем выдачу.")
                continue
                
            pos_display = p['pos'] if isinstance(p['pos'], str) else "/".join(p['pos'])
            c.execute('''
                INSERT INTO squad (user_id, player_name, rating, pos, status, is_banned) 
                VALUES (?, ?, ?, ?, "bench", 0)
            ''', (uid, p['name'], p['rating'], pos_display))
        
        conn.commit()
        
        formatted_balance = f"{start_balance:,}".replace(",", " ")
        await cb.message.delete()
        await cb.message.answer(
            f"✅ Вы возглавили <b>{selected_club}</b>!\n"
            f"💰 Ваш бюджет: <b>{formatted_balance} €</b>", 
            reply_markup=get_main_kb(uid), 
            parse_mode="HTML"
        )
        await state.clear()
        
    except Exception as e:
        print(f"❌ Ошибка при выборе клуба: {e}")
        await cb.message.answer("Произошла ошибка при регистрации клуба.")
    finally:
        conn.close()
    
@dp.message(F.text == "📋 Состав")
@dp.message(Command("squad")) 
async def show_squad(m: types.Message):
    # Проверяем тип чата сразу, чтобы не плодить лишние сообщения в группах
    if m.chat.type != "private":
        warning = await m.answer("❌ Команда доступна только в личных сообщениях!")
        await asyncio.sleep(6)
        try:
            await warning.delete()
            await m.delete()
        except:
            pass
        return

    # Если мы в личке, продолжаем работу
    msg = await m.answer("⏳ Загрузка состава...")
    
    # Вызываем твою функцию отрисовки
    await edit_squad_message(msg, m.from_user.id, m.chat.id)

@dp.callback_query(F.data == "back_to_field")
async def back(cb: types.CallbackQuery): 
    # Добавляем cb.message.chat.id
    await edit_squad_message(cb.message, cb.from_user.id, cb.message.chat.id)

@dp.callback_query(F.data.startswith("pl_"))
async def manage_player(cb: types.CallbackQuery, state: FSMContext):
    data_parts = cb.data.split("_")
    if len(data_parts) < 2: return await cb.answer("❌ Ошибка ID")
    
    pid_str = data_parts[1]
    user_id = cb.from_user.id
    
    conn = get_db(); c = conn.cursor()
    c.execute('''SELECT player_name, rating, pos, status, original_owner_id, 
                        training_until, injury_remaining, stamina, user_id
                 FROM squad WHERE id = ?''', (int(pid_str),))
    row = c.fetchone()
    conn.close()

    if not row: return await cb.answer("Игрок не найден", show_alert=True)
    
    name, rat, pos, status, orig_owner, t_until, inj, stam, p_owner_id = row
    
    # ПРОВЕРКИ
    is_viewer_owner = (int(p_owner_id) == user_id)
    # Проверка аренды (если оригинальный владелец существует и это не текущий)
    is_loaned_here = (orig_owner is not None and orig_owner != 0 and int(orig_owner) != int(p_owner_id))
    
    await state.update_data(curr_pid=pid_str)
    b = InlineKeyboardBuilder()
    
    # --- ЛОГИКА КНОПОК: Только для владельца ---
    if is_viewer_owner:
        # Если не на тренировке, не травмирован и не на рынке — можно тренировать
        if not is_loaned_here and not t_until and inj == 0 and status != "on_sale":
            b.button(text="🏋️‍♂️ Отправить на тренировку", callback_data=f"train_pl_{pid_str}")

        if status != "bench":
            b.button(text="📥 В запас", callback_data="quick_bench")

        if is_loaned_here:
            status_info = "🎭 <b>Статус:</b> В аренде у тебя"
        else:
            if status == "on_sale":
                b.button(text="❌ Снять с рынка", callback_data=f"remove_m_{pid_str}")
                status_text = "На трансфере"
            else:
                b.button(text="🚀 Выставить на рынок", callback_data="pre_sell")
                # b.button(text="🤝 Сдать в аренду", callback_data=f"pre_loan_{pid_str}")
                status_text = "В запасе" if status == "bench" else "В составе"
            status_info = f"📊 <b>Статус:</b> {status_text}"
    else:
        # Если смотрит чужой
        status_info = "📊 <b>Статус:</b> В чужом клубе"

    # Админка (всегда доступна тебе)
    if user_id in ADMINS:
        b.button(text="🛠 Админ-меню", callback_data=f"admin_manage_{pid_str}")

    b.button(text="⬅️ Назад", callback_data="back_to_field")
    b.adjust(1)
    
    text = (f"👤 <b>Игрок:</b> {name} (⭐{rat})\n"
            f"📍 <b>Позиция:</b> {pos}\n"
            f"🔋 <b>Энергия:</b> {stam}%\n"
            f"{status_info}")
    
    # Блок времени для тренировок
    if t_until:
        try:
            now = datetime.now()
            end_t = datetime.strptime(t_until, "%Y-%m-%d %H:%M:%S")
            if end_t > now:
                rem = end_t - now
                text += f"\n\n🏋️‍♂️ <b>На тренировке:</b> {rem.seconds // 3600}ч. {(rem.seconds//60)%60}м."
            else:
                text += f"\n\n✅ <b>Тренировка завершена!</b>"
        except: pass

    if inj > 0:
        text += f"\n\n🚑 <b>Травмирован:</b> еще {inj} тур(а)"
    
    await cb.message.edit_text(text, reply_markup=b.as_markup(), parse_mode="HTML")
        
@dp.message(F.text == "В")
async def cmd_schemes(message: types.Message):
    # Вызываем функцию, которую мы уже писали выше
    await show_formation_menu(message)

@dp.callback_query(F.data == "quick_bench")
async def quick_bench(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data(); pid = data.get("curr_pid")
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE squad SET slot_id = NULL, status = "bench" WHERE id = ?', (pid,))
    conn.commit(); conn.close()
    await cb.answer("Игрок убран")
    # Добавляем cb.message.chat.id
    await edit_squad_message(cb.message, cb.from_user.id, cb.message.chat.id)

@dp.callback_query(F.data == "autofill")
async def autofill(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    
    with get_db() as conn:
        c = conn.cursor()

        # 1. Получаем схему пользователя
        c.execute('SELECT formation FROM users WHERE user_id = ?', (user_id,))
        res = c.fetchone()
        if not res: 
            return await cb.answer("❌ Сначала выберите схему в настройках!")
        
        formation_name = res[0]
        f_parts = [int(x) for x in formation_name.split('-')]
        
        # Схема: Позиция и сколько человек нужно
        formation_logic = [
            ("GK", 1), 
            ("DEF", f_parts[0]), 
            ("MID", f_parts[1]), 
            ("FWD", f_parts[2])
        ]

        # 2. Сбрасываем текущий состав в запас
        c.execute('''UPDATE squad 
                     SET slot_id = NULL, status = "bench" 
                     WHERE user_id = ? AND status != "on_sale" AND training_until IS NULL''', (user_id,))

        players_added = 0
        current_slot = 1
        used_ids = [] # Список ID, которые мы уже поставили на поле

        # 3. Заполняем по позициям
        for pos, limit in formation_logic:
            # ИСПРАВЛЕНО: Используем LIKE %pos%, чтобы найти игрока с двойной позицией
            # Также добавили NOT IN (used_ids), чтобы один и тот же универсал не встал на две позиции сразу
            search_query = f"%{pos}%"
            
            # Формируем строку с уже использованными ID для SQL
            placeholders = ','.join(['?'] * len(used_ids)) if used_ids else '0'
            
            # --- ИСПРАВЛЕННЫЙ ЗАПРОС (Добавлен фильтр бана) ---
            query = f'''SELECT id, player_name, rating FROM squad 
                        WHERE user_id = ? 
                        AND pos LIKE ? 
                        AND status = "bench" 
                        AND injury_remaining = 0 
                        AND is_banned = 0  -- <--- ВОТ ЭТОГО НЕ ХВАТАЛО!
                        AND (training_until IS NULL OR training_until = '')
                        AND id NOT IN ({placeholders})
                        ORDER BY rating DESC LIMIT ?'''
            
            params = [user_id, search_query] + used_ids + [limit]
            c.execute(query, params)
            
            rows = c.fetchall()
            for row in rows:
                if players_added >= 11: break 
                
                c.execute('UPDATE squad SET slot_id = ?, status = "active" WHERE id = ?', 
                         (current_slot, row[0]))
                
                used_ids.append(row[0]) # Помечаем игрока как занятого
                current_slot += 1
                players_added += 1

        conn.commit()

    # 4. Итог
    if players_added < 11:
        msg = f"⚠ Состав: {players_added}/11. Не хватило здоровых игроков!"
    else:
        msg = f"🔥 Топ-состав собран! ({formation_name})"

    await cb.answer(msg, show_alert=True)
    
    # Обновляем сообщение (используй свою функцию перерисовки)
    try:
        await edit_squad_message(cb.message, user_id, cb.message.chat.id)
    except:
        pass
        
@dp.callback_query(F.data == "clear_squad")
async def clear_squad_handler(cb: types.CallbackQuery):
    user_id = cb.from_user.id # ID того, кто нажал
    
    # Чтобы нельзя было очистить чужой клуб:
    # Мы всегда очищаем только тот клуб, который ПРИНАДЛЕЖИТ нажавшему юзеру.
    conn = get_db(); c = conn.cursor()
    
    # Проверяем, есть ли у юзера вообще игроки в составе
    c.execute('SELECT COUNT(*) FROM squad WHERE user_id = ? AND slot_id IS NOT NULL', (user_id,))
    count = c.fetchone()[0]
    
    if count == 0:
        conn.close()
        return await cb.answer("📭 Ваш состав и так пуст!", show_alert=True)

    # Очищаем СВОЙ состав (по user_id нажавшего)
    c.execute('''UPDATE squad 
                 SET slot_id = NULL, status = "bench" 
                 WHERE user_id = ? AND (status != "on_sale" OR status IS NULL)''', (user_id,))
    
    conn.commit(); conn.close()

    await cb.answer("🧹 Ваш состав полностью очищен!")
    # Перерисовываем экран
    await edit_squad_message(cb.message, user_id, cb.message.chat.id)
    
@dp.callback_query(F.data == "pre_sell")
async def pre_sell(cb: types.CallbackQuery, state: FSMContext):
    if not is_transfer_open():
        return await cb.answer("🛑 Трансферное окно закрыто! Выставлять игроков нельзя.", show_alert=True)
    
    await cb.message.edit_text("Введите цену продажи (в млн €):\n\nДля отмены введите <b>Отмена</b>", parse_mode="HTML")
    await state.set_state(GameStates.setting_price)

# --- ПРОСМОТР ИГРОКОВ КЛУБА ---
@dp.callback_query(F.data.startswith("m_club_"))
async def show_club_players(cb: types.CallbackQuery):
    club_name = cb.data.replace("m_club_", "")
    
    conn = get_db(); c = conn.cursor()
    # Выбираем ID, Имя, Рейтинг и Позицию всех игроков клуба
    c.execute('''
        SELECT s.id, s.player_name, s.rating, s.pos, s.user_id 
        FROM squad s 
        JOIN users u ON s.user_id = u.user_id 
        WHERE u.club = ?
    ''', (club_name,))
    players = c.fetchall(); conn.close()

    if not players:
        return await cb.answer("В этом клубе сейчас нет игроков.")

    kb = InlineKeyboardBuilder()
    for pid, name, rat, pos, owner_id in players:
        # Скрываем кнопку, если это игрок самого юзера
        if owner_id == cb.from_user.id: continue
        # Ведем на меню выбора (Купить/Обмен)
        kb.button(text=f"{name} ({rat}) — {pos}", callback_data=f"buy_menu_{pid}")
    
    kb.adjust(1)
    # ИСПРАВЛЕНО: Кнопка назад к списку клубов (back_to_market_main)
    kb.button(text="⬅️ Назад к клубам", callback_data="back_to_market_main")
    
    await cb.message.edit_text(
        f"📋 Состав клуба <b>{club_name}</b>:\nВыберите игрока для трансферного предложения.", 
        reply_markup=kb.as_markup(), 
        parse_mode="HTML"
    )

# --- МЕНЮ ВЫБОРА: КУПИТЬ ИЛИ ОБМЕНЯТЬ ---
@dp.callback_query(F.data.startswith("buy_menu_"))
async def show_purchase_options(cb: types.CallbackQuery):
    pid = int(cb.data.split("_")[2])
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT player_name, rating, market_price, pos, user_id, club FROM squad WHERE id = ?', (pid,))
    p = c.fetchone(); conn.close()

    if not p: return await cb.answer("Игрок не найден.")
    name, rat, price, pos, seller_id, club_name = p

    text = (
        f"👤 <b>{name}</b> [{rat}]\n"
        f"🏃 Позиция: <b>{pos}</b>\n"
        f"💰 Рыночная цена: <b>{price} млн €</b>"
    )

    kb = InlineKeyboardBuilder()
    # Предложить только бабки (твой старый старт оффера)
    kb.button(text="✅ Купить за деньги", callback_data=f"m_target_{pid}") 
    # Предложить обмен + доплату
    kb.button(text="🔄 Обмен + 💰", callback_data=f"ex_start_{pid}")    
    kb.button(text="⬅️ Назад к игрокам", callback_data=f"m_club_{club_name}")
    
    kb.adjust(1)
    await cb.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")

# --- ЛОГИКА ОБМЕНА: ШАГ 1 (Выбор своего игрока) ---
@dp.callback_query(F.data.startswith("ex_start_"))
async def exchange_step_1(cb: types.CallbackQuery, state: FSMContext):
    target_pid = int(cb.data.split("_")[2])
    
    conn = get_db(); c = conn.cursor()
    # Ищем игроков, которые принадлежат тому, кто нажал кнопку
    c.execute('SELECT id, player_name, rating FROM squad WHERE user_id = ?', (cb.from_user.id,))
    my_players = c.fetchall(); conn.close()

    if not my_players:
        return await cb.answer("У тебя нет игроков для обмена!", show_alert=True)

    await state.update_data(ex_target_pid=target_pid)
    # Используем твой стейт waiting_for_trade_player
    await state.set_state(MarketStates.waiting_for_trade_player)

    kb = InlineKeyboardBuilder()
    for pid, name, rat in my_players:
        kb.button(text=f"{name} ({rat})", callback_data=f"ex_select_my_{pid}")
    
    kb.adjust(1)
    await cb.message.edit_text("🔄 <b>Выбери своего игрока</b>, которого отдаешь в обмен:", 
                               reply_markup=kb.as_markup(), parse_mode="HTML")

# --- ЛОГИКА ОБМЕНА: ШАГ 2 (Ввод доплаты) ---
@dp.callback_query(MarketStates.waiting_for_trade_player, F.data.startswith("ex_select_my_"))
async def exchange_step_2(cb: types.CallbackQuery, state: FSMContext):
    my_pid = int(cb.data.split("_")[3])
    await state.update_data(ex_my_pid=my_pid)
    
    await state.set_state(MarketStates.waiting_for_exchange_money)
    await cb.message.answer("💰 Введите сумму доплаты (млн €).\nЕсли без доплаты — введите 0:")
    await cb.answer()

# --- ЛОГИКА ОБМЕНА: ФИНАЛ (Отправка) ---
@dp.message(MarketStates.waiting_for_exchange_money)
async def exchange_finalize(m: types.Message, state: FSMContext):
    if not m.text.isdigit():
        return await m.answer("⚠️ Введите число!")

    money = int(m.text)
    data = await state.get_data()
    
    conn = get_db(); c = conn.cursor()
    # Получаем данные цели (кого хотим забрать)
    c.execute('SELECT player_name, user_id FROM squad WHERE id = ?', (data['ex_target_pid'],))
    target = c.fetchone()
    # Получаем данные нашего игрока (кого отдаем)
    c.execute('SELECT player_name FROM squad WHERE id = ?', (data['ex_my_pid'],))
    mine = c.fetchone()
    conn.close()

    if not target or not mine:
        await state.clear()
        return await m.answer("❌ Ошибка: один из игроков исчез из базы.")

    target_name, target_owner_id = target[0], target[1]
    my_player_name = mine[0]

    # Если пытаемся предложить обмен самому себе (через накрутку ID)
    if target_owner_id == m.from_user.id:
        return await m.answer("❌ Нельзя обмениваться с самим собой!")

    kb = InlineKeyboardBuilder()
    # Данные для кнопки: t_acc_ex_КТО_ПРЕДЛОЖИЛ_ЕГО_ПИД_ЦЕЛЬ_ПИД_ДЕНЬГИ
    cb_data = f"t_acc_ex_{m.from_user.id}_{data['ex_my_pid']}_{data['ex_target_pid']}_{money}"
    kb.button(text="✅ Принять обмен", callback_data=cb_data)
    kb.button(text="❌ Отклонить", callback_data=f"ref_b_{m.from_user.id}")
    kb.adjust(1)

    bonus_text = f" + доплата {money} млн €" if money > 0 else " (без доплаты)"

    try:
        await bot.send_message(
            target_owner_id,
            f"📩 ПРЕДЛОЖЕНИЕ ОБМЕНА!\n\n"
            f"За твоего игрока: {target_name}\n"
            f"Предлагают: {my_player_name}{bonus_text}\n"
            f"От: <a href='tg://user?id={m.from_user.id}'>{m.from_user.first_name}</a>",
            reply_markup=kb.as_markup(), parse_mode="HTML"
        )
        await m.answer(f"🚀 Предложение ({my_player_name} за {target_name}) отправлено владельцу!")
    except Exception as e:
        print(f"Ошибка отправки обмена: {e}")
        await m.answer("❌ Ошибка доставки: владелец заблокировал бота или ID неверен.")
    
    await state.clear()

# --- ТВОИ ОСТАЛЬНЫЕ ХЕНДЛЕРЫ БЕЗ ИЗМЕНЕНИЙ (Только поправил кнопку в back_to_market_main) ---

@dp.callback_query(F.data.startswith("m_target_"))
async def start_transfer_offer(cb: types.CallbackQuery, state: FSMContext):
    pid = int(cb.data.split("_")[2])
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT player_name, user_id, block_offers FROM squad WHERE id = ?', (pid,))
    res = c.fetchone(); conn.close()
    name, owner_id, is_blocked = res

    if is_blocked:
        return await cb.answer(f"❌ Владелец запретил торги по {name}!", show_alert=True)

    await state.update_data(trade_pid=pid, trade_owner=owner_id, trade_pname=name)
    await state.set_state(MarketStates.waiting_for_bid_price)
    await cb.message.answer(f"💰 Какую цену вы предлагаете за <b>{name}</b>? (Введите число в млн €):", parse_mode="HTML")
    await cb.answer()

@dp.callback_query(F.data.startswith("t_acc_cash_"))
async def accept_cash_offer(cb: types.CallbackQuery):
    # Распаковываем данные из колбэка
    # t_acc_cash_BUYERID_PID_PRICE
    data = cb.data.split("_")
    buyer_id = int(data[3])
    pid = int(data[4])
    price = float(data[5]) # Используем float, так как у тебя в балансе есть копейки
    seller_id = cb.from_user.id

    conn = get_db()
    c = conn.cursor()
    
    try:
        # 1. Получаем данные покупателя (баланс и его клуб)
        c.execute('SELECT balance, club FROM users WHERE user_id = ?', (buyer_id,))
        buyer = c.fetchone()
        
        if not buyer:
            return await cb.answer("❌ Покупатель не найден в базе.", show_alert=True)
            
        buyer_balance = float(buyer[0])
        buyer_club = buyer[1]

        # Проверка баланса
        if buyer_balance < price:
            return await cb.answer(f"❌ У покупателя не хватает денег! (Баланс: {buyer_balance})", show_alert=True)

        # 2. Проверяем наличие игрока у продавца
        c.execute('SELECT player_name FROM squad WHERE id = ? AND user_id = ?', (pid, seller_id))
        player = c.fetchone()
        
        if not player:
            return await cb.answer("❌ Этот игрок уже продан или не ваш.", show_alert=True)

        # 3. ФИНАНСОВАЯ ОПЕРАЦИЯ (Исправлено под накрутку)
        # Снимаем деньги у покупателя
        c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (price, buyer_id))
        # Начисляем продавцу
        c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (price, seller_id))
        
        # 4. ТРАНСФЕР ИГРОКА
        # Меняем владельца, клуб и сбрасываем статус продажи
        c.execute('''
            UPDATE squad 
            SET user_id = ?, 
                club = ?, 
                status = "bench", 
                market_price = 0 
            WHERE id = ?
        ''', (buyer_id, buyer_club, pid))
        
        conn.commit()
        
        # Уведомления
        await cb.message.edit_text(f"✅ Сделка завершена!\nИгрок {player[0]} продан за {price} млн €.", parse_mode="HTML")
        
        try:
            await bot.send_message(buyer_id, f"🎉 Поздравляем! Ты купил {player[0]} за {price} млн €!")
        except:
            pass # Если у покупателя заблокирован бот

    except Exception as e:
        print(f"ОШИБКА ПРИ ПОКУПКЕ: {e}")
        await cb.answer("🛑 Ошибка базы данных. Проверь консоль!")
    finally:
        conn.close()

@dp.callback_query(F.data.startswith("t_acc_ex_"))
async def accept_exchange_offer(cb: types.CallbackQuery):
    # Данные: [0]t, [1]acc, [2]ex, [3]offerer_id, [4]off_pid, [5]tar_pid, [6]money
    data = cb.data.split("_")
    offerer_id = int(data[3])
    off_pid = int(data[4])
    tar_pid = int(data[5])
    money = float(data[6])
    target_owner_id = cb.from_user.id

    conn = get_db(); c = conn.cursor()

    try:
        # 1. Получаем данные клубов обоих участников
        c.execute('SELECT club, balance FROM users WHERE user_id = ?', (offerer_id,))
        offerer_user = c.fetchone()
        c.execute('SELECT club, balance FROM users WHERE user_id = ?', (target_owner_id,))
        target_user = c.fetchone()

        if not offerer_user or not target_user:
            return await cb.answer("❌ Один из участников не найден в базе.", show_alert=True)

        offerer_club = offerer_user[0]
        offerer_balance = float(offerer_user[1])
        target_club = target_user[0]

        # 2. Проверяем наличие денег у инициатора обмена
        if offerer_balance < money:
            return await cb.answer("❌ У отправителя недостаточно денег для доплаты!", show_alert=True)

        # 3. Получаем данные игроков и проверяем их владельцев
        c.execute('SELECT player_name, user_id FROM squad WHERE id = ?', (off_pid,))
        p_off_data = c.fetchone()
        c.execute('SELECT player_name, user_id FROM squad WHERE id = ?', (tar_pid,))
        p_tar_data = c.fetchone()

        if not p_off_data or not p_tar_data:
            return await cb.answer("❌ Игрок не найден в базе.", show_alert=True)

        # Проверка: действительно ли игроки принадлежат участникам сделки
        if p_off_data[1] != offerer_id or p_tar_data[1] != target_owner_id:
            print(f"Ошибка ID: Предложил {offerer_id} (в базе {p_off_data[1]}), Принимает {target_owner_id} (в базе {p_tar_data[1]})")
            return await cb.answer("❌ Один из игроков уже сменил клуб.", show_alert=True)

        off_player_name = p_off_data[0]
        tar_player_name = p_tar_data[0]

        # --- ВЫПОЛНЕНИЕ ОБМЕНА ---
        # 1. Отдаем целевого игрока (tar_pid) отправителю в его клуб
        c.execute('''
            UPDATE squad 
            SET user_id = ?, club = ?, status = "bench", market_price = 0 
            WHERE id = ?
        ''', (offerer_id, offerer_club, tar_pid))

        # 2. Отдаем игрока отправителя (off_pid) владельцу целевого в его клуб
        c.execute('''
            UPDATE squad 
            SET user_id = ?, club = ?, status = "bench", market_price = 0 
            WHERE id = ?
        ''', (target_owner_id, target_club, off_pid))

        # 3. Проводим доплату, если она есть
        if money > 0:
            c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (money, offerer_id))
            c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (target_owner_id, money))

        conn.commit()

        # Уведомления об успехе
        await cb.message.edit_text(f"🔄 <b>Обмен совершен!</b>\n\n{tar_player_name} ↔️ {off_player_name}", parse_mode="HTML")
        
        try:
            await bot.send_message(offerer_id, f"✅ Обмен принят!\nВы получили <b>{tar_player_name}</b>, отдав <b>{off_player_name}</b>.", parse_mode="HTML")
        except:
            pass

    except Exception as e:
        print(f"Ошибка обмена: {e}")
        await cb.answer("🛑 Критическая ошибка при совершении сделки.")
    finally:
        conn.close()

@dp.message(MarketStates.waiting_for_bid_price)
async def send_bid_to_owner(m: types.Message, state: FSMContext):
    if not m.text.isdigit():
        return await m.answer("⚠️ Ошибка! Введите цену целым числом или напишите 'отмена'.")

    offer_price = int(m.text)
    data = await state.get_data()
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Принять", callback_data=f"t_acc_cash_{m.from_user.id}_{data['trade_pid']}_{offer_price}")
    kb.button(text="❌ Отклонить", callback_data=f"ref_b_{m.from_user.id}")
    kb.adjust(2)

    try:
        await bot.send_message(
            data['trade_owner'],
            f"📩 <b>ТРАНСФЕРНОЕ ПРЕДЛОЖЕНИЕ!</b>\n\n"
            f"Игрок: <b>{data['trade_pname']}</b>\n"
            f"Предложенная цена: <b>{offer_price} млн €</b>\n"
            f"От кого: <a href='tg://user?id={m.from_user.id}'>{m.from_user.first_name}</a>",
            reply_markup=kb.as_markup(), parse_mode="HTML"
        )
        await m.answer(f"🚀 Ваше предложение в {offer_price} млн € успешно отправлено!")
    except:
        await m.answer("❌ Ошибка отправки.")
    await state.clear()

@dp.callback_query(F.data.startswith("ref_b_"))
async def refuse_offer(cb: types.CallbackQuery):
    buyer_id = int(cb.data.split("_")[2])
    await cb.message.edit_text(f"{cb.message.text}\n\n❌ Вы отклонили это предложение.", parse_mode="HTML")
    try:
        await bot.send_message(buyer_id, "❌ Ваше предложение по игроку было отклонено владельцем.")
    except: pass
    await cb.answer()

@dp.callback_query(F.data == "back_to_market_main")
async def back_to_clubs(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT DISTINCT club FROM users WHERE club IS NOT NULL AND club != ""')
    clubs = c.fetchall(); conn.close()

    if not clubs:
        return await cb.answer("🛒 На рынке пока нет созданных клубов.")

    kb = InlineKeyboardBuilder()
    for (club_name,) in clubs:
        kb.button(text=f"🏟 {club_name}", callback_data=f"m_club_{club_name}")
    kb.adjust(2)
    
    await cb.message.edit_text(
        "🏪 Трансферный рынок\nВыберите клуб, чтобы просмотреть его игроков:", 
        reply_markup=kb.as_markup(), 
        parse_mode="HTML"
    )
    await cb.answer()

@dp.message(GameStates.setting_price)
async def market_sell(m: types.Message, state: FSMContext):
    if m.text and m.text.lower() == "отмена":
        await state.clear()
        return await m.answer("❌ Выставление игрока на рынок отменено.", reply_markup=get_main_kb(m.from_user.id))

    if not m.text.isdigit(): 
        return await m.answer("⚠️ Введите число (млн €) или напишите 'Отмена'!")
    
    uid = m.from_user.id
    price = int(m.text)
    
    # 1. Твоя проверка на количество игроков
    if check_squad_size(uid) <= 13:
        await state.clear()
        return await m.answer("❌ Нельзя выставить игрока! В команде должно остаться минимум 13 человек.")

    data = await state.get_data()
    pid = data.get("curr_pid")
    
    conn = get_db(); c = conn.cursor()
    
    # 2. ДОСТАЕМ РЕЙТИНГ ИГРОКА ДЛЯ ПРОВЕРКИ
    c.execute('SELECT player_name, rating FROM squad WHERE id = ?', (pid,))
    res = c.fetchone()
    
    if not res:
        conn.close()
        await state.clear()
        return await m.answer("❌ Ошибка: игрок не найден.")
    
    p_name, rat = res[0], int(res[1])

    # 3. ТА САМАЯ ЗАЩИТА (ЛИМИТЫ ЦЕН)
# Базовые значения (по умолчанию)
    min_p = 2
    max_p = 300

    # Сетка стала гораздо мягче, чтобы можно было продать "неликвид"
    if rat >= 95: 
        min_p, max_p = 80, 400   # Было 150
    elif rat >= 90: 
        min_p, max_p = 40, 300   # Было 100
    elif rat >= 85: 
        min_p, max_p = 15, 200   # Было 60 (теперь Джаку за 15-20 млн купят легко)
    elif rat >= 80: 
        min_p, max_p = 8, 100    # Было 30
    elif rat >= 75: 
        min_p, max_p = 3, 50     # Было 15
    elif rat >= 70: 
        min_p, max_p = 1, 20     # Было 5
    else:
        min_p, max_p = 1, 10

    if price < min_p:
        conn.close()
        # Мы НЕ очищаем state, чтобы юзер мог ввести цену еще раз
        return await m.answer(
            f"🚫 ЦЕНА СЛИШКОМ НИЗКАЯ!\n\n"
            f"Для рейтинга {rat} минималка: {min_p} млн €.\n"
            f"Твоя цена {price} млн € не подходит. Введи цену выше:"
        )
    
    if price > max_p:
        return await m.answer(
            f"🚫 Слишком дорого!\n"
            f"Для рейтинга {rat} потолок цены: {max_p} млн €.\n"
            f"Даже шейхи столько не заплатят. Сбавь аппетит!"
        )

    # 4. Если всё ок — выставляем
    try:
        c.execute('UPDATE squad SET status = "on_sale", market_price = ?, slot_id = NULL WHERE id = ?', (price, pid))
        conn.commit()
        await m.answer(f"✅ {p_name} выставлен за {price} млн €!", reply_markup=get_main_kb(uid))
    except Exception as e:
        print(f"Ошибка SQL: {e}")
        await m.answer("❌ Ошибка базы данных.")
    finally:
        conn.close()
        await state.clear()

# ШАГ 1: Выбор типа (Простая или с Выкупом)
@dp.callback_query(F.data.startswith("pre_loan_"))
async def process_loan_step_1(cb: types.CallbackQuery, state: FSMContext):
    if not is_transfer_open():
        return await cb.answer("🛑 Трансферное окно закрыто! Выставлять игроков нельзя.", show_alert=True)
    pid = cb.data.split("_")[2]
    await state.update_data(loan_pid=pid)
    
    b = InlineKeyboardBuilder()
    b.button(text="📄 Простая", callback_data="l_t_simple")
    b.button(text="💰 С выкупом", callback_data="l_t_buyout")
    await cb.message.edit_text("Тип аренды:", reply_markup=b.as_markup())
    await state.set_state(MarketStates.waiting_for_loan_type)

# ШАГ 2: Если выкуп — просим цену выкупа, если нет — идем к сроку
@dp.callback_query(MarketStates.waiting_for_loan_type)
async def process_loan_step_2(cb: types.CallbackQuery, state: FSMContext):
    l_type = cb.data.split("_")[2]
    await state.update_data(loan_type=l_type)
    
    if l_type == "buyout":
        await cb.message.edit_text("Введите цену будущего ВЫКУПА (млн €):")
        await state.set_state(MarketStates.waiting_for_buyout_price)
    else:
        await state.update_data(buyout_price=0)
        # Сразу прыгаем к выбору срока
        b = InlineKeyboardBuilder()
        b.button(text="⏳ Полгода", callback_data="l_d_1")
        b.button(text="🗓 Год", callback_data="l_d_2")
        await cb.message.edit_text("Выберите срок:", reply_markup=b.as_markup())
        await state.set_state(MarketStates.waiting_for_loan_duration)

# ШАГ 3: Получаем цену выкупа (только для типа "с выкупом")
@dp.message(MarketStates.waiting_for_buyout_price)
async def process_loan_step_3(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Введите число!")
    await state.update_data(buyout_price=int(m.text))
    
    b = InlineKeyboardBuilder()
    b.button(text="⏳ Полгода", callback_data="l_d_1")
    b.button(text="🗓 Год", callback_data="l_d_2")
    await m.answer("Выберите срок аренды:", reply_markup=b.as_markup())
    await state.set_state(MarketStates.waiting_for_loan_duration)

# ШАГ 4: Получаем срок и спрашиваем цену за АРЕНДУ (финал)
@dp.callback_query(MarketStates.waiting_for_loan_duration)
async def process_loan_step_4(cb: types.CallbackQuery, state: FSMContext):
    duration = int(cb.data.split("_")[2])
    await state.update_data(loan_duration=duration)
    await cb.message.edit_text("Введите стоимость АРЕНДЫ (заплатит сейчас, млн €):")
    await state.set_state(MarketStates.waiting_for_loan_fee)

# ШАГ 5: Запись в базу данных
@dp.message(MarketStates.waiting_for_loan_fee)
async def process_loan_step_5(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Введите число!")
    
    data = await state.get_data()
    loan_fee = int(m.text) # Цена сейчас
    buyout = data['buyout_price'] # Цена потом
    
    conn = get_db(); c = conn.cursor()
    # market_price — сколько стоит аренда сейчас
    # loan_term — цена выкупа (храним тут)
    # injury_type — пометка, что это аренда с выкупом (текст: buyout)
    c.execute('''UPDATE squad SET status = "loan_sale", market_price = ?, 
                 loan_expires_window = ?, loan_term = ?, injury_type = ? 
                 WHERE id = ?''', 
              (loan_fee, data['loan_duration'], buyout, data['loan_type'], data['loan_pid']))
    conn.commit(); conn.close()
    
    await m.answer(f"✅ Выставлен!\nАренда: {loan_fee} млн\nВыкуп: {buyout if buyout > 0 else 'Нет'}")
    await state.clear()

async def process_loan_returns():
    conn = get_db(); c = conn.cursor()
    # Ищем игроков, у которых закончился срок (loan_expires_window = 1)
    c.execute('''SELECT id, user_id, original_owner_id, player_name, loan_term, injury_type 
                 FROM squad WHERE original_owner_id IS NOT NULL AND loan_expires_window = 1''')
    expired = c.fetchall()

    for pid, renter_id, owner_id, p_name, b_price, l_type in expired:
        if l_type == "buyout":
            # Не возвращаем сразу! Спрашиваем арендатора.
            kb = InlineKeyboardBuilder()
            kb.button(text=f"✅ Выкупить за {b_price} млн", callback_data=f"conf_buyout_{pid}")
            kb.button(text="❌ Вернуть в клуб", callback_data=f"force_ret_{pid}")
            
            try:
                await bot.send_message(renter_id, 
                    f"🚨 Срок аренды <b>{p_name}</b> истек!\n"
                    f"Хотите выкупить игрока за <b>{b_price} млн €</b>?\n\n"
                    f"<i>Если нажмете 'Нет', он уйдет обратно.</i>",
                    reply_markup=kb.as_markup(), parse_mode="HTML")
            except: pass
        else:
            # Если простая аренда — возвращаем как обычно
            c.execute('''UPDATE squad SET user_id = ?, original_owner_id = NULL, 
                         loan_expires_window = 0, status = "bench" WHERE id = ?''', (owner_id, pid))

    # Уменьшаем срок у тех, кто еще остается в аренде
    c.execute('UPDATE squad SET loan_expires_window = loan_expires_window - 1 WHERE loan_expires_window > 1')
    conn.commit(); conn.close()

@dp.message(F.text == "🚀 Рынок")
async def show_market(m: types.Message):
    # --- ТВОЯ БЛОКИРОВКА В ГРУППЕ (БЕЗ ИЗМЕНЕНИЙ) ---
    if m.chat.id == -1003513118924: 
        try: await m.delete() 
        except: pass
        warn = await m.answer(
            f"⚠️ <b>{m.from_user.first_name}</b>, рынок доступен только в личных сообщениях!", 
            parse_mode="HTML"
        )
        await asyncio.sleep(10)
        try: await warn.delete()
        except: pass
        return 
    # --- КОНЕЦ БЛОКИРОВКИ ---

    if not is_transfer_open():
        return await m.answer("🛒 <b>Рынок закрыт.</b>\nДождитесь открытия трансферного окна!", parse_mode="HTML")
    
    # СОЗДАЕМ МЕНЮ ВЫБОРА
    kb = InlineKeyboardBuilder()
    kb.button(text="🏟 Обычный рынок (По клубам)", callback_data="open_clubs_market")
    kb.button(text="🔥 Выставленные игроки (Лоты)", callback_data="open_lots_market")
    kb.adjust(1)

    await m.answer(
        "🏪 <b>Трансферный центр</b>\n\n"
        "Выберите режим просмотра:\n\n"
        "• 🏟 <b>Обычный рынок</b> — просмотр всех клубов и поиск любого игрока.\n"
        "• 🔥 <b>Выставленные лоты</b> — игроки, которых менеджеры продают прямо сейчас.",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )

# --- РЕЖИМ 1: ПРОСМОТР ПО КЛУБАМ ---
@dp.callback_query(F.data == "open_clubs_market")
async def open_clubs_market(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT DISTINCT club FROM users WHERE club IS NOT NULL AND club != ""')
    clubs = c.fetchall(); conn.close()

    if not clubs:
        return await cb.answer("🛒 На рынке пока нет созданных клубов.", show_alert=True)

    kb = InlineKeyboardBuilder()
    for (club_name,) in clubs:
        kb.button(text=f"🏟 {club_name}", callback_data=f"m_club_{club_name}")
    
    kb.adjust(2)
    kb.button(text="⬅️ Назад в меню", callback_data="back_to_main_market")
    
    await cb.message.edit_text(
        "🏟 <b>ПОИСК ПО КЛУБАМ</b>\nВыберите клуб, чтобы посмотреть его состав и сделать предложение:",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )

# --- РЕЖИМ 2: ПРОСМОТР ЛОТОВ (ТВОЯ ЛОГИКА) ---
@dp.callback_query(F.data == "open_lots_market")
async def open_lots_market(cb: types.CallbackQuery):
    conn = get_db()
    c = conn.cursor()
    
    # ИСПОЛЬЗУЕМ JOIN: Берем актуальный клуб из таблицы users по seller_id
    c.execute('''
        SELECT s.id, s.player_name, s.rating, s.market_price, u.club, s.user_id, s.pos, s.status 
        FROM squad s
        LEFT JOIN users u ON s.user_id = u.user_id
        WHERE (s.status IN ("on_sale", "loan_sale")) AND s.market_price > 0
    ''') 
    lots = c.fetchall()
    conn.close()

    if not lots:
        return await cb.answer("🔥 На рынке пока нет активных лотов.", show_alert=True)

    await cb.message.delete()
    
    for lid, name, rat, price, actual_club, seller_id, pos, status in lots:
        is_my_lot = (seller_id == cb.from_user.id)
        
        # Если seller_id = 0 (админский дроп) или клуб не найден в users
        if seller_id == 0:
            club_display = "Свободный агент 🌍"
        else:
            club_display = actual_club if actual_club else "Без клуба 🏳️"
            
        deal_type = "🤝 Аренда" if status == "loan_sale" else "💰 Продажа"
        owner_label = " (Ваш лот 👤)" if is_my_lot else ""
        
        text = (
            f"👤 <b>{name}</b> [{rat}]{owner_label}\n"
            f"🏃 Позиция: <b>{pos}</b>\n"
            f"🏟 Клуб: <b>{club_display}</b>\n"
            f"📝 Тип: <b>{deal_type}</b>\n"
            f"💰 Цена: <b>{price} млн €</b>"
        )
        
        b = InlineKeyboardBuilder()
        
        if is_my_lot:
            b.button(text="❌ Снять с продажи", callback_data=f"remove_lot_{lid}")
        else:
            b.button(text="💵 Купить", callback_data=f"buy_menu_{lid}")
            b.button(text="📉 Торг", callback_data=f"bargain_{lid}")
            
            if seller_id != 0:
                b.button(text="💬 Чат", callback_data=f"chat_{seller_id}")
        
        b.adjust(2)
        await cb.message.answer(text, reply_markup=b.as_markup(), parse_mode="HTML")

    nav = InlineKeyboardBuilder()
    nav.button(text="⬅️ Назад в меню", callback_data="back_to_main_market")
    await cb.message.answer("Показаны все актуальные лоты.", reply_markup=nav.as_markup())

@dp.callback_query(F.data.startswith("remove_lot_"))
async def remove_from_sale(cb: types.CallbackQuery):
    pid = int(cb.data.split("_")[2])
    conn = get_db(); c = conn.cursor()
    
    # Возвращаем статус bench и обнуляем цену
    c.execute('UPDATE squad SET status = "bench", market_price = 0 WHERE id = ?', (pid,))
    conn.commit(); conn.close()
    
    await cb.message.edit_text(f"{cb.message.text}\n\n✅ Игрок снят с продажи!")
    await cb.answer("Снято!")

# --- ВОЗВРАТ В ГЛАВНОЕ МЕНЮ ---
@dp.callback_query(F.data == "back_to_main_market")
async def back_to_main_market(cb: types.CallbackQuery):
    await cb.message.delete()
    # Вызываем заново основную функцию
    await show_market(cb.message)

@dp.callback_query(F.data == "market_choice_clubs")
async def market_mode_clubs(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT DISTINCT club FROM users WHERE club IS NOT NULL AND club != ""')
    clubs = c.fetchall(); conn.close()

    if not clubs:
        return await cb.answer("🛒 Клубы пока не созданы.", show_alert=True)

    kb = InlineKeyboardBuilder()
    for (club_name,) in clubs:
        kb.button(text=f"🏟 {club_name}", callback_data=f"m_club_{club_name}")
    
    kb.adjust(2)
    kb.button(text="⬅️ Назад", callback_data="back_to_market_main_menu")
    
    await cb.message.edit_text(
        "🏟 <b>Поиск по клубам</b>\nВыберите клуб, чтобы посмотреть его состав:",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "market_choice_lots")
async def market_mode_lots(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    # Берем данные из таблицы лотов
    c.execute('SELECT id, player_name, rating, price, club, user_id, pos, status FROM market_lots')
    lots = c.fetchall(); conn.close()

    if not lots:
        return await cb.answer("🔥 Активных предложений пока нет.", show_alert=True)

    await cb.message.delete() # Удаляем меню выбора

    for lid, name, rat, price, club_name, seller_id, pos, status in lots:
        if seller_id == cb.from_user.id: continue

        deal_type = "🤝 Аренда" if status == "loan_sale" else "💰 Продажа"
        text = (
            f"👤 <b>{name}</b> [{rat}]\n"
            f"🏃 Позиция: <b>{pos}</b>\n"
            f"🏟 Клуб: <b>{club_name if club_name else '---'}</b>\n"
            f"📝 Тип: <b>{deal_type}</b>\n"
            f"💰 Цена: <b>{price} млн €</b>"
        )
        
        b = InlineKeyboardBuilder()
        buy_text = "💵 Купить" if status != "loan_sale" else "🤝 Арендовать"
        b.button(text=buy_text, callback_data=f"m_target_{lid}")
        
        if status != "loan_sale":
            b.button(text="🔄 Обмен", callback_data=f"ex_start_{lid}")
        
        b.button(text="💬 Чат", callback_data=f"chat_{seller_id}")
        b.adjust(2)
        await cb.message.answer(text, reply_markup=b.as_markup(), parse_mode="HTML")

    # Кнопка возврата в конце всех сообщений
    nav = InlineKeyboardBuilder()
    nav.button(text="⬅️ Назад в меню рынка", callback_data="back_to_market_main_menu")
    await cb.message.answer("Выше показаны все актуальные лоты.", reply_markup=nav.as_markup())

@dp.callback_query(F.data.startswith("tr_sel_"), MarketStates.waiting_for_trade_player)
async def trade_player_selected(cb: types.CallbackQuery, state: FSMContext):
    
    my_player_id = int(cb.data.split("_")[2])
    
    conn = get_db(); c = conn.cursor()
    
    c.execute('SELECT player_name, rating FROM squad WHERE id = ?', (my_player_id,))
    res = c.fetchone()
    conn.close()
    
    if not res:
        return await cb.answer("❌ Ошибка: игрок не найден в базе.", show_alert=True)
    
    p_name, rat = res
    
    
    await state.update_data(offer_player_id=my_player_id)
    
    
    await state.set_state(MarketStates.waiting_for_trade_cash)
    
    await cb.message.answer(
        f"✅ Вы выбрали: <b>{p_name}</b> ({rat})\n"
        f"Теперь введите сумму доплаты (млн €).\n"
        f"<i>Если доплата не нужна, просто введите 0.</i>",
        parse_mode="HTML"
    )
    
    # ОБЯЗАТЕЛЬНО закрываем "часики" на кнопке
    await cb.answer()

@dp.callback_query(F.data.startswith("bargain_"))
async def bargain_type_choice(cb: types.CallbackQuery):
    if not is_transfer_open():
        return await cb.answer("🛑 Трансферное окно закрыто!", show_alert=True)

    lot_id = int(cb.data.split("_")[1])
    
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id, status FROM squad WHERE id = ?', (lot_id,))
    res = c.fetchone()
    conn.close()
    
    if not res:
        return await cb.answer("❌ Игрок не найден!", show_alert=True)
        
    seller_id, status = res
    
    if seller_id == cb.from_user.id:
        return await cb.answer("🚫 Это твой собственный игрок!", show_alert=True)

    kb = InlineKeyboardBuilder()
    
    kb.button(text="💰 Предложить цену", callback_data=f"bid_c_{lot_id}")
      
    if status != "loan_sale":
        kb.button(text="🔄 Предложить обмен", callback_data=f"bid_t_{lot_id}")
    else:
        pass

    kb.adjust(1)
    
    await cb.message.edit_reply_markup(reply_markup=kb.as_markup())
    await cb.answer()

@dp.callback_query(F.data.startswith("bid_t_"))
async def start_trade_selection(cb: types.CallbackQuery, state: FSMContext):
    lot_id = int(cb.data.split("_")[2])
    await state.update_data(target_lot_id=lot_id)
    
    conn = get_db(); c = conn.cursor()
    # Теперь берем ВСЕХ игроков пользователя (и основу, и запас)
    c.execute('SELECT id, player_name, rating, pos, status FROM squad WHERE user_id = ?', (cb.from_user.id,))
    my_squad = c.fetchall()
    conn.close()
    
    if not my_squad:
        return await cb.answer("❌ У тебя нет игроков для обмена!", show_alert=True)

    kb = InlineKeyboardBuilder()
    for pid, name, rat, pos, stat in my_squad:
        # Добавим пометку, если игрок в основе
        prefix = "⭐️ " if stat == "active" else ""
        kb.button(text=f"{prefix}{name} ({rat}) [{pos}]", callback_data=f"tr_sel_{pid}")
    
    kb.adjust(1)
    await cb.message.answer("Кого из своих игроков предложишь взамен?\n(⭐️ — игрок основы)", reply_markup=kb.as_markup())
    await state.set_state(MarketStates.waiting_for_trade_player)

@dp.message(MarketStates.waiting_for_trade_cash)
async def send_trade_offer(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Введите число!")
    
    cash = int(m.text)
    data = await state.get_data()
    target_id = data['target_lot_id'] # Игрок на рынке
    offer_id = data['offer_player_id'] # Игрок покупателя
    
    conn = get_db(); c = conn.cursor()
    # Инфо о цели
    c.execute('SELECT player_name, rating, user_id FROM squad WHERE id = ?', (target_id,))
    t_res = c.fetchone()
    # Инфо о моем
    c.execute('SELECT player_name, rating FROM squad WHERE id = ?', (offer_id,))
    m_res = c.fetchone()
    conn.close()

    if not t_res or not m_res: return await m.answer("Ошибка данных.")

    t_name, t_rat, seller_id = t_res
    m_name, m_rat = m_res

    kb = InlineKeyboardBuilder()
    # callback: trade_accept_{кто_предложил}_{его_игрок}_{целевой_игрок}_{доплата}
    kb.button(text="✅ Принять обмен", callback_data=f"t_acc_{m.from_user.id}_{offer_id}_{target_id}_{cash}")
    kb.button(text="❌ Отклонить", callback_data=f"ref_b_{m.from_user.id}")

    await bot.send_message(
        seller_id if seller_id != 0 else ADMINS, # Если свободный агент — админу
        f"🔄 <b>ПРЕДЛОЖЕНИЕ ОБМЕНА!</b>\n\n"
        f"У вас хотят забрать: <b>{t_name}</b> ({t_rat})\n"
        f"Взамен отдают: <b>{m_name}</b> ({m_rat})\n"
        f"💰 Доплата вам: <b>{cash} млн €</b>\n\n"
        f"Согласны на такой обмен?",
        reply_markup=kb.as_markup(),
        parse_mode="HTML"
    )
    await m.answer("🚀 Предложение обмена отправлено владельцу!")
    await state.clear()

@dp.callback_query(F.data.startswith("t_acc_"))
async def accept_trade_final(cb: types.CallbackQuery):
    # t_acc_{buyer_id}_{buyer_pid}_{seller_pid}_{cash}
    parts = cb.data.split("_")
    b_id, b_pid, s_pid, cash = int(parts[2]), int(parts[3]), int(parts[4]), int(parts[5])
    s_id = cb.from_user.id 

    conn = get_db(); c = conn.cursor()
    
    # Считаем суммы в миллионах
    full_cash = cash * 1000000
    tax = int(full_cash * 0.10) # Налог 10%
    final_seller_money = full_cash - tax # Сколько получит продавец на руки

    # Проверка баланса покупателя
    c.execute('SELECT balance FROM users WHERE user_id = ?', (b_id,))
    res_bal = c.fetchone()
    
    if not res_bal or res_bal[0] < full_cash:
        conn.close()
        return await cb.message.answer("❌ У инициатора обмена не хватает денег на доплату!")

    try:
        # 1. Забираем игрока у продавца и отдаем покупателю
        # Сбрасываем slot_id, чтобы он исчез из основы
        c.execute('''
            UPDATE squad 
            SET user_id = ?, status = "bench", market_price = 0, slot_id = NULL 
            WHERE id = ?
        ''', (b_id, s_pid))
        
        # 2. Забираем игрока у покупателя и отдаем продавцу
        c.execute('''
            UPDATE squad 
            SET user_id = ?, status = "bench", market_price = 0, slot_id = NULL 
            WHERE id = ?
        ''', (s_id, b_pid))
        
        # 3. Финансовая часть с учетом комиссии
        if cash > 0:
            # С покупателя списываем ВСЮ сумму доплаты
            c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (full_cash, b_id))
            
            # Продавцу начисляем за вычетом 10%, если это не свободный агент (ID 0)
            if s_id != 0:
                c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (final_seller_money, s_id))
        
        conn.commit()

        # Красивый отчет о сделке
        tax_report = ""
        if cash > 0:
            tax_report = (
                f"\n💰 Доплата: <b>{cash} млн €</b>"
                f"\n🏦 Комиссия (10%): <b>{tax // 1000000} млн €</b>"
                f"\n💵 Получено на руки: <b>{final_seller_money // 1000000} млн €</b>"
            )

        await cb.message.edit_text(
            f"🤝 <b>Обмен успешно завершен!</b>\n"
            f"Игроки поменялись клубами и переведены в запас."
            f"{tax_report}", 
            parse_mode="HTML"
        )
        
        await bot.send_message(b_id, f"✅ Твой обмен принят! Игрок перешел в твой клуб.\nСписано: {cash} млн €.")
        
    except Exception as e:
        conn.rollback()
        print(f"Ошибка обмена с комиссией: {e}")
        await cb.answer("Ошибка базы данных.")
    finally:
        conn.close()


@dp.callback_query(F.data.startswith("bid_c_"))
async def start_cash_bargain(cb: types.CallbackQuery, state: FSMContext):
    lot_id = cb.data.split("_")[2]
    await state.update_data(bid_lot_id=lot_id)
    await state.set_state(MarketStates.waiting_for_bid_price)
    
    await cb.message.answer("💰 Введите цену (в млн €), которую вы готовы предложить:")
    await cb.answer() # Убирает "часики" с кнопки


@dp.callback_query(F.data.startswith("bid_t_"))
async def start_trade_bargain(cb: types.CallbackQuery, state: FSMContext):
    lot_id = int(cb.data.split("_")[2])
    
    # ПРОВЕРКА: Не свой ли это лот (на всякий случай)
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id FROM squad WHERE id = ?', (lot_id,))
    res = c.fetchone()
    conn.close()

    if res and res[1] == "loan_sale":
        return await cb.answer("🚫 Обмен для арендных игроков недоступен!", show_alert=True)
    
    if res and res[0] == cb.from_user.id:
        return await cb.answer("🚫 Это твой игрок!", show_alert=True)

    await state.update_data(target_lot_id=lot_id)
    
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT id, player_name, rating, pos, status FROM squad WHERE user_id = ?', (cb.from_user.id,))
    my_squad = c.fetchall()
    conn.close()
    
    if not my_squad:
        return await cb.answer("❌ У тебя нет игроков для обмена!", show_alert=True)

    kb = InlineKeyboardBuilder()
    for pid, name, rat, pos, stat in my_squad:
        prefix = "⭐️ " if stat == "active" else ""
        kb.button(text=f"{prefix}{name} ({rat}) [{pos}]", callback_data=f"tr_sel_{pid}")
    
    kb.adjust(1)
    await cb.message.answer("Кого из своих игроков предложишь взамен?", reply_markup=kb.as_markup())
    await state.set_state(MarketStates.waiting_for_trade_player)
    await cb.answer()

@dp.callback_query(F.data.startswith("player_info_"))
async def show_player_info(cb: types.CallbackQuery):
    player_id = int(cb.data.split("_")[2])
    
    conn = get_db(); c = conn.cursor()
    c.execute('''SELECT player_name, rating, pos, market_price FROM squad WHERE id = ?''', (player_id,))
    res = c.fetchone()
    conn.close()
    
    if not res:
        return await cb.answer("Игрок не найден!", show_alert=True)
    
    name, rat, pos, price = res
    
    # Можно добавить описание в зависимости от позиции или рейтинга
    descriptions = {
        "GK": "Надежный страж ворот, готовый спасать в безнадежных ситуациях.",
        "DEF": "Бетон в защите. Пройти его практически невозможно.",
        "MID": "Маэстро центра поля, видит поле на 360 градусов.",
        "FWD": "Прирожденный бомбардир. Каждый удар — угроза."
    }
    desc = descriptions.get(pos, "Звезда мирового уровня.")

    info_text = (
        f"🌟 <b>Досье игрока: {name}</b>\n"
        f"────────────────────\n"
        f"📊 Рейтинг: <b>{rat}</b>\n"
        f"🏃 Позиция: <b>{pos}</b>\n"
        f"💰 Оценка: <b>{price} млн €</b>\n\n"
        f"📝 <i>{desc}</i>\n"
        f"────────────────────\n"
        f"📍 Свободный агент доступен для прямого выкупа или торга с администрацией."
    )
    
    await cb.message.answer(info_text, parse_mode="HTML")
    await cb.answer()

@dp.callback_query(F.data.startswith("ref_b_"))
async def refuse_bid_callback(cb: types.CallbackQuery):
    buyer_id = int(cb.data.split("_")[2])
    
    # Уведомляем продавца (меняем текст кнопки)
    await cb.message.edit_text(f"{cb.message.text}\n\n❌ <b>Вы отклонили это предложение.</b>", parse_mode="HTML")
    
    # Уведомляем покупателя
    try:
        await bot.send_message(buyer_id, "❌ Твоё предложение по торгу было отклонено продавцом.")
    except:
        pass
    
    await cb.answer("Отклонено")

def is_transfer_open():
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute('SELECT value FROM settings WHERE key = "transfer_window"')
        res = c.fetchone()
        conn.close()
        
        if res:
            val = str(res[0]).strip() # Убираем лишние пробелы и приводим к строке
            print(f"DEBUG: transfer_window в базе = '{val}'") # Увидишь в консоли
            return val == "1"
        return False
    except Exception as e:
        print(f"ОШИБКА БД в is_transfer_open: {e}")
        return False

@dp.message(MarketStates.waiting_for_bid_price)
async def process_bargain_bid(m: types.Message, state: FSMContext):
    if not m.text.isdigit():
        return await m.answer("⚠️ Введите число!")

    bid_price = int(m.text)
    data = await state.get_data()
    lot_id = data.get("bid_lot_id")

    conn = get_db(); c = conn.cursor()
    # Достаем рейтинг игрока
    c.execute('SELECT player_name, rating, user_id FROM squad WHERE id = ?', (lot_id,))
    res = c.fetchone()
    
    if not res:
        conn.close()
        return await m.answer("❌ Игрок не найден.")

    p_name, rat, seller_id = res[0], int(res[1]), res[2]

    # 1. РАССЧИТЫВАЕМ РЫНОЧНЫЙ МИНИМУМ
    market_min = 1
    max_p = 250

    if rat >= 95: 
        market_min, market_max = 150, 250
    elif rat >= 90: 
        market_min, market_max = 80, 250
    elif rat >= 85: 
        market_min, market_max = 40, 150
    elif rat >= 80: 
        market_min, market_max = 15, 100
    elif rat >= 75: 
        market_min, market_max = 15, 60
    elif rat >= 70: 
        market_min, market_max = 5, 20
    else:
        market_min, market_max = 1, 10

    # 2. ДЕЛАЕМ СКИДКУ ДЛЯ ТОРГА
    bargain_min = int(market_min * 0.7) 
    bargain_max = int(market_max * 1.1)

    if bid_price < bargain_min:
        conn.close()
        return await m.answer(
            f"🚫 Слишком нагло!\n\n"
            f"Для рейтинга {rat} даже с торгом нельзя ставить меньше {bargain_min} млн €.\n"
            f"Попробуй предложить цену чуть выше."
        )
    
    if bid_price > bargain_max:
        conn.close()
        return await m.answer(
            f"🚫 <b>Цена завышена!</b>\n\n"
            f"Максимальная цена для игрока с рейтингом {rat} составляет <b>{bargain_max} млн €</b>.\n"
            f"Даже при торге нельзя предлагать больше этой суммы.",
            parse_mode="HTML"
        )

    # ОПРЕДЕЛЯЕМ ПОЛУЧАТЕЛЯ
    if seller_id == 0:
        # Если свободный агент, шлем первому админу из списка
        target_ids = ADMINS if isinstance(ADMINS, list) else [ADMINS]
        title_text = "🚀 <b>Торг по Свободному Агенту!</b>"
    else:
        # Если обычный игрок, шлем ЕГО ВЛАДЕЛЬЦУ
        target_ids = [seller_id]
        title_text = "🤝 <b>Предложение по торгу!</b>"

    for t_id in target_ids:
        try:
            builder = InlineKeyboardBuilder()
            builder.button(text="✅ Принять", callback_data=f"a_{lot_id}_{bid_price}_{m.from_user.id}")
            builder.button(text="❌ Отклонить", callback_data=f"ref_b_{m.from_user.id}")
            
            await bot.send_message(
                t_id,
                f"{title_text}\n\n"
                f"За игрока <b>{p_name}</b> ({rat}) предлагают <b>{bid_price} млн €</b>.\n"
                f"На рынке он стоит минимум {market_min} млн.\n\n"
                f"Покупатель: {m.from_user.first_name}\n"
                f"Принимаешь?",
                reply_markup=builder.as_markup(),
                parse_mode="HTML"
            )
        except Exception as e:
            print(f"Ошибка отправки на {t_id}: {e}")

    await m.answer(f"✅ Предложение в {bid_price} млн € отправлено владельцу!")
    await state.clear()

@dp.callback_query(F.data.startswith("a_"))
async def accept_bid_callback(cb: types.CallbackQuery):
    await cb.answer("♻️ Оформление трансфера...")
    parts = cb.data.split("_")
    # a_{lot_id}_{bid_price}_{buyer_id}
    lid, price_short, buyer_id = int(parts[1]), int(parts[2]), int(parts[3])

    with get_db() as conn:
        c = conn.cursor()
        
        # 1. Получаем данные игрока и текущее системное полугодие
        c.execute('SELECT value FROM settings WHERE key = "current_half"')
        ch_res = c.fetchone()
        current_half = int(ch_res[0]) if ch_res else 1

        c.execute('''SELECT player_name, rating, pos, status, loan_expires_window, user_id 
                     FROM squad WHERE id = ?''', (lid,))
        player = c.fetchone()
        
        if not player:
            return await cb.message.edit_text("❌ Ошибка: игрок не найден.")
        
        name, rat, pos, old_status, loan_val, seller_id = player

        # Расчет денег
        full_price = price_short * 1000000
        net_profit = int(full_price * 0.9) # 90% продавцу

        # 2. Проверка баланса покупателя
        c.execute('SELECT balance FROM users WHERE user_id = ?', (buyer_id,))
        b_bal = c.fetchone()
        if not b_bal or b_bal[0] < full_price:
            return await cb.message.edit_text("❌ У покупателя нет столько денег.")

        try:
            # --- ФИНАНСЫ ---
            c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (full_price, buyer_id))
            if seller_id != 0:
                c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (net_profit, seller_id))

            # --- ТРАНСФЕР (Удаление старого -> Создание нового) ---
            c.execute('DELETE FROM squad WHERE id = ?', (lid,))

            if old_status == "loan_sale":
                # ЛОГИКА АРЕНДЫ (0.5 или 1 год)
                # loan_val у тебя может приходить как 1 (полгода) или 2 (год)
                # Если 0.5 года (1 этап) — возвращаем в следующем полугодии
                # Если 1 год (2 этапа) — возвращаем через одно (т.е. в это же полугодие, но через круг)
                
                if loan_val == 1: # На полгода
                    expire_at = 2 if current_half == 1 else 1
                else: # На год
                    expire_at = current_half
                
                c.execute('''INSERT INTO squad (user_id, player_name, rating, pos, status, 
                                               original_owner_id, loan_expires_window, slot_id)
                             VALUES (?, ?, ?, ?, "loaned", ?, ?, NULL)''', 
                          (buyer_id, name, rat, pos, seller_id, expire_at))
                
                term_text = "0.5 года" if loan_val == 1 else "1 год"
                msg = f"🤝 <b>{name}</b> ушел в аренду на {term_text}!"
            else:
                # ОБЫЧНАЯ ПРОДАЖА
                c.execute('''INSERT INTO squad (user_id, player_name, rating, pos, status, slot_id)
                             VALUES (?, ?, ?, ?, "bench", NULL)''', 
                          (buyer_id, name, rat, pos))
                msg = f"✅ <b>{name}</b> продан навсегда!"

            conn.commit()

            if rat >= 85:
                c.execute('''INSERT INTO academy_stats (user_id, stars_sold) 
                             VALUES (?, 1) 
                             ON CONFLICT(user_id) DO UPDATE SET stars_sold = stars_sold + 1''', (seller_id,))
                conn.commit()

            # Отчеты
            await cb.message.edit_text(
                f"{msg}\n💰 Выручка: +{net_profit // 1000000} млн €", 
                parse_mode="HTML"
            )
            
            await bot.send_message(
                buyer_id, 
                f"🎉 Сделка закрыта! <b>{name}</b> теперь в вашем составе (в запасе).", 
                parse_mode="HTML"
            )

        except Exception as e:
            conn.rollback()
            await cb.message.answer(f"⚠️ Ошибка трансфера: {e}")

async def process_loan_returns():
    conn = get_db(); c = conn.cursor()
    
    # 1. Ищем всех, у кого срок аренды закончился (был 1, стал пора возвращать)
    c.execute('''SELECT id, user_id, original_owner_id, player_name, loan_term, injury_type 
                 FROM squad 
                 WHERE original_owner_id IS NOT NULL AND loan_expires_window = 1''')
    expired = c.fetchall()

    for pid, renter_id, owner_id, p_name, b_price, l_type in expired:
        if l_type == "buyout" and b_price > 0:
            # СЛУЧАЙ А: Аренда с выкупом — спрашиваем арендатора
            kb = InlineKeyboardBuilder()
            kb.button(text=f"✅ Выкупить за {b_price} млн", callback_data=f"conf_buyout_{pid}")
            kb.button(text="❌ Вернуть в клуб", callback_data=f"force_ret_{pid}")
            
            try:
                await bot.send_message(renter_id, 
                    f"🚨 Срок аренды <b>{p_name}</b> истек!\n"
                    f"Хотите выкупить игрока за <b>{b_price} млн €</b>?\n\n"
                    f"<i>Если откажетесь, он вернется к владельцу.</i>",
                    reply_markup=kb.as_markup(), parse_mode="HTML")
            except: pass
        else:
            # СЛУЧАЙ Б: Простая аренда — возвращаем автоматически
            c.execute('''UPDATE squad 
                         SET user_id = ?, original_owner_id = NULL, 
                             loan_expires_window = 0, status = "bench", slot_id = NULL 
                         WHERE id = ?''', (owner_id, pid))
            
            try:
                await bot.send_message(owner_id, f"✅ Игрок <b>{p_name}</b> вернулся из аренды.")
                await bot.send_message(renter_id, f"⌛ Аренда <b>{p_name}</b> завершена, игрок вернулся к владельцу.")
            except: pass

    # 2. ВАЖНО: Уменьшаем срок только тем, у кого он БОЛЬШЕ 1
    # Это уменьшит срок "Год" (2) до "Полгода" (1)
    c.execute('''UPDATE squad 
                 SET loan_expires_window = loan_expires_window - 1 
                 WHERE original_owner_id IS NOT NULL AND loan_expires_window > 1''')
    
    conn.commit(); conn.close()
    print(f"🔄 Обработка этапа завершена. Найдено возвратов: {len(expired)}")

# Хендлер для ВЫКУПА игрока
@dp.callback_query(F.data.startswith("conf_buyout_"))
async def confirm_buyout(cb: types.CallbackQuery):
    pid = cb.data.split("_")[2]
    uid = cb.from_user.id
    
    conn = get_db(); c = conn.cursor()
    # Проверяем цену выкупа и баланс
    c.execute("SELECT loan_term, player_name, original_owner_id FROM squad WHERE id = ?", (pid,))
    res = c.fetchone()
    if not res: return
    
    price, p_name, owner_id = res
    c.execute("SELECT balance FROM users WHERE user_id = ?", (uid,))
    balance = c.fetchone()[0]
    
    if balance < price:
        await cb.answer("❌ Недостаточно денег для выкупа!", show_alert=True)
        return

    # Списываем деньги, начисляем продавцу, убираем метку аренды
    c.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (price, uid))
    c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (price, owner_id))
    c.execute('''UPDATE squad SET original_owner_id = NULL, loan_expires_window = 0, 
                 loan_term = 0, injury_type = NULL, status = "bench" WHERE id = ?''', (pid,))
    conn.commit(); conn.close()
    
    await cb.message.edit_text(f"💰 Вы успешно выкупили <b>{p_name}</b> за {price} млн!", parse_mode="HTML")

# Хендлер для ВОЗВРАТА (если отказался выкупать)
@dp.callback_query(F.data.startswith("force_ret_"))
async def force_return(cb: types.CallbackQuery):
    pid = cb.data.split("_")[2]
    
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT original_owner_id, player_name FROM squad WHERE id = ?", (pid,))
    owner_id, p_name = c.fetchone()
    
    # Возвращаем владельцу
    c.execute('''UPDATE squad SET user_id = ?, original_owner_id = NULL, 
                 loan_expires_window = 0, status = "bench", injury_type = NULL WHERE id = ?''', 
              (owner_id, pid))
    conn.commit(); conn.close()
    
    await cb.message.edit_text(f"↩️ Игрок {p_name} возвращен владельцу.")

@dp.callback_query(F.data.startswith("conf_buyout_"))
async def confirm_buyout_handler(cb: types.CallbackQuery):
    pid = cb.data.split("_")[2]
    uid = cb.from_user.id
    
    conn = get_db(); c = conn.cursor()
    # Получаем данные об игроке: цену выкупа, имя и старого владельца
    c.execute("SELECT loan_term, player_name, original_owner_id FROM squad WHERE id = ?", (pid,))
    res = c.fetchone()
    
    if not res:
        return await cb.answer("❌ Игрок не найден в базе.")
    
    buyout_price, p_name, owner_id = res
    
    # Проверяем баланс покупателя
    c.execute("SELECT balance FROM users WHERE user_id = ?", (uid,))
    balance = c.fetchone()[0]
    
    if balance < buyout_price:
        return await cb.answer(f"❌ Недостаточно средств! Нужно {buyout_price} млн €", show_alert=True)

    # ПРОВЕДЕНИЕ СДЕЛКИ:
    # 1. Списываем у покупателя
    c.execute("UPDATE users SET balance = balance - ? WHERE user_id = ?", (buyout_price, uid))
    # 2. Начисляем продавцу (старому владельцу)
    c.execute("UPDATE users SET balance = balance + ? WHERE user_id = ?", (buyout_price, owner_id))
    # 3. Делаем игрока полноценным (убираем метки аренды)
    c.execute('''UPDATE squad SET 
                 original_owner_id = NULL, 
                 loan_expires_window = 0, 
                 loan_term = 0, 
                 injury_type = NULL, 
                 status = "bench" 
                 WHERE id = ?''', (pid,))
    
    conn.commit(); conn.close()
    
    await cb.message.edit_text(f"✅ Сделка закрыта! <b>{p_name}</b> теперь ваш полноценный игрок за {buyout_price} млн €.", parse_mode="HTML")
    # Опционально: можно отправить сообщение старому владельцу, что игрока выкупили

# 2. Если пользователь нажал "Вернуть в клуб"
@dp.callback_query(F.data.startswith("force_ret_"))
async def force_return_handler(cb: types.CallbackQuery):
    pid = cb.data.split("_")[2]
    
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT original_owner_id, player_name FROM squad WHERE id = ?", (pid,))
    res = c.fetchone()
    
    if not res:
        return await cb.answer("❌ Ошибка при возврате.")
        
    owner_id, p_name = res
    
    # Просто возвращаем игрока владельцу
    c.execute('''UPDATE squad SET 
                 user_id = ?, 
                 original_owner_id = NULL, 
                 loan_expires_window = 0, 
                 status = "bench", 
                 injury_type = NULL 
                 WHERE id = ?''', (owner_id, pid))
    
    conn.commit(); conn.close()
    
    await cb.message.edit_text(f"↩️ Вы отказались от выкупа. Игрок <b>{p_name}</b> вернулся к прежнему владельцу.", parse_mode="HTML")

@dp.callback_query(F.data.startswith("bargain_"))
async def bargain_start(cb: types.CallbackQuery, state: FSMContext):
    if not is_transfer_open():
        return await cb.answer("🛑 Трансферное окно закрыто! Торговаться нельзя.", show_alert=True)

    lot_id = cb.data.split("_")[1]
    buyer_id = cb.from_user.id # Тот, кто хочет поторговаться
    
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id FROM squad WHERE id = ?', (lot_id,))
    res = c.fetchone()
    conn.close()
    
    if res:
        seller_id = res[0] # Владелец игрока
        
        if seller_id == buyer_id:
            return await cb.answer("🚫 Нельзя торговаться с самим собой!", show_alert=True)
    
    # Если всё ок, идем дальше
    await cb.message.answer("Введите вашу цену для торга:")
    await state.update_data(bid_lot_id=lot_id)
    await state.set_state(MarketStates.waiting_for_bid_price)

    await cb.message.answer("💰 Торг начат!\nВведите цену (в млн €), которую вы готовы предложить:")

@dp.message(MarketStates.waiting_for_bid_price) # Проверь, что это состояние совпадает с тем, что в классе!
async def set_market_price_final(m: types.Message, state: FSMContext):
    # 1. Сразу проверяем, что ввели число
    if not m.text.isdigit():
        return await m.answer("⚠️ Введите число (млн €)!")

    price = int(m.text)
    data = await state.get_data()
    # Убедись, что ключ 'sell_player_id' или 'bid_lot_id' совпадает с тем, что ты сохранял ранее!
    pid = data.get("sell_player_id") or data.get("bid_lot_id") 

    conn = get_db(); c = conn.cursor()
    # 2. Берем рейтинг
    c.execute('SELECT player_name, rating FROM squad WHERE id = ?', (pid,))
    res = c.fetchone()
    
    if res:
        p_name, rat = res[0], int(res[1])
        
        # 3. ЖЕСТКИЕ ПОРОГИ (Специально упростил для теста)
        min_p = 5
        if rat >= 90: min_p = 100
        elif rat >= 85: min_p = 70
        elif rat >= 80: min_p = 50
        elif rat >= 75: min_p = 20
        elif rat >= 70: min_p = 5

        # 4. САМА ПРОВЕРКА
        if price < min_p:
            conn.close()
            return await m.answer(f"🚫 НИЗКАЯ ЦЕНА!\nДля рейтинга {rat} минимум — {min_p} млн €.")

        # 5. ЗАПИСЬ (Только если прошли проверку!)
        c.execute('''
            UPDATE squad 
            SET market_price = ?, status = "on_sale" 
            WHERE id = ?
        ''', (price, pid))
        
        conn.commit()
        await m.answer(f"✅ {p_name} выставлен на рынок за {price} млн €!")
    
    conn.close()
    await state.clear()


# Или через callback, если хочешь просто вывести username
@dp.callback_query(F.data.startswith("chat_"))
async def transfer_chat(cb: types.CallbackQuery):
    # Получаем ID продавца из callback_data
    seller_id = int(cb.data.split("_")[1])
    buyer_id = cb.from_user.id
    
    # 1. Проверка: не сам ли это продавец нажал
    if seller_id == buyer_id:
        return await cb.answer(
            "📱 Это твой собственный лот.", 
            show_alert=True
        )
    
    # 2. Пытаемся достать username продавца из базы (или просто используем ID)
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT username FROM users WHERE user_id = ?', (seller_id,))
    res = c.fetchone()
    conn.close()
    
    username = res[0] if res and res[0] else None

    # 3. Формируем ответ
    if username:
        # Если есть юзернейм, даем прямую ссылку
        text = f"✉️ Связаться с владельцем: @{username}\n\nНапиши ему в личку, чтобы обсудить трансфер!"
        await cb.message.answer(text)
    else:
        # Если юзернейма нет, даем ссылку через ID (tg://user?id=...)
        # Внимание: такая ссылка работает, только если у продавца нет запрета в настройках конфиденциальности
        builder = InlineKeyboardBuilder()
        builder.button(text="Написать продавцу", url=f"tg://user?id={seller_id}")
        await cb.message.answer(
            "У продавца не указан @username, попробуй написать через профиль:", 
            reply_markup=builder.as_markup()
        )
    
    await cb.answer()

@dp.callback_query(F.data.startswith("remove_m_"))
async def remove_sale(cb: types.CallbackQuery):
    pid = cb.data.split("_")[2]
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE squad SET status = "bench", market_price = 0 WHERE id = ? AND user_id = ?', (pid, cb.from_user.id))
    conn.commit(); conn.close()
    await cb.answer("Снято с продажи"); await cb.message.delete()

@dp.message(MarketStates.waiting_for_sell_price)
async def set_market_price(m: types.Message, state: FSMContext):
    print("--- ДИАГНОСТИКА ЗАПУЩЕНА ---") # Увидишь в консоли
    
    if not m.text.isdigit():
        return await m.answer("Введите число!")

    price = int(m.text)
    data = await state.get_data()
    player_id = data.get("sell_player_id")

    conn = get_db(); c = conn.cursor()
    c.execute('SELECT player_name, rating FROM squad WHERE id = ?', (player_id,))
    res = c.fetchone()
    
    if not res:
        print("ОШИБКА: Игрок не найден в базе")
        conn.close()
        return await m.answer("Игрок не найден.")

    p_name = res[0]
    raw_rating = res[1]
    
    # ПРЕОБРАЗУЕМ В ЧИСЛО ТУТ
    try:
        rat = int(raw_rating)
    except:
        rat = 0
        print(f"ОШИБКА: Рейтинг игрока '{raw_rating}' не является числом!")

    print(f"Игрок: {p_name}, Рейтинг: {rat}, Введенная цена: {price}")

    # ЖЕСТКИЕ ЛИМИТЫ
    min_p = 1
    if rat >= 90: min_p = 100
    elif rat >= 85: min_p = 70
    elif rat >= 80: min_p = 50
    elif rat >= 75: min_p = 20
    elif rat >= 70: min_p = 5

    print(f"Рассчитанный минимум: {min_p}")

    if price < min_p:
        print(f"РЕЗУЛЬТАТ: Цена {price} отклонена, так как минимум {min_p}")
        conn.close()
        return await m.answer(f"❌ Слишком дешево! Минимум: {min_p} млн €")

    # Если дошли сюда — значит проверка ПРОЙДЕНА
    print("РЕЗУЛЬТАТ: Проверка пройдена, записываю в базу...")
    c.execute('UPDATE squad SET market_price = ? WHERE id = ?', (price, player_id))
    conn.commit()
    conn.close()
    await m.answer(f"✅ {p_name} на рынке за {price} млн €")
    await state.clear()

@dp.callback_query(F.data.startswith("buy_"))
async def buy_player(cb: types.CallbackQuery):
    if not is_transfer_open():
        return await cb.answer("🛑 Трансферное окно закрыто! Покупки временно недоступны.", show_alert=True)

    lot_id = int(cb.data.split("_")[1]) 
    buyer_id = cb.from_user.id 
    
    conn = get_db()
    c = conn.cursor()
    
    # Достаем данные (включая статус и параметры аренды)
    c.execute('''SELECT user_id, market_price, player_name, status, loan_expires_window 
                 FROM squad WHERE id = ?''', (lot_id,))
    res = c.fetchone()
    
    if not res or res[1] <= 0:
        conn.close()
        return await cb.answer("❌ Игрок уже продан или снят с рынка!", show_alert=True)

    seller_id, price_short, p_name, status, loan_duration = res
    full_price = price_short * 1000000 

    if seller_id == buyer_id:
        conn.close()
        return await cb.answer("🚫 Это твой собственный игрок!", show_alert=True)
    
    c.execute('SELECT balance FROM users WHERE user_id = ?', (buyer_id,))
    buyer_res = c.fetchone()
    if not buyer_res:
        conn.close()
        return await cb.answer("Ошибка: ты не зарегистрирован!")
    
    buyer_bal = buyer_res[0]
    
    if buyer_bal < full_price:
        conn.close()
        return await cb.answer(f"💰 Недостаточно денег! Нужно {price_short} млн €, а у тебя {buyer_bal // 1000000} млн €.", show_alert=True)

    tax = int(full_price * 0.10) 
    final_seller_money = full_price - tax

    try:
        # --- ФИКС БАГА С ДУБЛЯМИ (НОВОЕ) ---
        # Узнаем клуб продавца, чтобы пометить игрока как "ушедшего навсегда"
        c.execute('SELECT club FROM users WHERE user_id = ?', (seller_id,))
        s_club_res = c.fetchone()
        seller_club = s_club_res[0] if s_club_res else None

        # --- ФИКС БАЛАНСОВ ---
        c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (full_price, buyer_id))
        if seller_id != 0:
            c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (final_seller_money, seller_id))
        
        # --- ЛОГИКА АРЕНДЫ VS ПРОДАЖИ ---
        if status == "loan_sale":
            c.execute('SELECT value FROM settings WHERE key = "window_counter"')
            setting_res = c.fetchone()
            current_window = setting_res[0] if setting_res else 0
            expire_window = current_window + loan_duration

            c.execute('''
                UPDATE squad 
                SET user_id = ?, original_owner_id = ?, status = "bench", 
                    market_price = 0, slot_id = NULL, loan_expires_window = ? 
                WHERE id = ?
            ''', (buyer_id, seller_id, expire_window, lot_id))
            deal_type = "в аренду"
        else:
            # ПРОДАЖА НАВСЕГДА
            c.execute('''
                UPDATE squad 
                SET user_id = ?, original_owner_id = NULL, status = "bench", 
                    market_price = 0, slot_id = NULL, loan_expires_window = 0
                WHERE id = ?
            ''', (buyer_id, lot_id))
            
            # Если игрок продан навсегда, записываем его в черный список клуба
            if seller_club:
                c.execute('INSERT OR IGNORE INTO sold_originals (club_name, player_name) VALUES (?, ?)', 
                          (seller_club, p_name))
            
            deal_type = "навсегда"

        conn.commit()

        c.execute('SELECT rating FROM squad WHERE id = ?', (lot_id,))
        r_data = c.fetchone()
        if r_data and int(r_data[0]) >= 85:
            c.execute('''INSERT INTO academy_stats (user_id, stars_sold) 
                         VALUES (?, 1) 
                         ON CONFLICT(user_id) DO UPDATE SET stars_sold = stars_sold + 1''', (seller_id,))
            conn.commit()

        # УВЕДОМЛЕНИЯ
        new_bal_display = (buyer_bal - full_price) // 1000000
        await cb.message.edit_text(
            f"🎉 Поздравляем! Вы взяли <b>{p_name}</b> {deal_type} за <b>{price_short} млн €</b>!\n"
            f"Ваш баланс: <b>{new_bal_display} млн €</b>", 
            parse_mode="HTML"
        )
        await cb.answer("Сделка завершена!")

        if seller_id != 0:
            try:
                await bot.send_message(
                    seller_id, 
                    f"💰 <b>Сделка завершена!</b>\n\n"
                    f"Клуб купил/арендовал у вас игрока: <b>{p_name}</b>\n"
                    f"Зачислено (чистыми): <b>{final_seller_money // 1000000} млн €</b>",
                    parse_mode="HTML"
                )
            except: pass 

    except Exception as e:
        conn.rollback()
        print(f"КРИТИЧЕСКАЯ ОШИБКА ТРАНСФЕРА: {e}")
        await cb.answer("Ошибка базы данных.", show_alert=True)
    finally:
        conn.close()

@dp.message(F.text == "📋 Весь состав")
async def show_all_interactive(m: Union[types.Message, types.CallbackQuery], target_user_id: int = None):

    if isinstance(m, types.Message) and m.chat.id == -1003513118924:
        try:
            await m.delete() # Удаляем команду игрока
        except:
            pass
            
        warn = await m.answer(
            f"⚠️ <b>{m.from_user.first_name}</b>, просмотр состава доступен только в личке!\n"
            f"Не спамь кнопками в общем чате.", 
            parse_mode="HTML"
        )
        
        # Удаляем предупреждение через 7 секунд
        await asyncio.sleep(7)
        try:
            await warn.delete()
        except:
            pass
        return
    
    viewer_id = m.from_user.id
    owner_id = target_user_id if target_user_id else viewer_id
    is_owner = (viewer_id == owner_id)

    conn = get_db()
    c = conn.cursor()

    c.execute('''SELECT id, player_name, rating, pos, status, original_owner_id 
                 FROM squad 
                 WHERE user_id = ? AND training_until IS NULL
                 ORDER BY rating DESC''', (owner_id,))
    ps = c.fetchall()
    conn.close()
    
    if not ps: 
        return await m.answer("📭 В этом клубе пока нет игроков.")

    title = "📂 <b>Ваша картотека</b>" if is_owner else f"📂 <b>Картотека игрока</b>"
    text = (
        f"{title}\n"
        f"<i>{'Выберите игрока для управления.' if is_owner else 'Просмотр состава.'}</i>\n"
        f"⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯"
    )
    
    builder = InlineKeyboardBuilder()
    em = {"GK": "🧤", "DEF": "🛡", "MID": "🧠", "FWD": "🎯"}
    
    for row in ps:
        pid, name, rat, pos, stat, orig_owner = row
        
        if stat == "on_sale": s_icon = "💰"
        elif orig_owner and orig_owner != 0: s_icon = "🎭"
        elif stat in ["active", "main"]: s_icon = "🏃"
        else: s_icon = "🪑"
        
        # Если не владелец — колбэк ведет на заглушку
        cb_data = f"pl_{pid}" if is_owner else "view_only_info"
        
        builder.button(
            text=f"{em.get(pos, '⚽️')} {name} ({rat}) {s_icon}", 
            callback_data=cb_data 
        )
    
    builder.adjust(1) 
    
    if not is_owner:
        builder.row(types.InlineKeyboardButton(text="⬅️ Назад в профиль", callback_data=f"view_profile_{owner_id}"))

    footer = "\n⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n🏃 — старт | 🪑 — запас | 💰 — рынок"
    
    if isinstance(m, types.Message):
        await m.answer(text + footer, reply_markup=builder.as_markup(), parse_mode="HTML")
    else:
        await m.message.edit_text(text + footer, reply_markup=builder.as_markup(), parse_mode="HTML")

# Заглушка для чужих нажатий
@dp.callback_query(F.data == "view_only_info")
async def view_only_info(cb: types.CallbackQuery):
    await cb.answer("👀 Это чужой состав, вы не можете им управлять.", show_alert=False)

@dp.message(F.text == "💰 Баланс")
async def bal(m: types.Message):
    conn = get_db() 
    c = conn.cursor()
    
    try:
        c.execute('SELECT balance FROM users WHERE user_id = ?', (m.from_user.id,))
        res = c.fetchone()
        
        raw_balance = res[0] if res else 0
        
        # ЛОГИКА ФИКСА: 
        if raw_balance >= 1000000:
            clean_balance = int(raw_balance / 1000000)
        else:
            clean_balance = raw_balance

        await m.answer(
            f"💳 Ваш бюджет: <b>{clean_balance} млн €</b>", 
            parse_mode="HTML"
        )
        
    except Exception as e:
        print(f"Ошибка при проверке баланса: {e}")
        await m.answer("⚠️ Не удалось получить данные о балансе.")
    finally:
        # Это лечит ошибку "database is locked"
        conn.close()

@dp.callback_query(F.data == "admin_create_fa")
async def start_fa_creation(cb: types.CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMINS: return
    await cb.message.answer("📝 Введите ИМЯ игрока для ивента:")
    await state.set_state(AdminStates.waiting_for_fa_name)

@dp.message(AdminStates.waiting_for_fa_price)
async def finalize_fa(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return
    price = int(m.text)
    data = await state.get_data()
    
    conn = get_db(); c = conn.cursor()
    # Создаем игрока "из ниоткуда" (user_id = 0 или NULL означает, что он ничей)
    c.execute('''INSERT INTO squad (user_id, player_name, rating, pos, status, market_price, stamina) 
                 VALUES (0, ?, ?, ?, "free_agent", ?, 0)''', 
              (data['name'], data['rat'], data['pos'], price))
    fa_id = c.lastrowid
    conn.commit(); conn.close()
    
    # Кнопка для ловли
    b = InlineKeyboardBuilder()
    b.button(text=f"⚡️ ЗАБРАТЬ ЗА {price} МЛН", callback_data=f"catch_{fa_id}")
    
    # РАССЫЛКА ВО ВСЕ ЧАТЫ (или в один главный)
    await m.answer(f"✅ Игрок {data['name']} создан!")
    await bot.send_message(
        3556034012, # Замени на ID своего главного чата
        f"🚨 <b>МИНИ-ИВЕНТ: ЛОВЛЯ ИГРОКА!</b> 🚨\n\n"
        f"На рынок выброшен свободный агент:\n"
        f"👤 <b>{data['name']}</b> [{data['rat']}]\n"
        f"🎭 Позиция: {data['pos']}\n"
        f"💰 Цена: {price} млн €\n\n"
        f"Кто первый нажмет на кнопку — тот забирает!",
        reply_markup=b.as_markup(),
        parse_mode="HTML"
    )
    await state.clear()

@dp.callback_query(F.data.startswith("catch_"))
async def catch_player(cb: types.CallbackQuery):
    if not is_transfer_open():
        return await cb.answer("🛑 Рынок сейчас закрыт!", show_alert=True)
        
    fa_id = int(cb.data.split("_")[1])
    buyer_id = cb.from_user.id
    
    conn = get_db(); c = conn.cursor()
    
    # 1. СТРОГАЯ ПРОВЕРКА: Игрок всё еще свободен?
    c.execute('SELECT player_name, market_price, status FROM squad WHERE id = ?', (fa_id,))
    res = c.fetchone()
    
    if not res or res[2] != "free_agent":
        conn.close()
        return await cb.answer("😢 Опоздал! Игрока уже перехватили.", show_alert=True)
    
    name, price, status = res
    
    # 2. Проверка денег
    c.execute('SELECT balance FROM users WHERE user_id = ?', (buyer_id,))
    bal_res = c.fetchone()
    if not bal_res or bal_res[0] < price:
        conn.close()
        return await cb.answer("💰 Недостаточно денег для ловли!", show_alert=True)
    
    # 3. МОМЕНТАЛЬНЫЙ ЗАХВАТ
    try:
        # Списываем бабки
        c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (price, buyer_id))
        # Меняем владельца и статус
        c.execute('UPDATE squad SET user_id = ?, status = "bench", market_price = 0 WHERE id = ?', (buyer_id, fa_id))
        conn.commit()
        
        # Редактируем сообщение для всех: показываем победителя
        await cb.message.edit_text(
            f"✅ <b>ИГРОК ПОЙМАН!</b>\n\n"
            f"Счастливчик: <a href='tg://user?id={buyer_id}'>{cb.from_user.first_name}</a>\n"
            f"Игрок: <b>{name}</b>\n"
            f"Сумма сделки: {price} млн €",
            parse_mode="HTML"
        )
        await cb.answer("🎉 Поздравляем! Игрок твой!")
        
    except Exception as e:
        print(f"Ошибка ловли: {e}")
        await cb.answer("Ошибка базы данных.")
    finally:
        conn.close()

@dp.message(StateFilter("waiting_for_loan_price")) # Фильтруем именно это состояние
async def process_loan_market_final(m: types.Message, state: FSMContext):
    # 1. Проверяем, что ввели число
    if not m.text.isdigit():
        return await m.answer("⚠️ Введите число (млн €)!")

    price = int(m.text)
    data = await state.get_data()
    pid = data.get("loan_pid")
    duration = data.get("loan_duration") # Это то, что мы выбрали (1 или 2)

    if not pid:
        await state.clear()
        return await m.answer("❌ Ошибка: данные игрока потеряны. Попробуй заново.")

    conn = get_db()
    c = conn.cursor()

    try:
        # 2. Достаем инфу об игроке
        c.execute('SELECT player_name, rating FROM squad WHERE id = ?', (pid,))
        res = c.fetchone()
        
        if not res:
            conn.close()
            return await m.answer("❌ Игрок не найден в базе.")

        p_name, rat = res[0], int(res[1])

        # 3. ПРОВЕРКА МИНИМАЛКИ (как в продаже, только можно сделать чуть меньше)
        # Если хочешь, можешь убрать этот блок, если в аренде нет лимитов
        min_p = 2 
        if rat >= 90: min_p = 20
        elif rat >= 85: min_p = 15
        elif rat >= 80: min_p = 10

        if price < min_p:
            conn.close()
            return await m.answer(f"🚫 Слишком дешево для аренды {rat} рейтинга! Минимум: {min_p} млн €.")

        # 4. ОБНОВЛЯЕМ СТАТУС (loan_sale — признак аренды)
        # Мы сохраняем цену и на сколько окон уходит игрок
        c.execute('''
            UPDATE squad 
            SET status = "loan_sale", 
                market_price = ?, 
                loan_expires_window = ? 
            WHERE id = ?
        ''', (price, duration, pid))
        
        conn.commit()
        
        duration_text = "полгода (до след. ТО)" if duration == 1 else "год (через одно ТО)"
        await m.answer(
            f"✅ <b>{p_name}</b> выставлен в аренду!\n"
            f"💰 Цена: <b>{price} млн €</b>\n"
            f"⏳ Срок: <b>{duration_text}</b>",
            parse_mode="HTML"
        )

    except Exception as e:
        print(f"Ошибка при выставлении в аренду: {e}")
        await m.answer("❌ Произошла ошибка при записи в базу.")
    finally:
        conn.close()
        await state.clear() # ОБЯЗАТЕЛЬНО очищаем состояние

#---МАТЧИИИИИИИИИИИИИИИИИИИИИИИИИИИИИИИИИ---#
@dp.message(F.text == "⚽️ Играть (Бот)")
async def pre_match_check(m: types.Message):
    uid = m.from_user.id
    
    conn = get_db(); c = conn.cursor()
    
    try: # <--- Твоё не удаляем, просто оборачиваем для защиты
        # АВТО-ЧИСТКА: Если в основе сидят травмированные или забаненные — выкидываем их в запас
        c.execute('''UPDATE squad SET slot_id = NULL, status = "bench" 
                     WHERE user_id = ? AND slot_id IS NOT NULL 
                     AND (is_banned = 1 OR injury_remaining > 0)''', (uid,))
        conn.commit()

        # Теперь считаем только реально готовых
        c.execute('''SELECT id, player_name, rating, goals, assists, pos, stamina, slot_id 
                     FROM squad 
                     WHERE user_id = ? AND slot_id IS NOT NULL 
                     ORDER BY slot_id ASC''', (uid,))
        all_players_in_slots = c.fetchall()

        active_slots = [p[7] for p in all_players_in_slots if 1 <= p[7] <= 11]
        unique_slots_count = len(set(active_slots))

        if unique_slots_count < 11:
            # Убираем conn.close() отсюда, так как блок finally сделает это сам
            return await m.answer(
                f"❌ <b>Состав не готов!</b>\n\n"
                f"Заполнено живых позиций: {unique_slots_count}/11\n"
                f"<i>Зайдите в '📋 Состав' и заполните пустые места. Травмированные и забаненные были автоматически сняты с игры.</i>", 
                parse_mode="HTML"
            )
            
        # Если всё ок, берем строго первых 11 по списку слотов
        players = all_players_in_slots[:11]

        # 3. Достаем запасных
        c.execute('''SELECT id, player_name, rating, pos, stamina 
                     FROM squad 
                     WHERE user_id = ? AND slot_id IS NULL 
                     AND is_banned = 0 AND injury_remaining = 0''', (uid,))
        bench_raw = c.fetchall()

        # Получаем название клуба
        c.execute('SELECT club FROM users WHERE user_id = ?', (uid,))
        u_row = c.fetchone()
        my_club_name = u_row[0] if u_row else "Мой Клуб"

        # Предварительно вызываем рейтинг, пока база еще точно открыта
        avg_rating = get_squad_rating(uid)

    finally:
        # Это сработает ВСЕГДА: и при ошибке, и при return, и при успехе.
        # Теперь база никогда не будет оставаться "залоченной".
        conn.close()

    # --- Дальше идет твой блок формирования данных, он уже вне транзакции БД ---
    
    my_players = []
    lineup_details = "" 
    
    for p in players:
        p_id, p_name, p_rat, _, _, p_pos, p_stam, _ = p
        lineup_details += f"👤 {p_name} (⭐{p_rat}) | {p_pos} | 🔋{p_stam}\n"
        
        my_players.append({
            "db_id": p_id,
            "name": p_name, 
            "rating": p_rat, 
            "pos": str(p_pos).upper(), 
            "stamina": p_stam,
            "yc": 0
        })

    lineup_text = f"📋 <b>Ваш состав (Рейтинг: {avg_rating}):</b>\n\n{lineup_details}"

    bench = []
    for b in bench_raw:
        bench.append({
            "db_id": b[0], "name": b[1], "rating": b[2], 
            "pos": str(b[3]).upper(), "stamina": b[4]
        })

    # Сохраняем в matches_data
    matches_data[uid] = {
        "my_players": my_players,
        "my_name": my_club_name,
        "bench": bench,
        "used_players": [p["name"] for p in my_players],
        "substituted_out": [],
        "score_me": 0, "score_opp": 0,
        "minute": 1, "tactic": "Тики-така",
        "opp_name": "", "opp_players": [],
        "match_log": [], "is_paused": False, "needs_sub": False
    }

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🚀 Начать матч", callback_data="conf_m")],
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_m")]
    ])
    
    await m.answer(lineup_text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "conf_m")
async def start_match_callback(cb: types.CallbackQuery):
    uid = cb.from_user.id
    
    # 1. РАБОТА С БД ЧЕРЕЗ КОНТЕКСТНЫЙ МЕНЕДЖЕР
    try:
        with get_db() as conn:
            c = conn.cursor()
            
            # Проверка кулдауна и клуба
            c.execute('SELECT last_match, club FROM users WHERE user_id = ?', (uid,))
            user_row = c.fetchone()
            
            # ИСПРАВЛЕНО: Правильное получение текущего времени
            # Если у тебя "from datetime import datetime", то просто datetime.now()
            # Если "import datetime", то datetime.datetime.now()
            try:
                now = datetime.now() 
            except NameError:
                import datetime
                now = datetime.datetime.now()

            cooldown_minutes = 30 

            if user_row and user_row[0]:
                try:
                    # Важно: используй тот же стиль вызова, что и для now
                    if hasattr(datetime, 'fromisoformat'):
                        last_match_dt = datetime.fromisoformat(user_row[0])
                    else:
                        import datetime as dt_mod
                        last_match_dt = dt_mod.datetime.fromisoformat(user_row[0])
                    
                    # Расчет разницы
                    try:
                        delta = timedelta(minutes=cooldown_minutes)
                    except NameError:
                        import datetime as dt_mod
                        delta = dt_mod.timedelta(minutes=cooldown_minutes)
                        
                    next_match_dt = last_match_dt + delta
                    
                    if now < next_match_dt:
                        diff = next_match_dt - now
                        mins_left = int(diff.total_seconds() // 60)
                        return await cb.answer(f"⏳ Команда восстанавливается! Подожди {mins_left} мин.", show_alert=True)
                except Exception as e:
                    print(f"Ошибка парсинга даты: {e}")
                    pass 

            # Обновляем время матча и получаем имя клуба
            c.execute('UPDATE users SET last_match = ? WHERE user_id = ?', (now.isoformat(), uid))
            my_club = user_row[1] if user_row and user_row[1] else "Мой Клуб"
            
            conn.commit()
            # Здесь соединение закроется САМО благодаря "with"
            
    except Exception as db_err:
        print(f"❌ Ошибка БД: {db_err}")
        return await cb.answer("❌ База данных занята. Попробуй через секунду.", show_alert=False)

    # --- ОСТАЛЬНАЯ ЛОГИКА (ВНЕ БЛОКИРОВКИ БД) ---
    
    if uid not in matches_data:
        return await cb.answer("❌ Ошибка: данные матча устарели.", show_alert=True)

    await cb.message.edit_reply_markup(reply_markup=None)

    if not matches_data[uid]["opp_name"]:
        # Исключаем свой клуб из списка соперников
        available_opponents = [k for k in CLUBS.keys() if k != my_club]
        if not available_opponents: # Если ты один в списке
            available_opponents = list(CLUBS.keys())
            
        opp_name = random.choice(available_opponents)
        matches_data[uid]["opp_name"] = opp_name
        matches_data[uid]["opp_players"] = CLUBS[opp_name]['players']

    # ЗАПУСК
    await run_match_simulation(cb.message, uid)

async def run_match_simulation(msg, uid):
    data = matches_data[uid]
    
    # Базовые шансы
    current_goal_chance = 0.10
    current_card_chance = 0.12
    
    my_ovr = get_squad_rating(uid)
    opp_ovr = data.get("opp_rating", 85)
    
    # НОВАЯ ЛОГИКА ВЛИЯНИЯ ТАКТИК (Модификаторы: шанс забить, шанс пропустить)
    tactic_mods = {
        "Автобус": (0.5, 0.4),      # Мало забиваем, почти не пропускаем
        "Прессинг": (1.4, 1.3),     # Много атакуем, но устаем и ловим контры
        "Тики-така": (1.1, 0.8),    # Контроль мяча (баланс)
        "Контратака": (1.3, 0.9),   # Опасные вылазки
        "Бей-беги": (1.6, 1.7)      # Безумный футбол в обе стороны
    }
    
    # Берем модификаторы (если тактика не найдена — ставим баланс 1.0)
    mod_goal, mod_miss = tactic_mods.get(data.get("tactic", "Тики-така"), (1.0, 1.0))

    start_min = data["minute"]
    end_min = 45 if start_min < 45 else 90
    current_min = 5 if start_min == 1 else start_min
    
    # Применяем моды к шансам
    goal_prob = current_goal_chance * mod_goal
    miss_prob = current_goal_chance * mod_miss

    # --- ЦИКЛ МАТЧА ---
    for minute_step in range(current_min, end_min + 1, 5):
        if data.get("is_paused"): return 
        
        data["minute"] = minute_step
        display_min = max(1, min(minute_step + random.randint(-2, 2), end_min))
        roll = random.random()

        # 1. ПРОВЕРКА НА ТРАВМУ (Внутри цикла)
        # Шанс травмы зависит от накопленной усталости игроков
        injury_chance = 0.01  # Базовый шанс 1% каждые 5 минут
        if random.random() < injury_chance:
            # Считаем текущих травмированных в БД
            conn = get_db(); c = conn.cursor()
            c.execute('SELECT COUNT(*) FROM squad WHERE user_id = ? AND injury_remaining > 0', (uid,))
            total_injured = c.fetchone()[0]
            conn.close()

            # Если лимит (4) не превышен
            if total_injured < 4:
                # Выбираем случайного игрока с поля
                injured_player = random.choice(data["my_players"])
                
                # Добавляем в лог и ставим флаг блокировки
                data["match_log"].append(f"🚑 <b>{display_min}' ТРАВМА!</b> {injured_player['name']} не может продолжать! Матч не продолжится, пока вы его не замените")
                data["needs_sub"] = True # Флаг: нельзя продолжать без замены
                data["injured_slot_name"] = injured_player['name']
                
                # Принудительно обновляем сообщение и ВЫХОДИМ, чтобы остановить симуляцию
                await update_match_message(msg, uid)
                return

        # --- ЛОГИКА СОБЫТИЙ ---

        roll = random.random() 
        display_min = data['minute']

        # --- 1. КАРТОЧКИ (А - ОБЕ КОМАНДЫ) ---
        if random.random() < current_card_chance:
            if random.random() < 0.5:
                # ФОЛИТ ВАШ ИГРОК (Б)
                target = random.choice(data["my_players"])
                t_id = target.get('db_id') or target.get('id')
                
                if 'yc' not in target: target['yc'] = 0
                target['yc'] += 1
                data['match_yellows'] = data.get('match_yellows', 0) + 1
                
                # Логика: прямая красная (5%) ИЛИ вторая желтая
                is_red = random.random() < 0.05 or target['yc'] >= 2
                
                conn = get_db(); c = conn.cursor()
                if is_red:
                    data['match_reds'] = data.get('match_reds', 0) + 1
                    reason = "вторая ЖК" if target['yc'] >= 2 else "прямая красная"
                    data["match_log"].append(f"🟥 {display_min}' <b>УДАЛЕНИЕ!</b> {target['name']} ({reason})")
                    
                    if target in data["my_players"]: data["my_players"].remove(target)
                    # Сохраняем КК и баним в БД
                    c.execute('''UPDATE squad 
                                 SET yellow_cards = yellow_cards + 1, red_cards = red_cards + 1, 
                                     is_banned = 1, slot_id = NULL, status = 'bench' 
                                 WHERE id = ?''', (t_id,))
                else:
                    data["match_log"].append(f"🟨 {display_min}' ЖК: {target['name']}")
                    # Сохраняем ЖК в БД
                    c.execute('UPDATE squad SET yellow_cards = yellow_cards + 1 WHERE id = ?', (t_id,))
                conn.commit(); conn.close()
            else:
                # ФОЛИТ БОТ (В)
                opp_p = random.choice(data["opp_players"])
                if 'yc' not in opp_p: opp_p['yc'] = 0
                opp_p['yc'] += 1
                
                # Бот тоже может получить красную (удаляем из списка, чтобы OVR упал)
                if random.random() < 0.05 or opp_p['yc'] >= 2:
                    data["match_log"].append(f"🟥 {display_min}' <b>УДАЛЕНИЕ!</b> {opp_p['name']} — {data['opp_name']}")
                    if opp_p in data["opp_players"]: data["opp_players"].remove(opp_p)
                else:
                    data["match_log"].append(f"🟨 {display_min}' ЖК ({data['opp_name']}): {opp_p['name']}")

        # --- 2. ГОЛ ВАШЕЙ КОМАНДЫ (Б - С АССИСТАМИ) ---
        if roll < (current_goal_chance * mod_goal * (my_ovr / opp_ovr)):
            is_pen = random.random() < 0.15
            shooters = [p for p in data["my_players"] if p['pos'] in ['FWD', 'MID']]
            if not shooters: shooters = data["my_players"]

            if is_pen:
                scorer = sorted(shooters, key=lambda x: x['rating'], reverse=True)[0]
                log_entry = f"⚽️ {display_min}' <b>ПЕНАЛЬТИ!</b> {scorer['name']} точен!"
                assister = None
            else:
                scorer = random.choice(shooters)
                log_entry = f"⚽️ {display_min}' <b>ГОООЛ!</b> {scorer['name']}"
                
                # ВЫБОР АССИСТЕНТА (Шанс 80%)
                s_id = scorer.get('db_id') or scorer.get('id')
                assister = get_weighted_assister(data["my_players"], s_id) if random.random() < 0.80 else None
                if assister:
                    log_entry += f"\n🅰️ пас: {assister['name']}"
                    data['match_assists'] = data.get('match_assists', 0) + 1

            data["score_me"] += 1
            data["match_log"].append(log_entry)
            
            # Обновляем БД (голы и ассисты игрокам) сразу
            conn = get_db(); c = conn.cursor()
            c.execute('UPDATE squad SET goals = goals + 1 WHERE id = ?', (scorer.get('db_id') or scorer.get('id'),))
            if assister:
                c.execute('UPDATE squad SET assists = assists + 1 WHERE id = ?', (assister.get('db_id') or assister.get('id'),))
            conn.commit(); conn.close()

        # --- 3. ГОЛ БОТА (В) ---
        bot_roll = random.random()
        comeback_mod = 0.02 if data["score_me"] > data["score_opp"] else 0.0
        
        if bot_roll < (current_goal_chance * mod_miss * (opp_ovr / my_ovr) + comeback_mod):
            is_opp_pen = random.random() < 0.15
            opp_shooters = [p for p in data["opp_players"] if p.get('pos') in ['FWD', 'MID']]
            if not opp_shooters: opp_shooters = data["opp_players"]

            if is_opp_pen:
                opp_scorer = sorted(opp_shooters, key=lambda x: x.get('rating', 0), reverse=True)[0]
                log_msg = f"🥅 {display_min}' <b>ПЕНАЛЬТИ!</b> {opp_scorer['name']} точен. — {data['opp_name']}"
            else:
                opp_scorer = random.choice(opp_shooters)
                log_msg = f"🥅 {display_min}' Гол! {opp_scorer['name']} — {data['opp_name']}"
            
            data["score_opp"] += 1
            data["match_log"].append(log_msg)

        # --- ВИЗУАЛИЗАЦИЯ (ОБНОВЛЕНИЕ СООБЩЕНИЯ) ---
        if data.get("is_paused"): return 

        log_v = "\n".join(data["match_log"][-3:]) # Показываем последние 3 события
        text = (f"🏟 <b>{data['opp_name']}</b>\n"
                f"⏱ {minute_step}' | Счет: <b>{data['score_me']}:{data['score_opp']}</b>\n"
                f"Тактика: {data['tactic']}\n"
                f"————————————————\n{log_v if log_v else 'Идет плотная борьба...'}")
        
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⚙️ Руководство", callback_data="manage_team")]
        ])
        
        try:
            await msg.edit_text(text, reply_markup=kb, parse_mode="HTML")
        except: 
            pass # Игнорируем ошибки, если текст не изменился

        await asyncio.sleep(2.0) # Скорость симуляции 

    # --- ПОСЛЕ ЦИКЛА (Перерыв или Конец) ---
    if data.get("is_paused"):
        return

    if minute_step >= 45 and minute_step < 50: 
        data["minute"] = 45 
        kb_half = types.InlineKeyboardMarkup(inline_keyboard=[
    [types.InlineKeyboardButton(text="⚙️ Руководство", callback_data="manage_team")],
    [types.InlineKeyboardButton(text="▶️ 2-й тайм", callback_data="continue_match")]
])
        await msg.answer("⏸ <b>Перерыв!</b> Смените тактику или сделайте замены.", reply_markup=kb_half, parse_mode="HTML")
    elif minute_step >= 90:
        await finish_match(msg, uid)

# 1. Сначала хендлер, чтобы кнопка вообще ожила
@dp.callback_query(F.data == "manage_team")
async def manage_team_callback(cb: types.CallbackQuery):
    uid = cb.from_user.id
    if uid not in matches_data:
        return await cb.answer("❌ Данные матча устарели.", show_alert=True)
    await manage_team(cb, uid)

# 2. Сама исправленная функция
async def manage_team(event, uid=None):
    """Центральное меню тактики и замен."""
    if uid is None:
        uid = event.from_user.id
    
    if uid not in matches_data:
        return 

    data = matches_data[uid]
    data["is_paused"] = True 

    conn = get_db(); c = conn.cursor()
    c.execute('SELECT club FROM users WHERE user_id = ?', (uid,))
    u_row = c.fetchone()
    club_name = u_row[0] if u_row else "Мой Клуб"
    conn.close()
    
    b = InlineKeyboardBuilder()
    
    # ТВОИ НОВЫЕ ТАКТИКИ ВМЕСТО ЗАЩ/АТК
    b.row(
        types.InlineKeyboardButton(text="🚌 Автобус", callback_data="m_tactic_Автобус"),
        types.InlineKeyboardButton(text="⚡️ Прессинг", callback_data="m_tactic_Прессинг")
    )
    b.row(
        types.InlineKeyboardButton(text="🪄 Тики-така", callback_data="m_tactic_Тики-така"),
        types.InlineKeyboardButton(text="🏹 Контра", callback_data="m_tactic_Контратака"),
        types.InlineKeyboardButton(text="🏃‍♂️ Бей-беги", callback_data="m_tactic_Бей-беги")
    )
    b.row(types.InlineKeyboardButton(text="🔄 Сделать замены", callback_data="sub_list"))
    b.row(types.InlineKeyboardButton(text="▶️ Продолжить матч", callback_data="continue_match"))

    # Заменяем дефолт на 'Тики-така' или любую другую
    current_tactic = data.get('tactic', 'Тики-така')

    text = (
        f"⚙️ <b>Управление: {club_name}</b>\n"
        f"⚽️ Счет: <b>{data['score_me']}:{data['score_opp']}</b> | ⏱ {data['minute']}'\n"
        f"Установка: <b>{current_tactic}</b>\n\n"
        f"<i>Выберите стиль игры для изменения хода матча:</i>"
    )

    try:
        if isinstance(event, types.CallbackQuery):
            await event.message.edit_text(text, reply_markup=b.as_markup(), parse_mode="HTML")
        else:
            await event.answer(text, reply_markup=b.as_markup(), parse_mode="HTML")
    except Exception as e:
        print(f"Ошибка в manage_team: {e}")

# Обработчик смены тактики
@dp.callback_query(F.data.startswith("m_tactic_"))
async def change_match_tactic(cb: types.CallbackQuery):
    new_t = cb.data.replace("m_tactic_", "")
    uid = cb.from_user.id
    if uid in matches_data:
        matches_data[uid]["tactic"] = new_t
        await cb.answer(f"Установка: {new_t}")
        await manage_team(cb) # Перерисовываем меню


@dp.callback_query(F.data.startswith("set_"))
async def set_player_in_match(cb: types.CallbackQuery):
    # Разбираем: ID игрока из базы и индекс слота в МАТЧЕ (0-10)
    _, pid, slot_idx = cb.data.split("_")
    uid, slot_idx = cb.from_user.id, int(slot_idx)
    
    # 1. Берем данные игрока из БД, чтобы просто знать его статы
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT player_name, rating, pos, stamina FROM squad WHERE id = ?', (pid,))
    new_p = c.fetchone(); conn.close()

    if uid in matches_data and new_p:
        data = matches_data[uid]
        
        # 2. Запоминаем, кто уходит с поля
        old_p = data["my_players"][slot_idx]
        if "substituted_out" not in data: 
            data["substituted_out"] = []
        data["substituted_out"].append(old_p['name'])
        
        # 3. ВАЖНЫЙ МОМЕНТ: 
        # Мы просто ПЕРЕЗАПИСЫВАЕМ ячейку в словаре матча.
        # В базе данных (таблица squad) у этого игрока slot_id так и останется NULL!
        data["my_players"][slot_idx] = {
            "name": new_p[0], 
            "rating": new_p[1], 
            "pos": new_p[2], 
            "stamina": new_p[3], 
            "yc": 0
        }
        
        # Логируем замену для красоты
        data["match_log"].append(f"🔄 {data['minute']}' {new_p[0]} ⬆️ {old_p['name']} ⬇️")
        
        # Если была травма — снимаем флаг блокировки
        data["needs_sub"] = False 
        
        await cb.answer(f"✅ {new_p[0]} вошел в игру!")
        
        # Возвращаемся в меню управления (где кнопка «Продолжить»)
        await manage_team(cb)
        
from contextlib import closing # Добавь этот импорт в начало файла

@dp.message(F.text == "📝 Записаться в Лигу")
async def process_league_join(message: types.Message):
    uid = message.from_user.id
    
    with closing(get_db()) as conn:
        with conn: 
            c = conn.cursor()
            
            # Проверяем клуб
            c.execute('SELECT club FROM users WHERE user_id = ?', (uid,))
            user_data = c.fetchone()
            if not user_data or not user_data[0]:
                return await message.answer("❌ Сначала создай клуб!")
            
            # Проверяем состав (минимум 11 здоровых)
            c.execute('SELECT COUNT(*) FROM squad WHERE user_id = ? AND is_banned = 0 AND injury_remaining = 0', (uid,))
            total_players = c.fetchone()[0]
            if total_players < 11:
                return await message.answer(f"❌ Нужно 11 здоровых игроков! У вас: {total_players}")
            
            try:
                # Записываем строго в обе таблицы
                c.execute('INSERT INTO league_participants (user_id) VALUES (?)', (uid,))
                c.execute('INSERT OR IGNORE INTO cup_participants (user_id) VALUES (?)', (uid,))
                
                await message.answer(
                    f"🏟 <b>Заявка принята!</b>\nКлуб: <b>{user_data[0]}</b>\n"
                    f"✅ Ты в списках Лиги и Кубка.", 
                    parse_mode="HTML"
                )
            except sqlite3.IntegrityError:
                await message.answer("⚠️ Вы уже подали заявку.")

@dp.callback_query(F.data == "back_to_field")
async def back_to_field(cb: types.CallbackQuery):
    uid = cb.from_user.id
    
    # Проверяем, идет ли сейчас матч у пользователя
    if uid in matches_data:
        # Вместо edit_squad_message вызываем функцию управления матчем
        await update_match_message(cb.message, uid)
    else:
        # Если матча нет (на всякий случай), возвращаем к обычному составу
        await edit_squad_message(cb.message, uid, cb.message.chat.id)
    
    await cb.answer()


@dp.callback_query(F.data == "sub_list")
async def show_sub_menu(cb: types.CallbackQuery):
    uid = cb.from_user.id
    if uid not in matches_data: return
    data = matches_data[uid]
    
    b = InlineKeyboardBuilder()
    for i, p in enumerate(data["my_players"]):
        # Меняем selectpos_ на msub_
        b.button(text=f"{p['pos']} | {p['name']} (🔋{p.get('stamina', 0)})", 
                 callback_data=f"msub_{p['pos']}_{i}")
    b.adjust(2)
    b.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data="manage_team"))
    await cb.message.edit_text("<b>Кого заменить?</b>", reply_markup=b.as_markup(), parse_mode="HTML")

@dp.callback_query(F.data.startswith("selectpos_"))
async def list_players(cb: types.CallbackQuery):
    # Разбираем колбэк
    parts = cb.data.split("_")
    pos_needed = parts[1] # GK, DEF, MID или FWD
    slot_idx = parts[2]
    uid = cb.from_user.id
    
    conn = get_db()
    c = conn.cursor()
    
    # Поиск свободных игроков по позиции
    search_pattern = f"%{pos_needed.upper()}%"
    
    c.execute('''SELECT id, player_name, rating, pos, stamina 
                 FROM squad 
                 WHERE user_id = ? 
                 AND UPPER(pos) LIKE ? 
                 AND slot_id IS NULL 
                 AND injury_remaining = 0 
                 AND is_banned = 0
                 AND (training_until IS NULL OR training_until = '')
                 ORDER BY rating DESC''', (uid, search_pattern))
    
    all_subs = c.fetchall()
    conn.close()
    
    if not all_subs:
        return await cb.answer(
            f"❌ Нет свободных игроков для {pos_needed}.\nВсе заняты или на тренировке.", 
            show_alert=True
        )

    # Используем InlineKeyboardBuilder с четким указанием типа кнопок
    b = InlineKeyboardBuilder()
    
    for pid, name, rat, p_pos, stam in all_subs:
        # Каждого игрока добавляем отдельной строкой (row)
        b.row(types.InlineKeyboardButton(
            text=f"[{p_pos}] {name} ({rat}) 🔋{stam}%", 
            callback_data=f"setslot_{pid}_{slot_idx}"
        ))
    
    # Кнопка возврата в меню состава
    b.row(types.InlineKeyboardButton(
        text="⬅️ К составу", 
        callback_data="open_squad" # Проверь, чтобы этот callback_data совпадал с твоим хендлером состава
    ))

    # Текст сообщения
    header_text = (
        f"📥 <b>ВЫБОР ИГРОКА: {pos_needed}</b>\n"
        f"————————————————————\n"
        f"Выбери бойца для перевода в стартовый состав:"
    )

    try:
        await cb.message.edit_text(
            header_text, 
            reply_markup=b.as_markup(), 
            parse_mode="HTML"
        )
    except Exception as e:
        print(f"Ошибка при выводе списка игроков: {e}")
        await cb.answer("⚠️ Произошла ошибка при отрисовке списка.")

@dp.callback_query(F.data.startswith("msub_"))
async def list_match_subs(cb: types.CallbackQuery):
    parts = cb.data.split("_")
    pos_needed = parts[1] # Например, 'MID'
    slot_idx = parts[2]
    
    uid = cb.from_user.id
    if uid not in matches_data: 
        return await cb.answer("❌ Ошибка: Данные матча не найдены.")
    
    data = matches_data[uid]
    current_names = [p['name'] for p in data["my_players"]]
    gone_names = data.get("substituted_out", []) 
    
    conn = get_db(); c = conn.cursor()
    
    search_query = f"%{pos_needed}%"
    
    c.execute('''SELECT id, player_name, rating, stamina, pos 
                 FROM squad 
                 WHERE user_id = ? 
                 AND pos LIKE ? 
                 AND injury_remaining = 0 
                 AND is_banned = 0''', (uid, search_query))
    
    all_subs = c.fetchall()
    conn.close()
    
    b = InlineKeyboardBuilder()
    count = 0
    
    for pid, name, rat, stam, p_pos in all_subs:
        if name not in current_names and name not in gone_names:
            b.button(text=f"[{p_pos}] {name} ({rat}) 🔋{stam}", 
                     callback_data=f"set_{pid}_{slot_idx}")
            count += 1
    
    if count == 0:
        return await cb.answer(f"❌ Нет свободных игроков на позицию {pos_needed}!", show_alert=True)
    
    b.adjust(1)
    b.row(types.InlineKeyboardButton(text="⬅️ Назад к списку", callback_data="sub_list"))
    
    await cb.message.edit_text(
        f"📥 <b>Замена на позицию {pos_needed}</b>\nКто выйдет на поле?", 
        reply_markup=b.as_markup(), 
        parse_mode="HTML"
    )

@dp.message(F.text == "📦 Вне состава")
async def show_hospital_msg(message: types.Message):
    user_id = message.from_user.id
    conn = get_db(); c = conn.cursor()
    
    try:
        c.execute('''SELECT player_name, pos, injury_remaining, is_banned, training_until 
                     FROM squad 
                     WHERE user_id = ? AND (injury_remaining > 0 OR is_banned > 0 OR training_until IS NOT NULL)''', (user_id,))
        players = c.fetchall()
        
        res = "🏥 <b>МЕДИЦИНСКИЙ ЦЕНТР И ОТСТРАНЕНИЯ</b>\n————————————————————\n\n"
        now = datetime.now()
        training, injured, banned = [], [], []

        for p in players:
            name, pos, inj, ban, t_until = p
            if t_until:
                try:
                    end = datetime.strptime(t_until, "%Y-%m-%d %H:%M:%S")
                    if end > now:
                        rem = end - now
                        training.append(f"🏋️‍♂️ {name} ({pos}) — {int(rem.total_seconds()//3600)}ч. {int((rem.total_seconds()//60)%60)}м.")
                except: pass

            if inj and inj > 0:
                injured.append(f"🚑 {name} ({pos}) — еще {inj} тур(а)")
            
            if ban and ban > 0:
                banned.append(f"🟥 {name} ({pos}) — еще {ban} тур(а)")

        res += "<b>🏋️‍♂️ Индивидуальный план:</b>\n" + ("\n".join(training) if training else "<i>— Все в общей группе</i>") + "\n\n"
        res += "<b>🚑 Лазарет:</b>\n" + ("\n".join(injured) if injured else "<i>— Пусто</i>") + "\n\n"
        res += "<b>🟥 Дисквалификации:</b>\n" + ("\n".join(banned) if banned else "<i>— Нарушений нет</i>")
        
        await message.answer(res, parse_mode="HTML")
    finally:
        conn.close()

@dp.callback_query(F.data == "continue_match")
async def continue_match_handler(cb: types.CallbackQuery):
    uid = cb.from_user.id
    if uid not in matches_data: 
        return await cb.answer("❌ Матч завершен или данные утеряны.")
    
    data = matches_data[uid]
    data["is_paused"] = False # ОБЯЗАТЕЛЬНО снимаем паузу
    
    await cb.answer("⏳ Матч продолжается...")
    # Запускаем симуляцию с той минуты, на которой остановились
    await run_match_simulation(cb.message, uid)

async def finish_match(msg, uid):
    # Проверяем, есть ли данные матча
    if uid not in matches_data:
        return
        
    data = matches_data[uid]
    score_me, score_opp = data["score_me"], data["score_opp"]
    
    conn = get_db()
    c = conn.cursor()
    res = ""
    reward = 0

    # Определение результата и награды
    if score_me > score_opp:
        reward = 2
        res = f"🎉 Победа! Вы заработали призовые: +{reward} млн €"
    elif score_me == score_opp:
        reward = 1
        res = f"🤝 Ничья. Призовые: +{reward} млн €"
    else:
        reward = 0
        res = "❌ Поражение. В этот раз без призовых."

    # --- ЛОГИКА УСТАЛОСТИ И ТРАВМ (ТВОЕ НЕ УДАЛЯТЬ) ---
    
    # 1. Считаем, сколько уже травмированных в клубе (макс 4)
    c.execute('SELECT COUNT(*) FROM squad WHERE user_id = ? AND injury_remaining > 0', (uid,))
    current_injured_count = c.fetchone()[0]
    
    injury_log = ""
    
    # Цикл по всем игрокам, которые были в матче
    for player in data["my_players"]:
        # Используем tired. т.к. функция в другом файле
        added_fatigue = tired.calculate_match_fatigue(player['pos'], is_league=False)
        
        # Обновляем стамину СТРОГО по db_id
        p_id = player.get('db_id')
        
        if p_id:
            c.execute('UPDATE squad SET stamina = MIN(50, stamina + ?) WHERE id = ?', (added_fatigue, p_id))
        else:
            c.execute('UPDATE squad SET stamina = MIN(50, stamina + ?) WHERE user_id = ? AND player_name = ?', 
                      (added_fatigue, uid, player['name']))
            
        # Сразу получаем актуальную стамину после обновления для проверки травмы
        if p_id:
            c.execute('SELECT stamina FROM squad WHERE id = ?', (p_id,))
        else:
            c.execute('SELECT stamina FROM squad WHERE user_id = ? AND player_name = ?', (uid, player['name']))
        
        row = c.fetchone()
        current_stamina = row[0] if row else 0
        
        # Проверяем шанс травмы
        if current_injured_count < 4:
            if injured.can_get_injured(current_injured_count) and injured.check_injury_chance(current_stamina):
                inj_name, duration = injured.get_random_injury()
                
                # Игрок выбывает
                if p_id:
                    c.execute('''UPDATE squad 
                                 SET injury_type = ?, injury_remaining = ?, status = "bench", slot_id = NULL 
                                 WHERE id = ?''', (inj_name, duration, p_id))
                else:
                    c.execute('''UPDATE squad 
                                 SET injury_type = ?, injury_remaining = ?, status = "bench", slot_id = NULL 
                                 WHERE user_id = ? AND player_name = ?''', (inj_name, duration, uid, player['name']))
                
                injury_log += f"\n🚑 <b>Травма:</b> {player['name']} ({inj_name} на {duration} матчей)"
                current_injured_count += 1 

# --- ОБНОВЛЕНИЕ ДАННЫХ ПОЛЬЗОВАТЕЛЯ (ОБЩАЯ СТАТИСТИКА) --- 
    try:
        # Исправлено: удален лишний .datetime, так как класс уже импортирован напрямую
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Обновляем баланс, голы и дату
        c.execute('''UPDATE users 
                     SET balance = balance + ?, 
                         goals_scored = goals_scored + ?, 
                         last_match = ? 
                     WHERE user_id = ?''', 
                  (reward, score_me, current_time, uid))
        
        # Добавляем +1 в общую колонку побед/ничей/поражений
        if score_me > score_opp:
            c.execute('UPDATE users SET wins = wins + 1 WHERE user_id = ?', (uid,))
        elif score_me == score_opp:
            c.execute('UPDATE users SET draws = draws + 1 WHERE user_id = ?', (uid,))
        else:
            c.execute('UPDATE users SET losses = losses + 1 WHERE user_id = ?', (uid,))
        
        # Снимаем бан за КК (если был временный)
        c.execute('UPDATE squad SET is_banned = 0 WHERE user_id = ? AND is_banned = 1', (uid,))
        
        conn.commit()
    except Exception as e:
        print(f"❌ Ошибка при обновлении статистики: {e}")
    finally:
        # ГАРАНТИРОВАННОЕ ЗАКРЫТИЕ (лечит "database is locked")
        conn.close()
    
    # Формируем сообщение
    final_text = (
        f"🏁 <b>Товарищеский матч окончен! {score_me}:{score_opp}</b>\n"
        f"{res}"
    )
    if injury_log:
        final_text += f"\n{injury_log}"
    
    final_text += f"\n\n<i>🔋 Игроки накопили усталость. Травмы не лечатся в матчах против ботов.</i>"

    await msg.answer(final_text, parse_mode="HTML")
    
    # Удаляем данные матча
    if uid in matches_data:
        del matches_data[uid]

@dp.callback_query(F.data == "cancel_match")
async def cancel_match(cb: types.CallbackQuery):
    await cb.message.edit_text("❌ Матч отменен.")


@dp.message(F.text == "📊 Статистика")
async def stats_choice(m: types.Message):
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🏠 Статистика клуба (Общая)", callback_data="stats_club")],
        [types.InlineKeyboardButton(text="🇪🇺 Статистика ЛЧ", callback_data="stats_ucl_menu")],
        [types.InlineKeyboardButton(text="🏆 Статистика Лиги", callback_data="stats_league_menu")]
    ])
    await m.answer("Выберите тип статистики:", reply_markup=kb)

@dp.callback_query(F.data == "st_cards")
async def show_top_cards(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    # Считаем сумму ЖК и КК, чтобы найти самых жестких
    c.execute('''SELECT player_name, yellow_cards, red_cards 
                 FROM squad WHERE user_id = ? 
                 AND (yellow_cards > 0 OR red_cards > 0)
                 ORDER BY (yellow_cards + red_cards * 3) DESC LIMIT 10''', (cb.from_user.id,))
    players = c.fetchall(); conn.close()
    
    if not players:
        return await cb.answer("В вашем клубе пока все играют чисто!", show_alert=True)
    
    text = "🟨🟥 <b>Топ грубиянов клуба:</b>\n\n"
    for i, (name, yc, rc) in enumerate(players, 1):
        text += f"{i}. {name} — 🟨{yc} | 🟥{rc}\n"
        
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_club")]
    ])
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

# --- ОБЩАЯ СТАТИСТИКА КЛУБА (Твой старый код) ---
@dp.callback_query(F.data == "stats_club")
async def show_stats_club(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    # Складываем обычные показатели и лиговые
    c.execute('''SELECT 
                 (wins + league_wins), 
                 (draws + league_draws), 
                 (losses + league_losses), 
                 (goals_scored + league_goals), 
                 club 
                 FROM users WHERE user_id = ?''', (cb.from_user.id,))
    row = c.fetchone(); conn.close()
    
    if not row: return await cb.answer("Клуб не найден")
    
    w, d, l, total_g, club = row
    
    text = (f"📈 <b>Общая статистика клуба ({club}):</b>\n"
            f"<i>(Лига + Товарищеские матчи)</i>\n\n"
            f"✅ Победы: {w} | 🤝 Ничьи: {d} | ❌ Поражения: {l}\n"
            f"————————————————\n"
            f"⚽️ Всего забито: <b>{total_g}</b>\n")
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⚽️ Топ бомбардиров", callback_data="st_goals")],
        [types.InlineKeyboardButton(text="🅰️ Топ ассистентов", callback_data="st_assists")],
        [types.InlineKeyboardButton(text="🟨 Топ грубиянов (ЖК/КК)", callback_data="st_cards")],
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_back")]
    ])
    
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "stats_ucl_menu")
async def ucl_stats_menu(cb: types.CallbackQuery):
    text = "🇪🇺 <b>Лига Чемпионов: Индивидуальные достижения</b>\n<i>Статистика элитного турнира.</i>"
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⚽️ Бомбардиры ЛЧ", callback_data="ustats_goals")],
        [types.InlineKeyboardButton(text="🅰️ Ассистенты ЛЧ", callback_data="ustats_assists")],
        [types.InlineKeyboardButton(text="🟨 Желтые карточки ЛЧ", callback_data="ustats_yellow")],
        [types.InlineKeyboardButton(text="🟥 Красные карточки ЛЧ", callback_data="ustats_red")],
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_back")]
    ])
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")



@dp.callback_query(F.data == "stats_league_menu")
async def league_stats_menu(cb: types.CallbackQuery):
    text = "🏆 <b>Индивидуальные достижения Лиги</b>\n<i>Здесь только голы, забитые в матчах против реальных игроков.</i>"
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⚽️ Бомбардиры Лиги", callback_data="lstats_goals")],
        [types.InlineKeyboardButton(text="🅰️ Ассистенты Лиги", callback_data="lstats_assists")],
        [types.InlineKeyboardButton(text="🟨 Желтые карточки", callback_data="lstats_yellow")],
        [types.InlineKeyboardButton(text="🟥 Красные карточки", callback_data="lstats_red")],
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_back")]
    ])
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "lstats_goals")
async def show_league_top_goals(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute('''
        SELECT s.player_name, u.club, ls.goals 
        FROM league_stats ls
        JOIN squad s ON ls.player_id = s.id
        JOIN users u ON ls.user_id = u.user_id
        WHERE ls.goals > 0
        ORDER BY ls.goals DESC LIMIT 10
    ''')
    rows = c.fetchall(); conn.close()
    
    res = "⚽️ <b>ТОП-10 БОМБАРДИРОВ ЛИГИ:</b>\n\n"
    for i, (name, club, goals) in enumerate(rows, 1):
        res += f"{i}. {name} ({club}) — <b>{goals}</b>\n"
    
    if not rows: res += "Пока голов не забито."
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_league_menu")]])
    await cb.message.edit_text(res, reply_markup=kb, parse_mode="HTML")

# АССИСТЕНТЫ
@dp.callback_query(F.data == "lstats_assists")
async def show_league_top_assists(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    
    # ИСПРАВЛЕНО: Заменили u.id на u.user_id в блоке JOIN
    c.execute('''
        SELECT s.player_name, u.club, ls.assists 
        FROM league_stats ls
        JOIN squad s ON ls.player_id = s.id
        JOIN users u ON ls.user_id = u.user_id
        WHERE ls.assists > 0
        ORDER BY ls.assists DESC LIMIT 10
    ''')
    rows = c.fetchall(); conn.close()
    
    res = "🅰️ <b>ТОП-10 АССИСТЕНТОВ ЛИГИ:</b>\n"
    res += "<i>Мастера последнего паса и командной игры.</i>\n\n"
    
    if not rows:
        res += "Пока голевых передач не зафиксировано."
    else:
        for i, (name, club, assists) in enumerate(rows, 1):
            res += f"{i}. {name} ({club}) — <b>{assists}</b>\n"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_league_menu")]
    ])
    
    await cb.message.edit_text(res, reply_markup=kb, parse_mode="HTML")
    await cb.answer()

# КАРТОЧКИ (Желтые)
@dp.callback_query(F.data == "lstats_yellow")
async def show_league_top_yellow(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute('''
        SELECT s.player_name, u.club, ls.yellow_cards 
        FROM league_stats ls
        JOIN squad s ON ls.player_id = s.id
        JOIN users u ON ls.user_id = u.user_id
        WHERE ls.yellow_cards > 0
        ORDER BY ls.yellow_cards DESC LIMIT 10
    ''')
    rows = c.fetchall(); conn.close()
    
    res = "🟨 <b>ГРУБИЯНЫ ЛИГИ (ЖК):</b>\n\n"
    for i, (name, club, cards) in enumerate(rows, 1):
        res += f"{i}. {name} ({club}) — <b>{cards}</b>\n"
    
    if not rows: res += "Пока без карточек."
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_league_menu")]])
    await cb.message.edit_text(res, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "lstats_red")
async def show_league_top_red(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    
    # Выбираем игрока, его клуб и количество красных карточек из league_stats
    c.execute('''
        SELECT s.player_name, u.club, ls.red_cards 
        FROM league_stats ls
        JOIN squad s ON ls.player_id = s.id
        JOIN users u ON ls.user_id = u.user_id
        WHERE ls.red_cards > 0
        ORDER BY ls.red_cards DESC LIMIT 10
    ''')
    rows = c.fetchall(); conn.close()
    
    res = "🟥 <b>ГЛАВНЫЕ НАРУШИТЕЛИ ЛИГИ (КК):</b>\n"
    res += "<i>Эти игроки чаще всего подводили свои команды.</i>\n\n"
    
    if not rows:
        res += "Пока в лиге обошлось без удалений. Все играют чисто! 🤝"
    else:
        for i, (name, club, reds) in enumerate(rows, 1):
            # Добавим изюминку: если у игрока много красных, пометим его особо
            warning = "⚠️" if reds > 1 else ""
            res += f"{i}. {name} ({club}) — <b>{reds}</b> {warning}\n"
    
    # Кнопка возврата в меню статистики лиги
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_league_menu")]
    ])
    
    await cb.message.edit_text(res, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "ustats_goals")
async def show_ucl_top_goals(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute('''
        SELECT s.player_name, u.club, us.goals 
        FROM ucl_stats us
        JOIN squad s ON us.player_id = s.id
        JOIN users u ON us.user_id = u.user_id
        WHERE us.goals > 0
        ORDER BY us.goals DESC LIMIT 10
    ''')
    rows = c.fetchall(); conn.close()
    
    res = "🏆 <b>БОМБАРДИРЫ ЛЧ:</b>\n\n"
    if not rows:
        res += "В этом сезоне ЛЧ голов еще не забивали."
    else:
        for i, (name, club, goals) in enumerate(rows, 1):
            res += f"{i}. {name} ({club}) — <b>{goals}</b> ⚽️\n"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_ucl_menu")]])
    await cb.message.edit_text(res, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "ustats_assists")
async def show_ucl_top_assists(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute('''
        SELECT s.player_name, u.club, us.assists 
        FROM ucl_stats us
        JOIN squad s ON us.player_id = s.id
        JOIN users u ON us.user_id = u.user_id
        WHERE us.assists > 0
        ORDER BY us.assists DESC LIMIT 10
    ''')
    rows = c.fetchall(); conn.close()
    
    res = "🅰️ <b>ЛУЧШИЕ АССИСТЕНТЫ ЛЧ:</b>\n\n"
    if not rows:
        res += "Голевых передач пока не зафиксировано."
    else:
        for i, (name, club, assists) in enumerate(rows, 1):
            res += f"{i}. {name} ({club}) — <b>{assists}</b> 🅰️\n"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_ucl_menu")]])
    await cb.message.edit_text(res, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "ustats_yellow")
async def show_ucl_top_yellow(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute('''
        SELECT s.player_name, u.club, us.yellow_cards 
        FROM ucl_stats us
        JOIN squad s ON us.player_id = s.id
        JOIN users u ON us.user_id = u.user_id
        WHERE us.yellow_cards > 0
        ORDER BY us.yellow_cards DESC LIMIT 10
    ''')
    rows = c.fetchall(); conn.close()
    
    res = "🟨 <b>ЖЕЛТЫЕ КАРТОЧКИ ЛЧ:</b>\n\n"
    if not rows:
        res += "На полях ЛЧ пока полная идиллия без ЖК."
    else:
        for i, (name, club, cards) in enumerate(rows, 1):
            res += f"{i}. {name} ({club}) — <b>{cards}</b> 🟨\n"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_ucl_menu")]])
    await cb.message.edit_text(res, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "ustats_red")
async def show_ucl_top_red(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute('''
        SELECT s.player_name, u.club, us.red_cards 
        FROM ucl_stats us
        JOIN squad s ON us.player_id = s.id
        JOIN users u ON us.user_id = u.user_id
        WHERE us.red_cards > 0
        ORDER BY us.red_cards DESC LIMIT 10
    ''')
    rows = c.fetchall(); conn.close()
    
    res = "🟥 <b>УДАЛЕНИЯ ЛЧ (КРАСНЫЕ):</b>\n\n"
    if not rows:
        res += "В этом турнире пока обходится без грубых нарушений!"
    else:
        for i, (name, club, reds) in enumerate(rows, 1):
            res += f"{i}. {name} ({club}) — <b>{reds}</b> 🟥\n"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[[types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_ucl_menu")]])
    await cb.message.edit_text(res, reply_markup=kb, parse_mode="HTML")

@dp.callback_query(F.data == "stats_back")
async def process_stats_back(cb: types.CallbackQuery):
    # Создаем ту же клавиатуру, что была в самом начале
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🏠 Статистика клуба (Общая)", callback_data="stats_club")],
        [types.InlineKeyboardButton(text="🏆 Статистика Лиги", callback_data="stats_league_menu")]
    ])
    
    # Редактируем старое сообщение, возвращая выбор
    await cb.message.edit_text("Выберите тип статистики:", reply_markup=kb)
    await cb.answer()

# Исправленный обработчик детальной статистики клуба (бомбардиры, ассисты и т.д.)
@dp.callback_query(F.data.startswith("st_"))
async def player_stats_callback(cb: types.CallbackQuery):
    action = cb.data.split("_")[1]
    uid = cb.from_user.id
    conn = get_db(); c = conn.cursor()
    
    if action == "goals":
        c.execute('SELECT player_name, goals FROM squad WHERE user_id = ? AND goals > 0 ORDER BY goals DESC LIMIT 10', (uid,))
        title = "⚽️ <b>Топ бомбардиров:</b>"
        icon = "гол(ов)"
    elif action == "assists":
        c.execute('SELECT player_name, assists FROM squad WHERE user_id = ? AND assists > 0 ORDER BY assists DESC LIMIT 10', (uid,))
        title = "🅰️ <b>Топ ассистентов:</b>"
        icon = "пас(ов)"
    elif action == "cards": # Твой старый st_cards переехал сюда для унификации
        c.execute('SELECT player_name, yellow_cards, red_cards FROM squad WHERE user_id = ? AND (yellow_cards > 0 OR red_cards > 0) ORDER BY (yellow_cards + red_cards * 3) DESC LIMIT 10', (uid,))
        title = "🟨🟥 <b>Грубияны клуба:</b>"
    else:
        conn.close()
        return await cb.answer("Неизвестный тип статистики")

    data = c.fetchall(); conn.close()
    
    if not data: 
        return await cb.answer("Статистики пока нет!", show_alert=True)
    
    res_text = f"{title}\n\n"
    for i, row in enumerate(data, 1):
        if action == "cards":
            res_text += f"{i}. {row[0]} — 🟨{row[1]} | 🟥{row[2]}\n"
        else:
            res_text += f"{i}. {row[0]} — {row[1]} {icon}\n"
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="stats_club")]
    ])
    
    await cb.message.edit_text(res_text, reply_markup=kb, parse_mode="HTML")
    await cb.answer()

@dp.message(F.text == "🏆 Таблица")
async def show_leaderboard(m: types.Message):
    conn = get_db(); c = conn.cursor()
    
    # 1. Получаем сезон и группу игрока
    c.execute('SELECT value FROM settings WHERE key = "current_season"')
    season_label = (c.fetchone() or ["25/26"])[0]

    c.execute('SELECT league_group FROM users WHERE user_id = ?', (m.from_user.id,))
    res_g = c.fetchone()
    user_group = res_g[0] if res_g else "A"

    c.execute('SELECT COUNT(*) FROM league_schedule')
    if c.fetchone()[0] == 0:
        conn.close()
        return await m.answer(f"🏆 <b>ТАБЛИЦА | СЕЗОН {season_label}</b>\n————————————————————\n⏳ Сезон еще не начат!", parse_mode="HTML")

    # 2. Грузим данные только для этой группы
    c.execute('''
        SELECT club, league_wins, league_draws, league_losses, league_goals,
               (league_wins + league_draws + league_losses) as played,
               (league_wins * 3 + league_draws) as pts 
        FROM users 
        WHERE club IS NOT NULL AND league_group = ?
        ORDER BY pts DESC, league_goals DESC, club ASC
    ''', (user_group,))
    
    rows = c.fetchall()
    conn.close()

    text = f"🏆 <b>ТАБЛИЦА | СЕТКА {user_group} | СЕЗОН {season_label}</b>\n"
    text += "<code> №  Клуб         И  В-Н-П  Г   О</code>\n"
    text += "<code>——————————————————————————————</code>\n"

    for i, (club, w, d, l, gs, pld, pts) in enumerate(rows, 1):
        club_name = (club[:10] + '..') if len(club) > 10 else club.ljust(12)
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i:<2}"
        text += f"<code>{medal} {club_name} {pld:<2} {w}-{d}-{l}  {gs:<3} {pts}</code>\n"

    text += "<code>——————————————————————————————</code>\n"
    text += f"<i>Вы находитесь в Сетке {user_group}</i>"
    
    # Добавим кнопку для переключения сеток
    kb = InlineKeyboardBuilder()
    other_group = "B" if user_group == "A" else "A"
    kb.button(text=f"Сетка {other_group}", callback_data=f"show_table_{other_group}")
    
    await m.answer(text, parse_mode="HTML", reply_markup=kb.as_markup())

@dp.callback_query(F.data.startswith("show_table_"))
async def toggle_table_group(cb: types.CallbackQuery):
    # Извлекаем группу из callback_data (например, "A" или "B")
    target_group = cb.data.replace("show_table_", "")
    
    conn = get_db(); c = conn.cursor()
    
    # 1. Получаем сезон
    c.execute('SELECT value FROM settings WHERE key = "current_season"')
    season_label = (c.fetchone() or ["25/26"])[0]

    # 2. Грузим данные для выбранной группы
    c.execute('''
        SELECT club, league_wins, league_draws, league_losses, league_goals,
               (league_wins + league_draws + league_losses) as played,
               (league_wins * 3 + league_draws) as pts 
        FROM users 
        WHERE club IS NOT NULL AND league_group = ?
        ORDER BY pts DESC, league_goals DESC, club ASC
    ''', (target_group,))
    
    rows = c.fetchall()
    conn.close()

    if not rows:
        return await cb.answer(f"Сетка {target_group} пока пуста!", show_alert=True)

    # 3. Формируем текст (такой же, как в основной команде)
    text = f"🏆 <b>ТАБЛИЦА | СЕТКА {target_group} | СЕЗОН {season_label}</b>\n"
    text += "<code> №  Клуб         И  В-Н-П  Г   О</code>\n"
    text += "<code>——————————————————————————————</code>\n"

    for i, (club, w, d, l, gs, pld, pts) in enumerate(rows, 1):
        club_name = (club[:10] + '..') if len(club) > 10 else club.ljust(12)
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i:<2}"
        text += f"<code>{medal} {club_name} {pld:<2} {w}-{d}-{l}  {gs:<3} {pts}</code>\n"

    text += "<code>——————————————————————————————</code>\n"
    text += f"<i>Просмотр Сетки {target_group}</i>"
    
    # 4. Кнопка для возврата/переключения назад
    kb = InlineKeyboardBuilder()
    other_group = "B" if target_group == "A" else "A"
    kb.button(text=f"Сетка {other_group}", callback_data=f"show_table_{other_group}")
    
    # Редактируем старое сообщение
    try:
        await cb.message.edit_text(text, parse_mode="HTML", reply_markup=kb.as_markup())
    except:
        pass # Игнорируем ошибку, если текст остался таким же
    
    await cb.answer()

# --- АДМИНКА ---

@dp.message(Command("reset_all_database"))
async def reset_db_command(m: types.Message):
    if m.from_user.id not in ADMINS: return
    
    import os
    conn = get_db()
    conn.close() # Закрываем соединение перед удалением
    
    try:
        if os.path.exists("players.db"):
            os.remove("players.db")
            init_db() # Сразу пересоздаем структуру таблиц
            await m.answer("🧨 <b>База данных полностью очищена!</b>\nВсе игроки и клубы удалены.", parse_mode="HTML")
        else:
            await m.answer("Файл базы данных не найден.")
    except Exception as e:
        await m.answer(f"Ошибка при удалении: {e}")

@dp.callback_query(F.data == "admin_list_users")
async def admin_list_users(cb: types.callback_query):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db()
    c = conn.cursor()
    c.execute('SELECT user_id, username, club FROM users')
    users = c.fetchall()
    conn.close()
    
    if not users:
        return await cb.answer("В базе пока никого нет.")
    
    text = "📋 <b>Список пользователей в базе:</b>\n\n"
    for uid, name, club in users:
        username = f"@{name}" if name else "Нет юзернейма"
        text += f"👤 {username}\n├ ID: <code>{uid}</code>\n└ Клуб: {club}\n\n"
    
    await cb.message.answer(text, parse_mode="HTML")
    await cb.answer()

@dp.message(F.text == "🛠 Админка")
async def adm(m: types.Message):
    if m.from_user.id not in ADMINS: return
    b = InlineKeyboardBuilder()
    b.button(text="👥 Список юзеров (ID)", callback_data="admin_list_users")
    b.button(text="🎲 Сгенерировать 3-х агентов", callback_data="admin_gen_random_fas")
    b.button(text="🔄 ТО (Открыть/Закрыть)", callback_data="admin_toggle_transfers")
    b.button(text="🚫 Выгнать", callback_data="admin_kick_user")
    b.button(text="💰 Выдать монеты", callback_data="admin_give_money")
    b.button(text="⚖️ Жеребьевка (Сетки A/B)", callback_data="admin_split_leagues")
    b.button(text="🗓 ЗАПУСТИТЬ ПОЛНЫЙ СЕЗОН (Лига+Кубок)", callback_data="admin_full_season_start")
    b.button(text="⚽️ ПРОВЕСТИ MATCH DAY (Все игры)", callback_data="admin_run_matchday")
    b.button(text="🏟 ПРОВЕСТИ МАТЧ ВРУЧНУЮ", callback_data="admin_manual_match")
    b.button(text="📰 Выпустить газету", callback_data="admin_post_news")
    b.button(text="🎬 Слить раздевалку", callback_data="handle_leaks_long")
    b.button(text="👞 Исключить из клуба", callback_data="admin_kick_club")
    b.button(text="💎 Апгрейд рейтинга", callback_data="admin_upgrade_start")
    b.button(text="🚀 Выбросить ТОП-игрока", callback_data="admin_drop_player")
    b.button(text="📢 Сделать рассылку", callback_data="start_broadcast")
    b.button(text="🏁 Завершить сезон и выдать 50кк", callback_data="admin_finish_season")
    b.button(text="🧨 ПОЛНЫЙ СБРОС БАЗЫ", callback_data="admin_full_reset")
    b.adjust(1)
    await m.answer("🔧 Админ-панель:", reply_markup=b.as_markup())

@dp.callback_query(F.data == "handle_leaks_long")
async def handle_leaks_long(cb: types.CallbackQuery):
    # Проверка на админа
    if cb.from_user.id not in ADMINS: 
        await cb.answer("Ты не админ!", show_alert=True)
        return
    
    await cb.answer("🔍 Взламываем диктофон...")

    try:
        # Пытаемся сгенерировать контент
        content = await generate_locker_room_action()
        
        if not content:
            await cb.message.answer("⚠️ Функция вернула пустой текст.")
            return

        # Отправляем ответ
        await cb.message.answer(content, parse_mode="HTML")

    except Exception as e:
        # Если что-то пошло не так (в базе или в коде leaks.py) — ты увидишь это в чате
        error_trace = f"❌ <b>ОШИБКА В LEAKS.PY:</b>\n<code>{e}</code>"
        await cb.message.answer(error_trace, parse_mode="HTML")
        print(f"ERROR: {e}")

# # --- ВСПОМОГАТЕЛЬНАЯ ПРОВЕРКА СОСТАВА (11 игроков) ---
# def get_manual_squad_count(user_id):
#     conn = get_db(); c = conn.cursor()
#     # Считаем именно тех, кто стоит в слотах 1-11
#     c.execute("SELECT COUNT(*) FROM squad WHERE user_id = ? AND slot_id IS NOT NULL", (user_id,))
#     count = c.fetchone()[0]
#     conn.close()
#     return count

# def get_clubs_kb():
#     conn = get_db(); c = conn.cursor()
#     c.execute("SELECT user_id, club FROM users WHERE club IS NOT NULL")
#     clubs = c.fetchall()
#     conn.close()
#     builder = InlineKeyboardBuilder()
#     for u_id, club_name in clubs:
#         builder.button(text=f"🛡 {club_name}", callback_data=f"sel_club_{u_id}")
#     builder.adjust(2)
#     return builder.as_markup()

# # --- ШАГ 1: Старт ---
# @dp.callback_query(F.data == "admin_manual_match")
# async def manual_match_start(cb: types.CallbackQuery, state: FSMContext):
#     await state.clear()
#     await cb.message.answer("📝 Введите название турнира:")
#     await state.set_state(ManualMatch.waiting_for_title)

# # --- ШАГ 2: Название -> Выбор Т1 ---
# @dp.message(ManualMatch.waiting_for_title)
# async def manual_match_title(m: types.Message, state: FSMContext):
#     await state.update_data(title=m.text)
#     await m.answer(f"🏆 {m.text}\n🔹 Выберите 1-й клуб:", reply_markup=get_clubs_kb())
#     await state.set_state(ManualMatch.selecting_t1)

# # --- ШАГ 3: Т1 -> Проверка состава -> Выбор Т2 ---
# async def perform_autofill_logic(user_id, formation_name, c):
#     if not formation_name: formation_name = "4-4-2"
#     try:
#         f_parts = [int(x) for x in formation_name.split('-')]
#     except:
#         f_parts = [4, 4, 2]
    
#     # Очищаем старые слоты перед набором
#     c.execute('UPDATE squad SET slot_id = NULL, status = "reserve" WHERE user_id = ?', (user_id,))
    
#     occupied_slots = []
#     # Логика: Вратарь (1), Защита (f1), Полузащита (f2), Нападение (f3)
#     formation_logic = [("GK", 1), ("DEF", f_parts[0]), ("MID", f_parts[1]), ("FWD", f_parts[2])]
#     current_slot = 1
    
#     for pos_code, limit in formation_logic:
#         # Ищем лучших доступных игроков на позицию
#         # Исправлено: передаем параметры кортежем, чтобы избежать ошибок биндинга
#         query = '''
#             SELECT id FROM squad 
#             WHERE user_id = ? AND pos LIKE ? 
#             AND injury_remaining = 0 AND is_banned = 0 
#             AND (training_until IS NULL OR training_until = '')
#             ORDER BY rating DESC LIMIT ?
#         '''
#         c.execute(query, (user_id, f"%{pos_code}%", limit))
#         players = c.fetchall()
        
#         for row in players:
#             if current_slot <= 11:
#                 c.execute('UPDATE squad SET slot_id = ?, status = "active" WHERE id = ?', (current_slot, row[0]))
#                 occupied_slots.append(current_slot)
#                 current_slot += 1

# # --- ИСПРАВЛЕННЫЕ ХЕНДЛЕРЫ ВЫБОРА КЛУБОВ ---
# @dp.callback_query(ManualMatch.selecting_t1, F.data.startswith("sel_club_"))
# async def manual_match_t1_selected(cb: types.CallbackQuery, state: FSMContext):
#     t_id = int(cb.data.replace("sel_club_", ""))
#     conn = get_db(); c = conn.cursor()
    
#     c.execute("SELECT club FROM users WHERE user_id=?", (t_id,))
#     name = c.fetchone()[0]
    
#     # ИСПРАВЛЕНО: используем player_name и pos (согласно твоим ошибкам в БД)
#     c.execute("SELECT player_name, pos, rating FROM squad WHERE user_id=? AND status='active' ORDER BY slot_id", (t_id,))
#     rows = c.fetchall()
#     sq = "\n".join([f"• {r[1]} {r[0]} ({r[2]})" for r in rows])
#     conn.close()

#     await state.update_data(t1_id=t_id, t1_name=name)
    
#     # Проверяем количество игроков (должно быть 11)
#     count = len(rows)
#     kb = InlineKeyboardBuilder()
#     if count < 11:
#         kb.button(text=f"🤖 Собрать состав ({count}/11)", callback_data=f"man_fill_{t_id}_t1")
#     kb.button(text="➡️ Далее (Выбор Т2)", callback_data="man_step_2")
#     kb.adjust(1)

#     await cb.message.answer(
#         f"✅ Команда 1: {name}\n📋 Состав:\n{sq if sq else 'Пусто'}\n\nВыберите 2-й клуб:",
#         reply_markup=kb.as_markup()
#     )

# # --- ШАГ 4: Т2 -> Проверка состава -> Кнопка старта ---
# @dp.callback_query(ManualMatch.selecting_t2, F.data.startswith("sel_club_"))
# async def manual_match_t2_selected(cb: types.CallbackQuery, state: FSMContext):
#     t2_id = int(cb.data.replace("sel_club_", ""))
#     data = await state.get_data()
    
#     if t2_id == data['t1_id']: 
#         return await cb.answer("❌ Нельзя играть с самим собой!", show_alert=True)

#     t1_count = get_manual_squad_count(data['t1_id'])
#     t2_count = get_manual_squad_count(t2_id)
    
#     conn = get_db(); c = conn.cursor()
#     c.execute("SELECT club FROM users WHERE user_id=?", (t2_id,))
#     t2_name = c.fetchone()[0]
#     # ИСПРАВЛЕНО: player_name
#     c.execute("SELECT player_name, pos, rating FROM squad WHERE user_id=? AND status='active'", (t2_id,))
#     sq_rows = c.fetchall()
#     sq = "\n".join([f"• {p} {n} ({r})" for n, p, r in sq_rows])
#     conn.close()
    
#     await state.update_data(t2_id=t2_id, t2_name=t2_name)

#     kb = InlineKeyboardBuilder()
#     if t1_count < 11:
#         kb.button(text=f"🤖 Собрать {data['t1_name']}", callback_data=f"manual_autofill_{data['t1_id']}_t2")
#     if t2_count < 11:
#         kb.button(text=f"🤖 Собрать {t2_name}", callback_data=f"manual_autofill_{t2_id}_t2")
    
#     if t1_count >= 11 and t2_count >= 11:
#         kb.button(text="⚽️ ПРОВЕСТИ МАТЧ", callback_data="run_manual_final")
#     else:
#         kb.button(text="🔄 Обновить статус", callback_data="manual_refresh")
    
#     kb.adjust(1)
    
#     msg = (f"✅ <b>Команда 2:</b> {t2_name}\n📋 <b>Состав:</b>\n{sq if sq else '<i>Пусто</i>'}\n\n"
#            f"⚔️ <b>{data['t1_name']} vs {t2_name}</b>\n"
#            f"📊 Статус: <b>{t1_count}/11</b> vs <b>{t2_count}/11</b>")

#     await cb.message.answer(msg, reply_markup=kb.as_markup(), parse_mode="HTML")

# # --- ОБРАБОТЧИК КНОПКИ "СОБРАТЬ СОСТАВ" ВНУТРИ МАНУАЛА ---
# @dp.callback_query(F.data.startswith("manual_autofill_"))
# async def manual_autofill_handler(cb: types.CallbackQuery, state: FSMContext):
#     parts = cb.data.split("_")
#     uid, step = int(parts[2]), parts[3]
    
#     conn = get_db(); c = conn.cursor()
#     c.execute('SELECT formation FROM users WHERE user_id = ?', (uid,))
#     form = c.fetchone()[0] or "4-4-2"
#     # Вызываем твою логику автосбора
#     await perform_autofill_logic(uid, form, c)
#     conn.commit(); conn.close()
    
#     await cb.answer("✅ Состав собран!")
#     # Обновляем сообщение в зависимости от того, на каком мы шаге
#     if step == "t1": await manual_match_t1_selected(cb, state)
#     else: await manual_match_t2_selected(cb, state)

# @dp.callback_query(F.data == "manual_refresh")
# async def manual_refresh(cb: types.CallbackQuery, state: FSMContext):
#     await manual_match_t2_selected(cb, state)

# # --- ШАГ 5: ФИНАЛЬНЫЙ ЗАПУСК (ТВОЯ ЛОГИКА UCL) ---
# @dp.callback_query(F.data == "run_manual_final")
# async def run_manual_final(cb: types.CallbackQuery, state: FSMContext):
#     data = await state.get_data()
#     if not data: return await cb.answer("🚨 Ошибка данных!")
    
#     await state.clear()
#     await cb.message.delete()

#     t1_id, t2_id = data['t1_id'], data['t2_id']
#     t1_n, t2_n = data['t1_name'], data['t2_name']

#     # Симуляция UCL
#     res = await play_ucl_match_logic(t1_id, t2_id, t1_n, t2_n)
#     if "error" in res: return await cb.message.answer(f"❌ {res['error']}")

#     # Логика пенальти (если ничья)
#     is_pen = False
#     pen_report = ""
#     winner_id = t1_id if res['h_s'] > res['a_s'] else t2_id
#     winner_name = t1_n if res['h_s'] > res['a_s'] else t2_n

#     if res['h_s'] == res['a_s']:
#         is_pen = True
#         h_p, a_p = random.randint(4, 5), random.randint(3, 4)
#         while h_p == a_p: h_p += 1
#         h_vis_str, a_vis_str = "✅✅✅❌✅", "✅✅❌✅❌"
        
#         pen_report = (f"\n 🎯 <b>Пен: ({h_p}:{a_p})</b>\n"
#                       f"<code>└───────────────────────┘</code>\n\n"
#                       f"🥅 <b>Серия:</b>\n🏠 : {h_vis_str}\n🚀 : {a_vis_str}\n")
#         winner_id = t1_id if h_p > a_p else t2_id
#         winner_name = t1_n if h_p > a_p else t2_n

#     events_html = "\n".join(res['log']) if res['log'] else "<i>— Без моментов</i>"

#     # MVP и Топ игроки
#     conn = get_db(); c = conn.cursor()
#     c.execute("SELECT player_name FROM squad WHERE user_id = ? AND status = 'active'", (t1_id,))
#     h_players = [{"player_name": r[0]} for r in c.fetchall()]
#     c.execute("SELECT player_name FROM squad WHERE user_id = ? AND status = 'active'", (t2_id,))
#     a_players = [{"player_name": r[0]} for r in c.fetchall()]
#     conn.close()

#     fake_events = []
#     for log_entry in res['log']:
#         try:
#             min_val = int(log_entry.split("'")[0].split(" ")[-1])
#             fake_events.append([min_val, log_entry])
#         except: fake_events.append([0, log_entry])

#     performers = get_match_performers(h_players, a_players, fake_events, t1_n, t2_n, winner_name)
#     mvp_text = " | ".join(performers)

#     final_report = (
#         f"🏆 <b>{data['title'].upper()}</b>\n"
#         f"<code>┌───────────────────────┐</code>\n"
#         f"  <b>{t1_n} {res['h_s']}:{res['a_s']} {t2_n}</b>"
#         f"{pen_report if is_pen else pen_report + '<code>└───────────────────────┘</code>'}\n\n"
#         f"📋 <b>Ключевые моменты:</b>\n"
#         f"{events_html}\n\n"
#         f"🌟 <b>Топ игроки:</b>\n"
#         f"{mvp_text}\n"
#         f"<code>———————————————————————</code>\n"
#         f"🎉 <b>ПОБЕДИТЕЛЬ: {winner_name}!</b>"
#     )

#     try: await apply_match_aftermath(cb.bot, t1_id, t2_id, winner_id, is_cup=True)
#     except: pass

#     await cb.message.answer(final_report, parse_mode="HTML")
#     await cb.answer()

@dp.callback_query(F.data == "admin_split_leagues")
async def split_leagues(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id, club FROM users WHERE club IS NOT NULL')
    teams = c.fetchall()
    n_teams = len(teams)
    
    # Проверка на минимальное количество
    if n_teams < 4:
        conn.close()
        return await cb.answer(f"❌ Нужно минимум 4 команды! Сейчас: {n_teams}", show_alert=True)
    
    # Проверка на четность
    if n_teams % 2 != 0:
        conn.close()
        return await cb.answer(f"❌ Нечетное количество клубов ({n_teams})! Нужно четное число для равного деления.", show_alert=True)
    
    import random
    random.shuffle(teams) 
    
    mid = n_teams // 2
    group_a = teams[:mid]
    group_b = teams[mid:]
    
    # Записываем группу А
    for user_id, club in group_a:
        c.execute('UPDATE users SET league_group = "A" WHERE user_id = ?', (user_id,))
        try: await cb.bot.send_message(user_id, f"⚖️ Жеребьевка: Ваш клуб {club} попал в СЕТКУ А! 🔥", parse_mode="HTML")
        except: pass

    # Записываем группу Б
    for user_id, club in group_b:
        c.execute('UPDATE users SET league_group = "B" WHERE user_id = ?', (user_id,))
        try: await cb.bot.send_message(user_id, f"⚖️ Жеребьевка: Ваш клуб {club} попал в СЕТКУ Б! 💪", parse_mode="HTML")
        except: pass
        
    conn.commit(); conn.close()
    
    await cb.message.answer(
        f"⚖️ <b>ЖЕРЕБЬЕВКА ЗАВЕРШЕНА</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"Всего клубов: {n_teams}\n"
        f"🅰️ Сетка А: {len(group_a)}\n"
        f"🅱️ Сетка Б: {len(group_b)}\n\n"
        f"Все участники получили уведомления!", 
        parse_mode="HTML"
    )
    await cb.answer()

@dp.callback_query(F.data == "admin_run_matchday")
async def run_matchday(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    
    # 1. Проверяем, остались ли вообще матчи в лиге
    c.execute('SELECT COUNT(*) FROM league_schedule WHERE is_played = 0')
    remaining_matches = c.fetchone()[0]

    if remaining_matches == 0:
        # ВАЖНО: Если матчей лиги нет, запускаем Золотой Матч (Финал Сезона)
        conn.close()
        # Вызываем функцию завершения сезона (ту самую, с интригой)
        return await callback_finish_season(cb)

    # 2. Получаем текущий тур
    c.execute('SELECT value FROM settings WHERE key = "window_counter"')
    curr_tour_data = c.fetchone()
    if not curr_tour_data:
        conn.close()
        return await cb.answer("Ошибка: window_counter не найден в базе.")
    
    curr_tour = int(curr_tour_data[0])
    
    # 3. Проверяем на кубковый перерыв
    c.execute('SELECT is_cup_break FROM league_schedule WHERE tour_number = ? LIMIT 1', (curr_tour,))
    res = c.fetchone()
    is_cup_time = res[0] if res else 0

    # 4. ЛОГИКА КУБКА
    if is_cup_time == 1:
        c.execute('SELECT stage FROM cup_bracket WHERE winner_id IS NULL LIMIT 1')
        active_cup = c.fetchone()
        
        if active_cup:
            conn.close()
            await run_cup_stage(cb) 
            
            # Апаем счетчик после кубка
            conn = get_db(); c = conn.cursor()
            c.execute('UPDATE settings SET value = value + 1 WHERE key = "window_counter"')
            conn.commit(); conn.close()
            return 
        else:
            update_cup_path() # Если сетки нет — создаем

    # 5. ЛОГИКА ЛИГИ
    conn.close()
    await run_league_tour(cb) 
    
    # 6. Финальный отчет
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT value FROM settings WHERE key = "window_counter"')
    new_tour_val = c.fetchone()[0]
    conn.close()

    played_tour = int(new_tour_val) - 1
    await cb.message.answer(
        f"🚀 <b>ИГРОВОЙ ДЕНЬ ЗАВЕРШЕН</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"⚽️ Тур <b>{played_tour}</b> успешно сыгран.\n"
        f"📋 Следующий по расписанию: <b>{new_tour_val}</b>",
        parse_mode="HTML"
    )

@dp.message(AdminStates.waiting_for_season_name)
async def process_admin_full_season_start(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMINS: return
    season_name = m.text.strip()
    conn = get_db(); c = conn.cursor()
    
    try:
        # Миграции (на всякий случай)
        try: c.execute('ALTER TABLE league_schedule ADD COLUMN is_cup_break INTEGER DEFAULT 0')
        except: pass 
        try: c.execute('ALTER TABLE league_schedule ADD COLUMN league_group TEXT DEFAULT "A"')
        except: pass
        try: c.execute('ALTER TABLE league_schedule ADD COLUMN is_played INTEGER DEFAULT 0')
        except: pass

        # Обновление настроек
        c.execute('INSERT OR REPLACE INTO settings (key, value) VALUES ("current_season", ?)', (season_name,))
        c.execute('UPDATE settings SET value = 1 WHERE key = "window_counter"')
        c.execute('UPDATE users SET league_wins = 0, league_draws = 0, league_losses = 0, league_goals = 0')
        
        # Полная очистка старых турниров
        c.execute('DELETE FROM league_schedule')
        c.execute('DELETE FROM cup_bracket')
        c.execute('DELETE FROM ucl_bracket') # Очищаем ЛЧ при старте любого сезона
        c.execute('DELETE FROM sqlite_sequence WHERE name="cup_bracket"') 
        c.execute('DELETE FROM sqlite_sequence WHERE name="ucl_bracket"') 
        
        # --- ГЕНЕРАЦИЯ ЛИГИ ДЛЯ КАЖДОЙ СЕТКИ (ТВОЙ КОД) ---
        for grp in ["A", "B"]:
            c.execute('SELECT user_id FROM users WHERE club IS NOT NULL AND league_group = ?', (grp,))
            uids = [row[0] for row in c.fetchall()]
            
            if len(uids) < 2: continue 
            
            import random
            random.shuffle(uids)
            if len(uids) % 2 != 0: uids.append(None)
            
            n = len(uids)
            for r in range((n - 1) * 2):
                round_num = r + 1
                is_break = 1 if round_num % 3 == 0 else 0 
                for i in range(n // 2):
                    t1, t2 = uids[i], uids[n - 1 - i]
                    if t1 and t2:
                        c.execute('''INSERT INTO league_schedule 
                                     (home_id, away_id, tour_number, is_cup_break, league_group, is_played) 
                                     VALUES (?, ?, ?, ?, ?, 0)''', (t1, t2, round_num, is_break, grp))
                uids.insert(1, uids.pop())

        # --- ЛОГИКА ЛЧ (ТОЛЬКО ЕСЛИ ЭТО НЕ ПЕРВЫЙ СЕЗОН) ---
        # Проверяем, есть ли уже данные для отбора (топ-8)
        # Если это старт 2-го сезона, мы должны наполнить ucl_bracket
        if season_name != "25/26": # Замени "25/26" на название своего САМОГО ПЕРВОГО сезона
            c.execute('''SELECT user_id, club FROM users WHERE league_group = "A" 
                         ORDER BY (league_wins*3 + league_draws) DESC, league_goals DESC LIMIT 8''')
            top_a = c.fetchall()
            c.execute('''SELECT user_id, club FROM users WHERE league_group = "B" 
                         ORDER BY (league_wins*3 + league_draws) DESC, league_goals DESC LIMIT 8''')
            top_b = c.fetchall()

            if len(top_a) == 8 and len(top_b) == 8:
                # Жеребьевка 1/8: A1 vs B8, A2 vs B7 и т.д.
                top_b.reverse() 
                for i in range(8):
                    c.execute('''INSERT INTO ucl_bracket (stage, t1_id, t1_name, t2_id, t2_name) 
                                 VALUES ("1/8", ?, ?, ?, ?)''', 
                              (top_a[i][0], top_a[i][1], top_b[i][0], top_b[i][1]))
                ucl_msg = "\n🇪🇺 Лига Чемпионов на этот сезон сформирована!"
            else:
                ucl_msg = "\n⚠️ Недостаточно игроков для ЛЧ (нужно по 8 в группах A и B)."
        else:
            ucl_msg = "\n🐣 Это первый сезон. ЛЧ начнется со следующего!"

        # --- ЛОГИКА КУБКА (ТВОЙ КОД) ---
        c.execute('SELECT user_id, club FROM users WHERE club IS NOT NULL ORDER BY (league_wins*3 + league_draws) DESC')
        teams_data = c.fetchall()
        n_teams = len(teams_data)

        if n_teams >= 6:
            # ... тут твой код генерации кубка без изменений ...
            if 6 <= n_teams <= 7: target, next_stage = 4, "1/2"
            elif 8 <= n_teams <= 15: target, next_stage = 8, "1/4"
            elif 16 <= n_teams <= 31: target, next_stage = 16, "1/8"
            else: target, next_stage = 32, "1/16"

            num_pi = (n_teams - target) * 2 if n_teams > target else 0
            auto_pass = teams_data[:n_teams - num_pi]
            pi_pool = teams_data[n_teams - num_pi:]

            for i in range(0, len(pi_pool), 2):
                c.execute("INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) VALUES ('Play-In', ?, ?, ?, ?)", (pi_pool[i][0], pi_pool[i][1], pi_pool[i+1][0], pi_pool[i+1][1]))

            for i in range(0, len(auto_pass), 2):
                if i+1 < len(auto_pass):
                    c.execute(f"INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) VALUES (?, ?, ?, ?, ?)", (next_stage, auto_pass[i][0], auto_pass[i][1], auto_pass[i+1][0], auto_pass[i+1][1]))
                else:
                    c.execute(f"INSERT INTO cup_bracket (stage, t1_id, t1_name) VALUES (?, ?, ?)", (next_stage, auto_pass[i][0], auto_pass[i][1]))

            c.execute("SELECT COUNT(*) FROM cup_bracket WHERE stage = ?", (next_stage,))
            cur_m = c.fetchone()[0]
            for _ in range((target // 2) - cur_m):
                c.execute("INSERT INTO cup_bracket (stage) VALUES (?)", (next_stage,))

        conn.commit()
        await m.answer(f"✅ Сезон {season_name} настроен!\nГруппы A и B сгенерированы отдельно.{ucl_msg}")
        await state.clear()
    except Exception as e: 
        print(f"Ошибка старта сезона: {e}")
        await m.answer(f"❌ Ошибка: {e}")
    finally: conn.close()

@dp.callback_query(F.data == "admin_full_season_start")
async def ask_season_name(cb: types.CallbackQuery, state: FSMContext):
    if cb.from_user.id not in ADMINS: return
    
    await cb.message.answer("📝 Введите название нового сезона\nНапример: 25/26")
    await state.set_state(AdminStates.waiting_for_season_name)
    await cb.answer()
        
@dp.callback_query(F.data == "run_cup_stage")
async def run_cup_stage(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT stage FROM cup_bracket WHERE winner_id IS NULL LIMIT 1")
    row = c.fetchone()
    if not row: return await cb.answer("Все матчи сыграны!")
    
    current_stage = row[0]
    c.execute("SELECT id, t1_id, t2_id, t1_name, t2_name, first_leg_score FROM cup_bracket WHERE stage = ? AND winner_id IS NULL", (current_stage,))
    matches = c.fetchall()
    conn.close() 

    report = f"🏆 <b>КУБОК: {current_stage}</b>\n━━━━━━━━━━━━━━━━━━━━\n\n"

    for m_id, t1_id, t2_id, t1_n, t2_n, fl_score in matches:
        prev_score = (0, 0)
        use_ot = True 
        if current_stage == '1/2' and fl_score is None: use_ot = False 
        elif fl_score:
            h_p, a_p = map(int, fl_score.split(':'))
            prev_score = (h_p, a_p)

        # Вызываем твою обновленную функцию
        res = await play_cup_match_full(t1_id, t2_id, t1_n, t2_n, cb.bot, prev_score=prev_score, use_extra_time=use_ot)
        
        # Оформление для админ-отчета
        match_card = f"<b>{t1_n} {res['h_s']}:{res['a_s']} {t2_n}</b>\n"
        if res['h_p'] is not None:
            match_card += f"🎯 Пенальти: <b>{res['h_p']}:{res['a_p']}</b>\n"
        
        # Фикс вывода событий (если событие - кортеж (мин, текст), берем текст)
        formatted_events = []
        for e in res['events'][-3:]:
            txt = e[1] if isinstance(e, tuple) else e
            formatted_events.append(f"└ {txt}")
        
        match_card += "\n".join(formatted_events) + "\n"
        report += match_card + "————————————————————\n"

        # Победитель
        total_h = res['h_s'] + prev_score[0] + (res['h_p'] or 0)
        total_a = res['a_s'] + prev_score[1] + (res['a_p'] or 0)
        w_id = t1_id if total_h > total_a else t2_id

        # Применяем последствия (усталость, травмы в БД)
        await apply_match_aftermath(cb.bot, t1_id, t2_id, w_id, is_final=(current_stage == 'Final'), is_cup=True)

        # ПУШ-УВЕДОМЛЕНИЯ
        for p_id in [t1_id, t2_id]:
            try:
                status_icon = "🎉" if p_id == w_id else "💔"
                status_text = "<b>ПРОХОД В СЛЕДУЮЩИЙ РАУНД!</b>" if p_id == w_id else "<b>ВЫЛЕТ ИЗ ТУРНИРА</b>"
                
                # Формируем текст событий для пуша
                push_events = []
                for e in res["events"][-5:]:
                    txt = e[1] if isinstance(e, tuple) else e
                    push_events.append(txt)
                events_list = "\n".join(push_events)
                
                push_msg = (
                    f"🏆 <b>КУБОК: {current_stage.upper()}</b>\n"
                    f"<code>┏━━━━━━━━━━━━━━━━━━━━┓</code>\n"
                    f"  {t1_n} <b>{res['h_s']}:{res['a_s']}</b> {t2_n}\n"
                )
                
                if res['h_p'] is not None:
                    push_msg += f"  🎯 Пен: <b>({res['h_p']}:{res['a_p']})</b>\n"
                
                push_msg += (
                    f"<code>┗━━━━━━━━━━━━━━━━━━━━┛</code>\n\n"
                    f"📝 <b>Ключевые моменты:</b>\n"
                    f"{events_list if events_list else 'Матч прошел в равной борьбе.'}\n\n"
                )

                if res['h_p'] is not None:
                    # Используем твой новый pen_report_text для красоты
                    push_msg += f"🥅 Серия:\n{res.get('pen_report_text', '')}\n\n"

                push_msg += (
                    f"🌟 >Топ игроки:\n"
                    f"{res.get('mvp_text', 'Не определены')}\n"
                    f"————————————————————\n"
                    f"{status_icon} {status_text}"
                )

                await cb.bot.send_message(p_id, push_msg, parse_mode="HTML")
            except Exception as e:
                print(f"Ошибка пуша кубка {p_id}: {e}")

        # Сохранение результатов
        conn = get_db(); c = conn.cursor()
        if current_stage == '1/2' and fl_score is None:
            # ЭТО ПЕРВЫЙ МАТЧ. Просто пишем счет и НЕ ставим winner_id
            c.execute("UPDATE cup_bracket SET first_leg_score = ?, h_score = ?, a_score = ? WHERE id = ?", 
                     (f"{res['h_s']}:{res['a_s']}", res['h_s'], res['a_s'], m_id))
            is_first_leg_done = True
        else:
            # ЭТО ЛИБО ОБЫЧНЫЙ РАУНД, ЛИБО ОТВЕТКА 1/2
            c.execute("UPDATE cup_bracket SET winner_id=?, h_score=?, a_score=?, h_pen=?, a_pen=? WHERE id=?", 
                     (w_id, res['h_s'], res['a_s'], res.get('h_p'), res.get('a_p'), m_id))
            is_first_leg_done = False
        conn.commit(); conn.close()

    # --- ПОСЛЕ ЦИКЛА (ГЕНЕРАЦИЯ СЛЕДУЮЩЕЙ СТАДИИ) ---
    conn = get_db(); c = conn.cursor()
    
    # Считаем, сколько матчей реально завершено (есть победитель)
    c.execute("SELECT COUNT(*) FROM cup_bracket WHERE stage = ? AND winner_id IS NULL", (current_stage,))
    remaining = c.fetchone()[0]

    if remaining == 0:
        # Если реально всё доиграли (включая ответки) — только тогда двигаем
        update_cup_path()
        report += f"\n🚀 Стадия {current_stage} полностью завершена! Победители в сетке."
    else:
        if current_stage == '1/2':
            report += f"\n🏟 Первые матчи 1/2 завершены!\nОтветные матчи пройдут через несколько туров Лиги."
            # СТАВИМ ПАУЗУ: переключаем в настройках, чтобы следующий "Игровой день" был Лигой
            c.execute("UPDATE settings SET value = 'league' WHERE key = 'next_match_type'") 
        else:
            report += f"\n⏳ Осталось доиграть: {remaining}"
    
    conn.commit(); conn.close()
    
    # И в самом конце отправляем итоговый отчет админу
    await cb.message.answer(report, parse_mode="HTML")

async def apply_match_aftermath(bot, t1_id, t2_id, winner_id, is_final=False, is_cup=False):
    # ЛОГИКА ИНТЕРВЬЮ (без лечения!)
    loser_id = t2_id if winner_id == t1_id else t1_id

    if is_final:
        # В финале — оба
        await start_interview(bot, winner_id, "cup_win" if is_cup else "league_win")
        await start_interview(bot, loser_id, "loss")
    else:
        # В обычных матчах — один случайный
        target = random.choice([winner_id, loser_id])
        sit = ("win" if target == winner_id else "cup_out") if is_cup else ("win" if target == winner_id else "loss")
        await start_interview(bot, dp, target, sit)

def global_squad_update():
    conn = get_db(); c = conn.cursor()
    # Уменьшаем сроки всем игрокам в лиге
    c.execute('UPDATE squad SET injury_remaining = injury_remaining - 1 WHERE injury_remaining > 0')
    c.execute('UPDATE squad SET is_banned = is_banned - 1 WHERE is_banned > 0')
    # Возвращаем в актив
    c.execute('''UPDATE squad SET injury_remaining = 0, status = 'active' 
                 WHERE injury_remaining <= 0 AND is_banned <= 0 AND status != 'active' ''')
    conn.commit(); conn.close()

@dp.callback_query(F.data == "admin_init_cup")
async def admin_init_cup(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    c.execute('DROP TABLE IF EXISTS cup_bracket')
    c.execute('''CREATE TABLE cup_bracket (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stage TEXT, t1_id INTEGER, t1_name TEXT, t2_id INTEGER, t2_name TEXT,
        winner_id INTEGER, h_score INTEGER DEFAULT 0, a_score INTEGER DEFAULT 0,
        h_pen INTEGER, a_pen INTEGER, status TEXT DEFAULT 'pending',
        first_leg_score TEXT DEFAULT NULL
    )''')

    # 1. Получаем участников, сортируя по силе (очкам в лиге)
    c.execute('SELECT user_id, club FROM users WHERE club IS NOT NULL ORDER BY (wins*3 + draws) DESC')
    teams = c.fetchall()
    n = len(teams)

    if n < 2: 
        return await cb.answer("Мало людей для кубка!", show_alert=True)

    # --- ЖЕСТКАЯ ЛОГИКА ОПРЕДЕЛЕНИЯ ЦЕЛИ (ОТ 6 ДО 40) ---
    # Группа 1: Цель — 4 команды (Стадия 1/2)
    if 6 <= n <= 7:
        target, next_stage = 4, "1/2"
    # Группа 2: Цель — 8 команд (Стадия 1/4)
    elif 8 <= n <= 15:
        target, next_stage = 8, "1/4"
    # Группа 3: Цель — 16 команд (Стадия 1/8)
    elif 16 <= n <= 31:
        target, next_stage = 16, "1/8"
    # Группа 4: Цель — 32 команды (Стадия 1/16)
    elif 32 <= n <= 40:
        target, next_stage = 32, "1/16"
    else:
        # Для случаев менее 6 команд
        target, next_stage = (2, "Финал") if n < 4 else (4, "1/2")

    # Математика Play-In: (Всего - Цель) * 2
    num_pi_players = (n - target) * 2 if n > target else 0
    num_bye_players = n - num_pi_players

    # Пул Play-In (слабые) и Пул Bye (сильные)
    pi_pool = teams[num_bye_players:]
    bye_pool = teams[:num_bye_players]

    # --- ШАГ 1: Создаем матчи Play-In ---
    for i in range(0, len(pi_pool), 2):
        c.execute("""INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) 
                     VALUES ('Play-In', ?, ?, ?, ?)""",
                  (pi_pool[i][0], pi_pool[i][1], pi_pool[i+1][0], pi_pool[i+1][1]))

    # --- ШАГ 2: Создаем пары в основной сетке (1/16, 1/8, 1/4 или 1/2) ---
    # Ставим пары из тех, кто прошел напрямую
    for i in range(0, len(bye_pool) // 2 * 2, 2):
        c.execute("""INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) 
                     VALUES (?, ?, ?, ?, ?)""",
                  (next_stage, bye_pool[i][0], bye_pool[i][1], bye_pool[i+1][0], bye_pool[i+1][1]))

    # Если остался один "счастливчик" без пары, он ждет победителя из Play-In
    if len(bye_pool) % 2 != 0:
        c.execute("""INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) 
                     VALUES (?, ?, ?, NULL, NULL)""",
                  (next_stage, bye_pool[-1][0], bye_pool[-1][1]))

    # --- ШАГ 3: Добиваем пустые слоты под будущих победителей Play-In ---
    current_slots = (len(bye_pool) + 1) // 2
    total_needed_slots = target // 2
    
    for _ in range(total_needed_slots - current_slots):
        c.execute("INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) VALUES (?, NULL, NULL, NULL, NULL)", (next_stage,))

    conn.commit()
    conn.close()

    # Уведомление для админа
    res_msg = [
        f"🏆 <b>Кубок сформирован!</b>",
        f"👥 Всего участников: <b>{n}</b>",
        f"🎯 Основная сетка: <b>{next_stage}</b>",
        f"⚔️ Матчей Play-In: <b>{num_pi_players // 2}</b>",
        f"✅ Прошли напрямую: <b>{num_bye_players}</b>"
    ]
    await cb.message.answer("\n".join(res_msg), parse_mode="HTML")
    
def update_cup_path():
    conn = get_db(); c = conn.cursor()
    
    # Строгий порядок стадий
    stages_order = ['Play-In', '1/16', '1/8', '1/4', '1/2', 'Финал']
    
    for i in range(len(stages_order) - 1):
        curr_stage = stages_order[i]
        next_stage = stages_order[i+1]
        
        # 1. Берем победителей текущей стадии, которые еще не перешли выше
        c.execute("""
            SELECT winner_id, 
                   (SELECT club FROM users WHERE user_id = winner_id) 
            FROM cup_bracket 
            WHERE stage = ? AND winner_id IS NOT NULL 
            AND winner_id NOT IN (
                SELECT t1_id FROM cup_bracket WHERE stage = ? AND t1_id IS NOT NULL
                UNION
                SELECT t2_id FROM cup_bracket WHERE stage = ? AND t2_id IS NOT NULL
            )
        """, (curr_stage, next_stage, next_stage))
        
        winners_to_move = c.fetchall()
        
        for w_id, w_name in winners_to_move:
            # 2. ПРИОРУТЕТ 1: Ищем матч, где ОДИН слот уже занят (ждем соперника)
            # Ищем где t1 есть, а t2 пуст
            c.execute("SELECT id FROM cup_bracket WHERE stage = ? AND t1_id IS NOT NULL AND t2_id IS NULL LIMIT 1", (next_stage,))
            row = c.fetchone()
            if row:
                c.execute("UPDATE cup_bracket SET t2_id = ?, t2_name = ? WHERE id = ?", (w_id, w_name, row[0]))
                continue

            # Ищем где t2 есть, а t1 пуст (на всякий случай)
            c.execute("SELECT id FROM cup_bracket WHERE stage = ? AND t2_id IS NOT NULL AND t1_id IS NULL LIMIT 1", (next_stage,))
            row = c.fetchone()
            if row:
                c.execute("UPDATE cup_bracket SET t1_id = ?, t1_name = ? WHERE id = ?", (w_id, w_name, row[0]))
                continue

            # 3. ПРИОРИТЕТ 2: Ищем полностью пустой созданный слот (оба NULL)
            c.execute("SELECT id FROM cup_bracket WHERE stage = ? AND t1_id IS NULL AND t2_id IS NULL LIMIT 1", (next_stage,))
            row = c.fetchone()
            if row:
                c.execute("UPDATE cup_bracket SET t1_id = ?, t1_name = ? WHERE id = ?", (w_id, w_name, row[0]))
                continue

            # 4. ФИНАЛЬНЫЙ ВАРИАНТ: Если места не было вообще — создаем новую строку
            c.execute("INSERT INTO cup_bracket (stage, t1_id, t1_name) VALUES (?, ?, ?)", (next_stage, w_id, w_name))

    conn.commit()
    conn.close()

async def play_ucl_match_logic(t1_id, t2_id, t1_name, t2_name):
    score = {t1_id: 0, t2_id: 0}
    events = []
    
    conn = get_db(); c = conn.cursor()
    # Берем только активных игроков
    c.execute("SELECT id, player_name FROM squad WHERE user_id = ? AND status = 'active'", (t1_id,))
    s1 = c.fetchall()
    c.execute("SELECT id, player_name FROM squad WHERE user_id = ? AND status = 'active'", (t2_id,))
    s2 = c.fetchall()
    conn.close()

    if not s1 or not s2:
        return {"error": "Недостаточно игроков для матча!"}

    # Симуляция по таймингам
    for minute in range(5, 91, 12):
        event_roll = random.random()
        team_id = random.choice([t1_id, t2_id])
        curr_squad = s1 if team_id == t1_id else s2
        opp_squad = s2 if team_id == t1_id else s1
        t_name = t1_name if team_id == t1_id else t2_name
        p = random.choice(curr_squad)

        # 1. Гол + Ассист
        if event_roll < 0.25:
            score[team_id] += 1
            events.append(f"⚽️ {minute}' <b>ГОООЛ!</b> {p[1]} ({t_name}) прошивает сетку!")
            update_ucl_stat(p[0], team_id, 'goals')
            if random.random() < 0.8: # Шанс на ассист
                ast = random.choice([x for x in curr_squad if x[0] != p[0]])
                update_ucl_stat(ast[0], team_id, 'assists')
                events.append(f"🅰️ Блестящий пас от {ast[1]}!")

        # 2. VAR и отмена
        elif event_roll < 0.35:
            events.append(f"🖥 {minute}' <b>VAR!</b> Судья чертит линии... Гол {t_name} <b>ОТМЕНЕН</b>! Офсайд!")

        # 3. Потасовка и карточки
        elif event_roll < 0.45:
            p2 = random.choice(opp_squad)
            events.append(f"🥊 {minute}' <b>ПОТАСОВКА!</b> {p[1]} и {p2[1]} сошлись в рукопашной!")
            update_ucl_stat(p[0], team_id, 'yellow_cards')
            update_ucl_stat(p2[0], (t2_id if team_id == t1_id else t1_id), 'yellow_cards')
            events.append(f"🟨 Обоим зачинщикам по горчичнику!")

        # 4. Жесткий фол и Красная
        elif event_roll < 0.50:
            events.append(f"🟥 {minute}' <b>ГРУБЕЙШИЙ ФОЛ!</b> {p[1]} ({t_name}) прямой ногой влетает в соперника! Удаление!")
            update_ucl_stat(p[0], team_id, 'red_cards')

        # 5. Штанга / Сейв
        elif event_roll < 0.65:
            moments = [f"💥 ШТАНГА! {p[1]} бьет в каркас!", f"🧤 СЕЙВ! Вратарь тащит мертвый мяч после удара {p[1]}!"]
            events.append(f"🏟 {minute}' {random.choice(moments)}")

    return {
        "h_s": score[t1_id], "a_s": score[t2_id],
        "score_str": f"{score[t1_id]}:{score[t2_id]}",
        "log": events
    }

# Вспомогательная функция для записи в таблицу ucl_stats
def update_ucl_stat(player_id, user_id, column):
    conn = get_db(); c = conn.cursor()
    # Проверяем, есть ли уже запись игрока в стате ЛЧ
    c.execute("INSERT OR IGNORE INTO ucl_stats (player_id, user_id) VALUES (?, ?)", (player_id, user_id))
    c.execute(f"UPDATE ucl_stats SET {column} = {column} + 1 WHERE player_id = ?", (player_id,))
    conn.commit(); conn.close()

def get_penalties_visual(history):
    # history — это список True/False (забил/промах)
    return "".join(["✅" if hit else "❌" for hit in history])

@dp.callback_query(F.data == "run_ucl_stage")
async def run_ucl_stage_full(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT stage FROM ucl_bracket WHERE winner_id IS NULL LIMIT 1")
    stage_row = c.fetchone()
    
    if not stage_row: 
        return await cb.answer("❌ Все матчи ЛЧ завершены!")
    
    current_stage = stage_row[0]
    c.execute("SELECT id, t1_id, t2_id, t1_name, t2_name FROM ucl_bracket WHERE stage = ? AND winner_id IS NULL", (current_stage,))
    matches = c.fetchall()
    conn.close()

    await cb.message.edit_text(f"🇪🇺 <b>UCL: Симуляция стадии {current_stage}...</b>", parse_mode="HTML")

    for m_id, t1_id, t2_id, t1_name, t2_name in matches:
        # Используем мощный движок симуляции с поддержкой пенальти
        # play_ucl_match_logic должен возвращать детальный словарь
        res = await play_ucl_match_logic(t1_id, t2_id, t1_name, t2_name, use_extra_time=True)
        
        if "error" in res: continue

        # --- ФОРМИРОВАНИЕ ОТЧЕТА В СТИЛЕ image_570a21_2.png ---
        
        # 1. Счёт и рамка
        score_display = f"{t1_name} {res['h_s']}:{res['a_s']} {t2_name}"
        pen_info = ""
        pen_visual_block = ""
        
        if res.get('is_penalties'):
            pen_info = f"\n 🎯 <b>Пен: ({res['pens_h']}:{res['pens_a']})</b>"
            h_vis = get_penalties_visual(res['pens_history']['h'])
            a_vis = get_penalties_visual(res['pens_history']['a'])
            pen_visual_block = (
                f"\n🥅 <b>Серия:</b>\n"
                f"🏠 : {h_vis}\n"
                f"🚀 : {a_vis}\n"
            )

        # 2. События (сортируем по минутам)
        events_list = sorted(res.get('events', []), key=lambda x: x[0])
        events_html = "\n".join([e[1] for e in events_list])

        # 3. MVP (Топ игроки)
        # Генерируем красивые медали и дробный рейтинг
        performers = get_match_performers(res['h_players'], res['a_players'], res['events'], t1_name, t2_name, res['winner_club'])
        mvp_text = " | ".join(performers)

        # СБОРКА ФИНАЛЬНОГО СООБЩЕНИЯ
        final_report = (
            f"🏆 <b>ЛИГА ЧЕМПИОНОВ: {current_stage}</b>\n"
            f"<code>┌───────────────────────┐</code>\n"
            f"  <b>{score_display}</b>"
            f"{pen_info}\n"
            f"<code>└───────────────────────┘</code>\n\n"
            f"📋 <b>Ключевые моменты:</b>\n"
            f"{events_html if events_html else '<i>— Безголевая засуха</i>'}\n"
            f"{pen_visual_block}\n"
            f"🌟 <b>Топ игроки:</b>\n"
            f"{mvp_text}\n"
            f"<code>———————————————————————</code>\n"
            f"🎉 <b>ПРОХОД В СЛЕДУЮЩИЙ РАУНД: {res['winner_club']}!</b>"
        )

        # Сохранение в БД
        conn = get_db(); c = conn.cursor()
        c.execute("UPDATE ucl_bracket SET res1 = ?, winner_id = ? WHERE id = ?", 
                  (f"{res['h_s']}:{res['a_s']}", res['winner_id'], m_id))
        
        # Обновление статистики игроков (UCL_STATS)
        for event in res.get('stats_to_save', []):
            update_ucl_stat(event['p_id'], event['u_id'], event['col'])
            
        conn.commit(); conn.close()

        # Рассылка
        for user_id in [t1_id, t2_id]:
            try: await cb.bot.send_message(user_id, final_report, parse_mode="HTML")
            except: pass
        
        await asyncio.sleep(1.5)

    await cb.message.answer(f"✅ Стадия {current_stage} полностью завершена!")

def get_match_performers(h_players, a_players, events, h_club, a_club, winner_club):
    stats = {}
    for p in h_players + a_players:
        name = p['player_name']
        # Базовая оценка 6.0 - 7.0
        stats[name] = {"goals": 0, "rating": random.uniform(6.0, 7.0), "club": h_club if p in h_players else a_club}

    for _, txt in events:
        for name in stats:
            if f"Гол! <b>{name}</b>" in txt:
                stats[name]["goals"] += 1
                stats[name]["rating"] += random.uniform(1.5, 2.0) # За гол оценка растет сильно
            elif f"пас: {name}" in txt:
                stats[name]["rating"] += random.uniform(0.5, 0.8)

    # Бонус игрокам победителя
    for name in stats:
        if stats[name]["club"] == winner_club:
            stats[name]["rating"] += random.uniform(0.3, 0.5)

    # Берем топ-3
    sorted_p = sorted(stats.items(), key=lambda x: x[1]['rating'], reverse=True)
    
    medals = ["🥇", "🥈", "🥉"]
    results = []
    for i in range(min(3, len(sorted_p))):
        name, d = sorted_p[i]
        results.append(f"{medals[i]} {name} ({round(d['rating'], 1)})")
        
    return results


@dp.callback_query(F.data == "admin_kick_user")
async def adm_kick(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("Введите ID игрока для удаления:"); await state.set_state(AdminStates.target_id)

@dp.message(AdminStates.target_id)
async def process_kick(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMINS: return
    uid = int(m.text); conn = get_db(); c = conn.cursor()
    c.execute('DELETE FROM users WHERE user_id = ?', (uid,))
    c.execute('DELETE FROM squad WHERE user_id = ?', (uid,))
    conn.commit(); conn.close(); await m.answer("✅ Удален"); await state.clear()

@dp.callback_query(F.data == "admin_next_stage")
async def next_stage_callback(cb: types.CallbackQuery):
    # Проверка на админа
    if cb.from_user.id not in ADMINS:
        return await cb.answer("У вас нет прав!", show_alert=True)

    # 1. Запускаем возвраты и уведомления (твоя функция)
    await process_loan_returns()
    
    # 2. Уменьшаем срок в базе данных
    conn = get_db()
    c = conn.cursor()
    # Уменьшаем срок только тем, у кого он больше 1, 
    # так как те, у кого был 1, уже обработаны функцией возврата
    c.execute('UPDATE squad SET loan_expires_window = loan_expires_window - 1 WHERE loan_expires_window > 1')
    conn.commit()
    conn.close()
    
    # Отвечаем пользователю (всплывающее окно или изменение текста)
    await cb.answer("✅ Сроки аренд обновлены!", show_alert=True)
    await cb.message.answer("🕒 Этап завершен: проверены возвраты, сроки остальных аренд уменьшены на 1.")

def get_random_club(all_clubs):
    # Просто выбирает случайный клуб из списка, который ты ей дашь
    return random.choice(all_clubs) if all_clubs else "Неизвестный клуб"

@dp.callback_query(F.data == "admin_finish_season")
async def callback_finish_season(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: 
        return await cb.answer("Доступ закрыт!", show_alert=True)

    conn = get_db(); c = conn.cursor()
    
    # Чтобы бот не "висел", сразу отвечаем Telegram
    await cb.answer("Запуск финальной церемонии...")

    try:
        # 1. Определяем чемпионов групп А и Б
        def get_winner(group):
            c.execute('''SELECT user_id, club FROM users 
                         WHERE league_group = ? 
                         ORDER BY (league_wins * 3 + league_draws) DESC, league_goals DESC LIMIT 1''', (group,))
            return c.fetchone()

        winner_a = get_winner("A")
        winner_b = get_winner("B")

        if not winner_a or not winner_b:
            return await cb.message.answer("❌ Ошибка: не удалось найти чемпионов в группах А и Б.")

        # --- СУМАСШЕДШИЙ СУПЕРФИНАЛ (LIVE-ТРАНСЛЯЦИЯ) ---
        match_msg = await cb.message.answer(
            f"🏆 <b>ДОБРО ПОЖАЛОВАТЬ НА ЗОЛОТОЙ МАТЧ СЕЗОНА!</b> 🏆\n\n"
            f"🅰️ Чемпион Сетки А: <b>{winner_a[1]}</b>\n"
            f"🅱️ Чемпион Сетки Б: <b>{winner_b[1]}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"⏳ <i>Команды выходят на поле...</i>", parse_mode="HTML"
        )
        await asyncio.sleep(4)

        events = [
            "⚽️ <b>15'</b> — Опасный момент у ворот Сетки Б! Мяч попадает в штангу!",
            "🔥 <b>45'</b> — Перерыв! Команды уходят в раздевалки при нулевом счете. Напряжение зашкаливает!",
            "⚡️ <b>70'</b> — ГООООООЛ... Нет! Офсайд! Болельщики в ярости!",
            "🔔 <b>90'</b> — Основное время закончено! Нас ждут экстра-таймы или пенальти!"
        ]

        for event in events:
            await match_msg.edit_text(f"{match_msg.text}\n\n{event}", parse_mode="HTML")
            await asyncio.sleep(3.5)

        # Решающий расчет матча
        res = await play_cup_match_full(winner_a[0], winner_b[0], winner_a[1], winner_b[1], cb.bot)
        
        # Определяем кто есть кто
        h_total = res["h_s"] + (res["h_p"] or 0)
        a_total = res["a_s"] + (res["a_p"] or 0)

        if h_total > a_total:
            abs_champ_id, abs_champ_name = winner_a[0], winner_a[1]
            finalist_id, finalist_name = winner_b[0], winner_b[1]
        else:
            abs_champ_id, abs_champ_name = winner_b[0], winner_b[1]
            finalist_id, finalist_name = winner_a[0], winner_a[1]

        # Итоговый счет в чат
        score_line = f"🏟 <b>ИТОГ МАТЧА: {res['h_s']}:{res['a_s']}</b>"
        if res['h_p'] is not None:
            score_line += f" (по пен. <b>{res['h_p']}:{res['a_p']}</b>)"
        
        await match_msg.edit_text(
            f"🎊 <b>ФИНАЛЬНЫЙ СВИСТОК!</b> 🎊\n\n"
            f"👑 Абсолютный чемпион: <b>{abs_champ_name}</b>\n"
            f"{score_line}\n\n"
            f"💰 Победитель получает: <b>150,000,000 €</b>\n"
            f"🥈 Финалист получает: <b>110,000,000 €</b>", parse_mode="HTML"
        )

        # --- ВЫПЛАТЫ ПРИЗОВЫХ ---
        # 1. Топ-2 (Финалисты)
        c.execute('UPDATE users SET balance = balance + 150000000 WHERE user_id = ?', (abs_champ_id,))
        c.execute('UPDATE users SET balance = balance + 110000000 WHERE user_id = ?', (finalist_id,))

        # 2. Остальные участники (по их местам в общей таблице, исключая финалистов)
        c.execute('''SELECT user_id, club FROM users 
                     WHERE user_id NOT IN (?, ?) 
                     AND (league_wins + league_draws + league_losses) > 0
                     ORDER BY (league_wins * 3 + league_draws) DESC, league_goals DESC''', (abs_champ_id, finalist_id))
        
        standings = c.fetchall()
        for index, (u_id, u_club) in enumerate(standings):
            rank = index + 3 # Т.к. 1 и 2 место — это финалисты
            if rank == 3: prize = 90_000_000
            elif rank <= 6: prize = 80_000_000
            elif rank <= 10: prize = 60_000_000
            else: prize = 40_000_000
            
            c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (prize, u_id))
            try:
                await cb.bot.send_message(u_id, f"🏁 Сезон окончен!\nВаше место: {rank}\nПризовые: {prize/1_000_000} млн €")
            except: pass

        # --- СБОР СТАТИСТИКИ (ФИКС NONE) ---
        def get_clean_stat(col):
            c.execute(f'SELECT MAX({col}) FROM league_stats')
            mv = c.fetchone()[0]
            if mv is None or mv == 0: return "—"
            c.execute(f'''SELECT s.player_name, u.club FROM league_stats ls
                          JOIN squad s ON ls.player_id = s.id
                          JOIN users u ON ls.user_id = u.user_id
                          WHERE ls.{col} = ? LIMIT 1''', (mv,))
            r = c.fetchone()
            return f"{r[0]} ({r[1]}) — {mv}" if r else "—"

        top_s = get_clean_stat("goals")
        top_a = get_clean_stat("assists")

        # --- ЗАВЕРШЕНИЕ (ОБНУЛЕНИЕ) ---
        c.execute('SELECT value FROM settings WHERE key = "current_season"')
        old_s = c.fetchone()[0]
        new_s = increment_season(old_s) # Убедись, что эта функция у тебя есть

        c.execute('UPDATE settings SET value = ? WHERE key = "current_season"', (new_s,))
        c.execute('UPDATE users SET league_wins=0, league_draws=0, league_losses=0, league_goals=0, league_group=NULL')
        c.execute('UPDATE squad SET goals=0, assists=0, yellow_cards=0, red_cards=0, is_banned=0, injury_remaining=0')
        c.execute('DELETE FROM league_schedule')
        c.execute('DELETE FROM cup_bracket')
        
        conn.commit()

        # Финальный отчет админу
        report = (
            f"🏁 <b>ИТОГИ СЕЗОНА {old_s}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"🏆 Чемпион: {abs_champ_name}\n"
            f"🥈 Финалист: {finalist_name}\n\n"
            f"⚽️ Бомбардир: {top_s}\n"
            f"🅰️ Ассистент: {top_a}\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 Новый сезон {new_s} открыт!"
        )
        await cb.message.answer(report, parse_mode="HTML")

    except Exception as e:
        print(f"ОШИБКА ФИНАЛА: {e}")
        await cb.message.answer(f"❌ Произошла ошибка: {e}")
    finally:
        conn.close()

@dp.callback_query(F.data == "admin_init_cup")
async def admin_init_cup(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: 
        return

    conn = get_db(); c = conn.cursor()
    
    # 1. ПРОВЕРКА: Если кубок уже запущен, блокируем повторную инициализацию
    c.execute('SELECT COUNT(*) FROM cup_bracket')
    if c.fetchone()[0] > 0:
        conn.close()
        return await cb.message.answer(
            "⚠️ <b>Кубок уже запущен!</b>\n"
            "Сетка существует. Чтобы сбросить её, нужно вручную очистить таблицу в БД, "
            "просто так «пересоздать» его нельзя, чтобы не потерять прогресс.", 
            parse_mode="HTML"
        )

    # 2. Получаем команды для кубка
    c.execute('SELECT user_id, club FROM users ORDER BY (wins*3 + draws) DESC')
    teams = c.fetchall()
    
    if len(teams) < 6:
        conn.close()
        return await cb.answer(f"Нужно минимум 20 команд! (У нас {len(teams)})", show_alert=True)

    # 3. Формируем Плей-ин (т.к. таблица гарантированно пуста после проверки выше)
    # 8 команд (с 13-го по 20-е место)
    pi_pool = teams[12:20] 
    for i in range(0, 8, 2):
        c.execute('''INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) 
                     VALUES ('Play-In', ?, ?, ?, ?)''', 
                  (pi_pool[i][0], pi_pool[i][1], pi_pool[i+1][0], pi_pool[i+1][1]))
    
    conn.commit(); conn.close()
    await cb.message.answer(
        "🏆 <b>Кубок успешно инициализирован!</b>\n"
        "Пары Плей-ин созданы. Теперь кнопка сброса заблокирована до конца турнира.", 
        parse_mode="HTML"
    )

async def play_cup_match_full(t1_id, t2_id, t1_name, t2_name, bot, prev_score=(0, 0), use_extra_time=True):
    conn = get_db(); c = conn.cursor()
   
    def get_team_context(uid):
        c.execute("SELECT formation, tactic, captain_id, penalty_id, freekick_id, club FROM users WHERE user_id = ?", (uid,))
        u = c.fetchone()
        
        # --- ФИКС ТУТ: Проверяем, что юзер вообще есть в таблице ---
        if u is None:
            u = ("4-4-3", "Обычная", None, None, None, "Unknown")

        # Основа (11 человек) — используем ключи 'name' для совместимости с твоим MVP блоком
        c.execute("SELECT id, player_name, pos, rating FROM squad WHERE user_id = ? AND slot_id IS NOT NULL AND injury_remaining = 0 AND is_banned = 0", (uid,))
        sq = [{"id": r[0], "name": r[1], "pos": r[2], "rat": r[3]} for r in c.fetchall()]
        
        # Запас (для автозамен)
        c.execute("SELECT id, player_name, pos, rating FROM squad WHERE user_id = ? AND slot_id IS NULL AND injury_remaining = 0 AND is_banned = 0", (uid,))
        bench = [{"id": r[0], "name": r[1], "pos": r[2], "rat": r[3]} for r in c.fetchall()]
        
        avg_rat = sum(p['rat'] for p in sq) / 11 if len(sq) >= 11 else 40.0
        
        return {"sq": sq, "bench": bench, "rat": avg_rat, "form": u[0], "tac": u[1], "cap": u[2], "pen": u[3], "fk": u[4], "club": u[5]}

    tm1 = get_team_context(t1_id); tm2 = get_team_context(t2_id)
    res = {"h_s": 0, "a_s": 0, "h_p": None, "a_p": None, "events": [], "pen_report": [], "played_ids": {}}

    # Инициализируем иммунитет для старта (1.0 — начали в основе)
    for p in tm1['sq']: res['played_ids'][p['id']] = 1.0
    for p in tm2['sq']: res['played_ids'][p['id']] = 1.0

    f1_mod = FORMATION_MODS.get(tm1['form'], {"atk": 1.0, "def": 1.0})
    f2_mod = FORMATION_MODS.get(tm2['form'], {"atk": 1.0, "def": 1.0})

    current_limit = 90
    minute = 1
   
    while minute <= current_limit:
        if minute == 90 and use_extra_time:
            if (res["h_s"] + prev_score[0]) == (res["a_s"] + prev_score[1]):
                current_limit = 120
                res["events"].append(f"⏳ {minute}' <b>Дополнительное время!</b>")

        h_mod = (len(tm1['sq']) / 11) ** 2
        a_mod = (len(tm2['sq']) / 11) ** 2

        if minute % 4 == 0:
            roll = random.random()
            
            # Считаем разницу сил (экспоненциально, чтобы топ-клубы давили сильнее)
            # Если у Баварии 85, а у Сандерленда 70, разница будет ощутимой
            power_tm1 = (tm1['rat'] ** 1.5) * h_mod
            power_tm2 = (tm2['rat'] ** 1.5) * a_mod
            total_power = power_tm1 + power_tm2
            
            chance_tm1 = power_tm1 / total_power if total_power > 0 else 0.5

            if random.random() < chance_tm1:
                atk, dfs, is_home, a_n = tm1, tm2, True, t1_name
                a_m, d_m = f1_mod['atk'], f2_mod['def']
            else:
                atk, dfs, is_home, a_n = tm2, tm1, False, t2_name
                a_m, d_m = f2_mod['atk'], f1_mod['def']

            # 1. ТРАВМА И АВТОЗАМЕНА (5%)
            if roll < 0.05:
                if not atk['sq']: continue
                
                p_off = random.choice(atk['sq'])
                dur = random.randint(2, 4)
                
                inj_messages = [
                    f"🚑 {minute}' <b>{p_off['name']}</b> неудачно приземлился и просит замену!",
                    f"🚑 {minute}' Ой-ой! <b>{p_off['name']}</b> держится за заднюю поверхность бедра. Это финиш.",
                    f"🚑 {minute}' Жесткий стык! <b>{p_off['name']}</b> покидает поле на носилках под аплодисменты.",
                    f"🚑 {minute}' <b>{p_off['name']}</b> захромал. Похоже, сегодня он больше не помощник."
                ]
                res["events"].append(random.choice(inj_messages))
                
                sub = next((b for b in atk['bench'] if b['pos'] == p_off['pos']), None)
                if not sub and atk['bench']: sub = atk['bench'][0]
                
                if sub:
                    res["events"].append(f"🔄 Срочная замена: {sub['name']} ⬆️ вместо пострадавшего {p_off['name']} ⬇️")
                    res['played_ids'][sub['id']] = 0.5
                    atk['sq'].remove(p_off); atk['sq'].append(sub); atk['bench'].remove(sub)
                else:
                    res["events"].append(f"⚠️ У {atk['club']} кончились люди! Доигрывают в меньшинстве.")
                    atk['sq'].remove(p_off)
                
                c.execute("UPDATE squad SET injury_remaining = ?, slot_id = NULL, status = 'bench' WHERE id = ?", (dur, p_off['id']))

            # 2. УДАЛЕНИЕ (3%)
            elif roll < 0.08:
                if not atk['sq']: continue
                p_red = random.choice(atk['sq'])
                
                red_messages = [
                    f"🟥 {minute}' <b>ГРУБЕЙШИЙ ФОЛ!</b> <b>{p_red['name']}</b> видит перед собой красный свет!",
                    f"🟥 {minute}' Прямая красная! <b>{p_red['name']}</b> чуть не оторвал ноги сопернику!",
                    f"🟥 {minute}' Судья неумолим! <b>{p_red['name']}</b> отправляется в раздевалку раньше времени.",
                    f"🟥 {minute}' <b>{p_red['name']}</b> психанул и толкнул арбитра. Это удаление, без вариантов!"
                ]
                res["events"].append(random.choice(red_messages))
                atk['sq'].remove(p_red)
                c.execute("UPDATE squad SET is_banned = 2, slot_id = NULL, status = 'bench' WHERE id = ?", (p_red['id'],))

            # 3. ДИНАМИЧЕСКИЕ ЗАМЕНЫ (Шанс 15%)
            elif roll < 0.23:
                side = atk
                if side["bench"] and len(side["sq"]) > 0:
                    out_pool = [p for p in side["sq"] if p['pos'] != 'GK' and res['played_ids'].get(p['id'], 0) == 1.0]
                    if not out_pool: out_pool = [p for p in side["sq"] if p['pos'] != 'GK']
                    
                    p_out = random.choice(out_pool)
                    sub = next((b for b in side["bench"] if b['pos'] == p_out['pos']), side["bench"][0])
                    
                    tactical_messages = [
                        f"🔄 Тренер {side['club']} решил освежить игру: <b>{sub['name']}</b> в деле!",
                        f"🔄 <b>{p_out['name']}</b> сегодня не попал в ритм. Вместо него выходит <b>{sub['name']}</b>.",
                        f"🔄 Тактический ход! <b>{p_out['name']}</b> ⬇️ уступает место <b>{sub['name']}</b> ⬆️",
                        f"🔄 На поле появляется <b>{sub['name']}</b>. Посмотрим, усилит ли он атаку {side['club']}."
                    ]
                    res["events"].append(random.choice(tactical_messages))
                    
                    res['played_ids'][p_out['id']] = 0.5
                    res['played_ids'][sub['id']] = 0.5 
                    side["sq"].remove(p_out); side["sq"].append(sub); side["bench"].remove(sub)

            elif roll < 0.35:
                if atk['sq']:
                    scorer = random.choice([p for p in atk['sq'] if p['pos'] != 'GK'])
                    miss_reasons = [
                        f"пробил выше ворот из убойной позиции!",
                        f"попал в ШТАНГУ! Весь стадион ахнул!",
                        f"закрутил мяч, но тот пролетел в сантиметре от крестовины.",
                        f"поскользнулся в момент удара! Мяч улетел на трибуны.",
                        f"обыграл двоих, но пробил прямо в защитника.",
                        f"вышел 1 на 1, но замешкался и упустил мяч."
                    ]
                    res["events"].append(f"🔥 {minute}' Момент у {atk['club']}! {scorer['name']} {random.choice(miss_reasons)}")

            # 4. ШТРАФНОЙ / ПЕНАЛЬТИ (Улучшенный шанс от рейтинга)
            elif roll < 0.50:
                is_pen = random.random() < 0.25
                exec_p = next((p for p in atk['sq'] if p['id'] == (atk['pen'] if is_pen else atk['fk'])), None)
                if not exec_p: exec_p = random.choice(atk['sq'])

                # Шанс гола теперь сильнее зависит от рейтинга бьющего против рейтинга защиты
                goal_prob = 0.75 if is_pen else 0.3
                if random.random() < (goal_prob * (exec_p['rat'] / dfs['rat'])):
                    if is_home: res["h_s"] += 1
                    else: res["a_s"] += 1
                    icon = "🎯 ПЕНАЛЬТИ!" if is_pen else "☄️ ШТРАФНОЙ!"
                    res["events"].append(f"⚽ {minute}' <b>{icon} ГОЛ!</b> {exec_p['name']} ювелирно исполнил в угол!")
                    c.execute("UPDATE squad SET goals = goals + 1 WHERE id = ?", (exec_p['id'],))
                else:
                    save_desc = ["вытащил мяч из девятки!", "намертво зафиксировал мяч.", "отбил перед собой, но защитники подстраховали."]
                    res["events"].append(f"🧤 {minute}' Сэйвище! Вратарь {dfs['club']} {random.choice(save_desc)}")

            # 5. ОБЫЧНЫЙ ГОЛ (С весами позиций)
            elif roll < 0.60:
                # Влияние тактики и рейтинга
                base_chance = 0.15 
                rating_diff = (atk['rat'] / dfs['rat']) ** 2 # Квадратичная зависимость для доминирования
                chance = base_chance * rating_diff * a_m * (h_mod if is_home else a_mod)
                
                if random.random() < chance:
                    # Используем твой get_weighted_scorer для выбора автора
                    scorer = get_weighted_scorer([p for p in atk['sq']])
                    if is_home: res["h_s"] += 1
                    else: res["a_s"] += 1
                    
                    goal_styles = [
                        "замкнул прострел с фланга!",
                        "прошил вратаря мощным ударом издали!",
                        "технично перебросил вышедшего голкипера!",
                        "заколачивает мяч головой после подачи углового!"
                    ]
                    res["events"].append(f"⚽ {minute}' <b>ГОООЛ!</b> {scorer['name']} {random.choice(goal_styles)}")
                    c.execute("UPDATE squad SET goals = goals + 1 WHERE id = ?", (scorer['id'],))

        minute += 1

    # --- СЕРИЯ ПЕНАЛЬТИ И MVP (Твой оригинальный код) ---
    total_h = res["h_s"] + prev_score[0]
    total_a = res["a_s"] + prev_score[1]

    if total_h == total_a and use_extra_time:
        res["events"].append("🎯 <b>СЕРИЯ ПЕНАЛЬТИ!</b>")
        res["h_p"], res["a_p"] = 0, 0
        h_pool = tm1['sq'][:]; a_pool = tm2['sq'][:]
        
        def sort_kickers(pool, main_pen_id):
            main_p = next((p for p in pool if p['id'] == main_pen_id), None)
            others = sorted([p for p in pool if p != main_p], key=lambda x: x['rat'], reverse=True)
            return ([main_p] if main_p else []) + others

        h_kickers = sort_kickers(h_pool, tm1['pen'])
        a_kickers = sort_kickers(a_pool, tm2['pen'])
        
        if h_kickers and a_kickers:
            h_line, a_line, round_idx = "", "", 0
            while True:
                p_h = h_kickers[round_idx % len(h_kickers)]
                ok_h = random.random() < (0.80 * (p_h['rat'] / tm2['rat']))
                if ok_h: res["h_p"] += 1
                h_line += "✅" if ok_h else "❌"
                
                p_a = a_kickers[round_idx % len(a_kickers)]
                ok_a = random.random() < (0.80 * (p_a['rat'] / tm1['rat']))
                if ok_a: res["a_p"] += 1
                a_line += "✅" if ok_a else "❌"

                if round_idx >= 4:
                    if res["h_p"] != res["a_p"]: break
                else:
                    rem = 4 - round_idx
                    if res["h_p"] > res["a_p"] + rem or res["a_p"] > res["h_p"] + rem: break
                round_idx += 1
            res["pen_report_text"] = f"🏠: {h_line}\n🚀: {a_line}"

    # MVP БЛОК (Твой оригинальный)
    scorers = [e.split('<b>')[1].split('</b>')[0] for e in res["events"] if "⚽" in e]
    
    # 2. Объединяем игроков обеих команд
    potential_mvps = tm1['sq'] + tm2['sq']
    
    # Если на поле никого не было (мало ли), возвращаем заглушку
    if not potential_mvps:
        res["mvp_text"] = "⭐ MVP не определен"
        conn.commit(); conn.close()
        return res

    # 3. Сортируем: сначала те, кто забивал, затем по рейтингу
    sorted_players = sorted(potential_mvps, key=lambda x: (x['name'] in scorers, x['rat']), reverse=True)
    mvp_list = sorted_players[:3]
    
    # Генерируем оценки
    ratings = [round(random.uniform(8.5, 9.9), 1), round(random.uniform(8.0, 8.4), 1), round(random.uniform(7.4, 8.2), 1)]
    
    # Собираем строку (проверяем наличие 2-го и 3-го места на случай коротких составов)
    mvp_parts = [f"⭐ {mvp_list[0]['name']} (<b>{ratings[0]}</b>)"]
    if len(mvp_list) > 1: mvp_parts.append(f"🥈 {mvp_list[1]['name']} (<b>{ratings[1]}</b>)")
    if len(mvp_list) > 2: mvp_parts.append(f"🥉 {mvp_list[2]['name']} (<b>{ratings[2]}</b>)")
    
    res["mvp_text"] = " | ".join(mvp_parts)

    # 4. ФИКС UPDATE: Начисляем MVP именно ИГРОКУ в таблицу squad по его ID
    try:
        # Увеличиваем счетчик MVP самому крутому игроку матча
        c.execute("UPDATE squad SET mvp_stats = mvp_stats + 1 WHERE id = ?", (mvp_list[0]['id'],))
    except Exception as e:
        print(f"Ошибка при обновлении MVP: {e}")

    conn.commit(); conn.close()
    return res

@dp.message(Command("finish_season"))
async def finish_season(m: types.Message):
    if m.from_user.id not in ADMINS: 
        return await m.answer("У вас нет прав для завершения сезона.")

    conn = get_db(); c = conn.cursor()

    c.execute('''
        SELECT user_id, club, (wins * 3 + draws) as pts, goals_scored 
        FROM users 
        WHERE (wins + draws + losses) > 0
        ORDER BY pts DESC, goals_scored DESC 
        LIMIT 1
    ''')
    winner_league = c.fetchone()

    if not winner_league:
        conn.close()
        return await m.answer("❌ Невозможно завершить сезон: в Лиге не сыграно ни одного матча.")
    
    w_id, w_club, w_pts, w_gs = winner_league


    c.execute("SELECT id, t1_id, t2_id, t1_name, t2_name FROM cup_bracket WHERE stage = 'Final'")
    f = c.fetchone()
    
    cup_report = ""
    if f:
        match_id, t1_id, t2_id, t1_n, t2_n = f
        
        res = await play_cup_match_full(t1_id, t2_id, t1_n, t2_n, m.bot)
        
        # Победитель кубка
        h_total = res["h_s"] + (res["h_p"] or 0)
        a_total = res["a_s"] + (res["a_p"] or 0)
        
        cup_winner_id = t1_id if h_total > a_total else t2_id
        cup_winner_name = t1_n if h_total > a_total else t2_n

        c.execute("UPDATE users SET balance = balance + 20000000 WHERE user_id = ?", (cup_winner_id,))
        
        c.execute("UPDATE cup_bracket SET winner_id = ?, h_score = ?, a_score = ?, h_pen = ?, a_pen = ? WHERE id = ?", 
                  (cup_winner_id, res["h_s"], res["a_s"], res["h_p"], res["a_p"], match_id))

        cup_report = (
            f"\n\n🏆 <b>ФИНАЛ КУБКА ЗАВЕРШЕН!</b>\n"
            f"🏟 {t1_n} {res['h_s']}:{res['a_s']} {t2_n}\n"
            f"🥇 Победитель Кубка: <b>{cup_winner_name}</b> (+20,000,000 €)\n"
        )
        if res['h_p'] is not None:
            cup_report = cup_report.replace("🏟", f"🎯 Пенальти: {res['h_p']}:{res['a_p']}\n🏟")

        try:
            await m.bot.send_message(cup_winner_id, "🏆 <b>ТЫ ЛУЧШИЙ!</b>\nТвой клуб выиграл Кубок! 20,000,000 € на счету!")
        except: pass

    league_prize = 50_000_000
    c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (league_prize, w_id))
    
    
    c.execute('UPDATE users SET wins=0, draws=0, losses=0, goals_scored=0')
    c.execute('UPDATE squad SET goals=0, assists=0, yellow_cards=0, red_cards=0, is_banned=0, injury_remaining=0')
    
    
    c.execute('DELETE FROM league_schedule')
    c.execute('DELETE FROM cup_bracket')

    conn.commit()
    conn.close()

    final_text = (
        f"🎊 СЕЗОН ОФИЦИАЛЬНО ЗАВЕРШЕН! 🎊\n"
        f"————————————————————\n"
        f"🥇 Чемпион Лиги: {w_club}\n"
        f"📊 Очки: {w_pts} | Награда: <b>50,000,000 €</b>\n"
        f"{cup_report}"
        f"————————————————————\n"
        f"🚀 Вся статистика обнулена. Ждем вас в новом сезоне!"
    )
    
    await m.answer(final_text, parse_mode="HTML")
    
    try:
        await m.bot.send_message(w_id, f"🏆 <b>ПОЗДРАВЛЯЕМ!</b>\nВаш клуб {w_club} выиграл Лигу! 50,000,000 € зачислены!")
    except: pass

def get_random_clubs(all_clubs):
    """Перемешивает список клубов и отдает по одному. 
    Если клубы кончились, перемешивает снова."""
    pool = list(all_clubs)
    random.shuffle(pool)
    while True:
        for club in pool:
            yield club
        random.shuffle(pool)

# Хендлер для вызова из админки
@dp.callback_query(F.data == "admin_post_news")
async def admin_post_news(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: 
        return await cb.answer("Ты не редактор газеты! 🚫")
    
    try:
        text = await generate_daily_news()
        # Отправляем в чат (убедись, что CHAT_ID с -100)
        await bot.send_message(CHAT_ID, text, parse_mode="HTML")
        await cb.answer("📰 Газета успешно опубликована!")
    except Exception as e:
        print(f"ОШИБКА ГАЗЕТЫ: {e}")
        await cb.answer("Ошибка при печати тиража!")

async def generate_daily_news():
    conn = get_db(); c = conn.cursor()

    # 1. Сбор всех доступных клубов для рандома
    c.execute('SELECT DISTINCT club FROM users WHERE club IS NOT NULL')
    db_clubs = [row[0] for row in c.fetchall()]
    all_clubs = list(set(db_clubs + list(CLUBS.keys())))
    
    # Создаем генератор: он будет перемешивать клубы и выдавать по одному
    def get_club_gen():
        pool = all_clubs[:]
        random.shuffle(pool)
        while True:
            for club in pool:
                yield club
            random.shuffle(pool) # Перемешиваем заново, когда список кончился

    club_gen = get_club_gen()
    def rc(): return next(club_gen) # Функция-помощник для вставки в текст

    # 2. СБОР ДАННЫХ ИЗ БД (Твои запросы без изменений)
    c.execute('SELECT s.player_name, s.goals, u.club FROM squad s JOIN users u ON s.user_id = u.user_id WHERE s.goals > 0 ORDER BY s.goals DESC LIMIT 5')
    scorers = c.fetchall()
    
    c.execute('SELECT s.player_name, s.assists, u.club FROM squad s JOIN users u ON s.user_id = u.user_id WHERE s.assists > 0 ORDER BY s.assists DESC LIMIT 5')
    assisters = c.fetchall()
    
    c.execute('SELECT s.player_name, s.yellow_cards, s.red_cards, u.club FROM squad s JOIN users u ON s.user_id = u.user_id WHERE (s.yellow_cards > 0 OR s.red_cards > 0) ORDER BY (s.red_cards * 3 + s.yellow_cards) DESC LIMIT 5')
    bad_boys = c.fetchall()
    
    c.execute('SELECT club, losses FROM users WHERE losses > 0 ORDER BY losses DESC LIMIT 5')
    losers = c.fetchall()
    
    c.execute('SELECT player_name, price, buyer_club FROM transfer_history ORDER BY id DESC LIMIT 5')
    deals = c.fetchall()
    conn.close()

    # Твои лозунги
    slogan = random.choice([
        "🗞 <b>Твой инсайд в мире голов.</b>",
        "🗞 <b>Не читал — считай, пропустил пенальти!</b>",
        "🗞 <b>Твой клуб. Твоя лига. Твоя история.</b>"
    ])

    mandatory_blocks = [] 
    random_pool = []

    # --- ТВОИ ЦИТАТЫ ШНЯКИНА (Теперь с авто-клубами) ---
    expert_quote = random.choice([
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «Главное в матче <b>«{rc()}»</b> — чтобы не выключили свет на стадионе. Остальное — нюансы!»",
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «Я внимательно изучил <b>«{rc()}»</b>. Мой вердикт: если они забьют больше соперника, то точно не проиграют. Скриньте!» 📈",
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «В <b>«{rc()}»</b> сейчас такая атмосфера, что даже мяч не хочет залетать в ворота. Я бы поставил на ничью». 📉",
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «Видел я тренировку <b>«{rc()}»</b>... Там нападающий попал по мячу с первого раза. Это знак!» 🔮",
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «Игрокам <b>«{rc()}»</b> нужно просто выйти на поле и сыграть в футбол. Если они выйдут играть в домино — шансов меньше». ✍️",
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «По моим данным, <b>«{rc()}»</b> сегодня выберет тактику 'бей-беги'. Гениально!» 🧠"
    ])
    
    if scorers:
        p = random.choice(scorers)
        mandatory_blocks.append(f"『 <b>OFFENSIVE PROTOCOL</b> 』\n<b>{p[0].upper()}</b> [{p[2].upper()}] — деструкция обороны завершена. Зафиксирован {p[1]}-й запуск мяча в сетку. ☄️")
    else:
        mandatory_blocks.append(f"『 <b>STRIKE STATUS</b> 』\nСистема не обнаружила результативных действий. Нападающие в режиме ожидания. 🔍")

    if assisters:
        a = random.choice(assisters)
        mandatory_blocks.append(f"『 <b>NEURAL LINK</b> 』\n<b>{a[0].upper()}</b> — идеальная передача данных. {a[1]}-й ассист заблокировал логику защиты. 🎯")

    if deals:
        d = random.choice(deals)
        mandatory_blocks.append(f"『 <b>MARKET UPDATE</b> 』\nОбъект <b>{d[0].upper()}</b> переведен в сектор <b>{d[2].upper()}</b>. Транзакция: {d[1]}M €. 💸")
    else:
        mandatory_blocks.append(f"『 <b>MARKET STANDBY</b> 』\nФинансовые потоки стабильны. Трансферная активность в «{rc()}» не обнаружена. 🧊")

    if bad_boys:
        b = random.choice(bad_boys)
        mandatory_blocks.append(f"『 <b>SYSTEM ERROR</b> 』\nИгрок <b>{b[0].upper()}</b> нарушил протокол дисциплины. Уровень угрозы: RED. 🟥")

    if losers:
        l = random.choice(losers)
        mandatory_blocks.append(f"『 <b>CRITICAL FAILURE</b> 』\nСбой в системе клуба <b>{l[0].upper()}</b>. {l[1]}-е поражение подряд. Требуется перезагрузка. 📉")

    random_pool.extend([
        f"🏟 <b>АНШЛАГ!</b> Стадионы забиты, а фанаты поют громче сирен! 📣",
        f"🌭 <b>НОВОСТИ КЕЙТЕРИНГА:</b> Сосиски на стадионе <b>«{rc()}»</b> признаны лучшими в лиге. 🌭",
        f"🏟 <b>СКАНДАЛ:</b> Фанаты клуба <b>«{rc()}»</b> устроили невероятный перфоманс. 🔥",
        f"🎤 <b>ИНСАЙД:</b> Тренер <b>«{rc()}»</b> в ярости. Говорят, игроки расслабились перед выездом к <b>«{rc()}»</b>. 🤬",
        f"🚑 <b>МЕДИЦИНА:</b> Врачи <b>«{rc()}»</b> творят чудеса! Лидеры восстановились. 💊",
        f"⭐ <b>НОВАЯ ЗВЕЗДА:</b> В молодежке <b>«{rc()}»</b> подрастает новый Мбаппе. 📈",
        f"🤝 <b>ТОВАРИЩЕСКИЙ УЖИН:</b> Владельцы <b>«{rc()}»</b> и <b>«{rc()}»</b> были замечены в ресторане. 🤔",
        f"🍺 <b>ПИВНОЙ СКАНДАЛ:</b> На стадионе <b>«{rc()}»</b> фанаты выпили годовой запас пенного! 🍺",
        f"🏠 <b>ЖИЛИЩНЫЙ ВОПРОС:</b> Клуб <b>«{rc()}»</b> выставил на трансфер вратаря за долги по ипотеке! 💸",
        f"🐐 <b>АГРО-НОВОСТИ:</b> На поле <b>«{rc()}»</b> ночью паслись козы. Пасуются лучше защиты! 🐐",
        f"🕺 <b>ДИСКО-БОЛ:</b> Игроков <b>«{rc()}»</b> заметили в ночном клубе. 💃",
        f"🕶 <b>ЗРЕНИЕ:</b> Фанаты <b>«{rc()}»</b> скинулись судье на операцию. Доброта! 👓",
        f"🧦 <b>ПРОКЛЯТЫЕ ГЕТРЫ:</b> Клуб <b>«{rc()}»</b> вышел на поле в разных носках на удачу. 🩹",
        f"🍕 <b>ДИЕТА:</b> Тренер <b>«{rc()}»</b> застукал нападающих в бургерной. 🍔",
        f"🚜 <b>АГРО-ФИТНЕС:</b> На базе <b>«{rc()}»</b> игроки дубля стригли траву ножницами. ✂️",
        f"📢 <b>ГОРЛОПАНЫ:</b> Фанаты <b>«{rc()}»</b> пели так громко, что рухнул забор! 🏗",
        f"👓 <b>АКЦИЯ:</b> Клуб <b>«{rc()}»</b> подарил судье собаку-поводыря. 🐕",
        f"🧙‍♂️ <b>МАГИЯ:</b> Шаман <b>«{rc()}»</b> побрызгал штанги святой водой. ✨",
        f"🚌 <b>АВТОБУС:</b> Тактика «10 защитников» от <b>«{rc()}»</b> признана самой скучной! 😴",
        f"🗿 <b>ДЗЕН:</b> Тренер <b>«{rc()}»</b> заставил игроков медитировать на штангу. 🧘‍♂️",
        f"📦 <b>VAR:</b> В клубе <b>«{rc()}»</b> повторы смотрят на телефоне охранника. 📱",
        f"🦖 <b>ДИНОЗАВР:</b> Фанаты <b>«{rc()}»</b> вывели на поле надувного тираннозавра. 😱",
        f"🧺 <b>СТИРКА:</b> В <b>«{rc()}»</b> форму стирают в фонтане. Запах сбивает врагов! 🧼",
        f"🐈 <b>ЧЕРНЫЙ СПИСОК:</b> Автобус <b>«{rc()}»</b> ездит кругами, чтобы не встретить кота! 🚌"
    ])

    # Выбираем 2 случайных из пула
    random.shuffle(random_pool)
    selected_random = random_pool[:3] # Строго 3 штуки

    # ФИНАЛЬНАЯ СБОРКА (Складываем списки напрямую)
    final_blocks = mandatory_blocks + selected_random
    
    # Твой обязательный шаффл, чтобы база и филлеры перемешались
    random.shuffle(final_blocks)

    try:
        date_str = datetime.now().strftime("%d.%m.%Y")
    except AttributeError:
        import datetime as dt_module
        date_str = dt_module.datetime.now().strftime("%d.%m.%Y")

    header = random.choice(["🗞 <b>FOOTBALL DAILY</b>", "⚽️ <b>ВЕСТНИК ЛИГИ</b>", "🏟 <b>СТАДИОННЫЙ КУРЬЕР</b>"])
    date_str = datetime.now().strftime("%d.%m.%Y")
    
    final_report = f"{header}\n<i>Выпуск от {date_str}</i>\n" + ("—" * 20) + "\n\n"
    final_report += "\n\n".join(final_blocks)
    final_report += f"\n\n————————————————\n{expert_quote}"
    final_report += f"\n\n————————————————\n{slogan}"

    return final_report

@dp.callback_query(F.data == "admin_give_player")
async def adm_give(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("Формат: ID Имя Рейтинг Позиция"); await state.set_state(AdminStates.player_data)

@dp.message(AdminStates.player_data)
async def process_give(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMINS: return
    try:
        d = m.text.split(); conn = get_db(); c = conn.cursor()
        c.execute('INSERT INTO squad (user_id, player_name, rating, pos) VALUES (?, ?, ?, ?)', (int(d[0]), d[1], int(d[2]), d[3].upper()))
        conn.commit(); conn.close(); await m.answer("✅ Выдан"); await state.clear()
    except: await m.answer("Ошибка формата")

@dp.callback_query(F.data == "admin_give_money")
async def admin_list_users(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    # Берем баланс как есть из таблицы users
    c.execute('SELECT user_id, username, balance FROM users')
    users = c.fetchall(); conn.close()
    
    builder = InlineKeyboardBuilder()
    for uid, name, bal in users:
        label = name if name else f"ID: {uid}"
        
        # ИСПРАВЛЕНО: Делим реальный баланс на 1,000,000 для отображения в "M" (миллионах)
        display_bal = bal / 1_000_000 
        
        # Теперь в кнопке будет "Имя (150.0M)" вместо "Имя (150000000M)"
        builder.button(text=f"{label} ({display_bal:.1f}M)", callback_data=f"give_money_to_{uid}")
    
    builder.adjust(1)
    builder.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_main"))
    await cb.message.edit_text("💰 <b>Кому начислить деньги?</b>", reply_markup=builder.as_markup(), parse_mode="HTML")

# Шаг 2: Запрос суммы
@dp.callback_query(F.data.startswith("give_money_to_"))
async def ask_amount(cb: types.CallbackQuery, state: FSMContext):
    target_id = cb.data.replace("give_money_to_", "")
    await state.update_data(target_uid=target_id)
    await cb.message.answer(f"🔢 Введите сумму (число) для {target_id}:")
    # Указываем состояние через класс
    await state.set_state(AdminMoney.waiting_for_amount)

# Шаг 3: Применение (исправлен фильтр состояния)
@dp.message(AdminMoney.waiting_for_amount) # Вот так правильно в aiogram 3
async def apply_money(m: types.Message, state: FSMContext):
    # Проверка на число
    clean_text = m.text.replace('-', '').strip()
    if not clean_text.isdigit():
        return await m.answer("❌ Введите корректное число!")
    
    amount_mln = int(m.text)
    # Переводим в реальные деньги (если в базе хранишь полные суммы, а вводишь в млн)
    # Если в базе хранишь просто "120", то оставляй как есть.
    # Но обычно баланс = вводимое число * 1_000_000
    real_amount = amount_mln * 1_000_000 

    data = await state.get_data()
    target_id = data.get('target_uid')
    
    if not target_id: 
        await m.answer("⚠️ Ошибка: цель не найдена.")
        return await state.clear()

    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (real_amount, target_id))
    conn.commit(); conn.close()
    
    await m.answer(f"✅ Пользователю {target_id} начислено {amount_mln}M €.")
    await state.clear()
# 2. Снятие пользователя с клуба (обнуление состава без удаления юзера)
@dp.callback_query(F.data == "admin_kick_club")
async def pre_kick_club(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("Введите ID пользователя, которого нужно исключить из клуба:")
    await state.set_state("waiting_for_kick_id")

@dp.callback_query(F.data == "admin_league_run_tour")
async def run_league_tour(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    
    # 1. Узнаем текущий тур лиги
    c.execute('SELECT MIN(tour_number) FROM league_schedule WHERE status = "pending"')
    row = c.fetchone()
    current_tour = row[0] if row else 99
    
    # 3. Аналогично для 1/8 (приоритет 5.5 — перед 6 туром)
    if current_tour >= 6:
        c.execute('SELECT COUNT(*) FROM cup_bracket WHERE stage = "1/8" AND winner_id IS NULL')
        if c.fetchone()[0] > 0:
            conn.close()
            return await cb.message.answer("⚠️ Сначала завершите 1/8 Кубка!")

    # Дальше идет твой обычный код выбора матчей лиги...
    c.execute('''
        SELECT s.id, s.home_id, s.away_id, u1.club, u2.club, u1.formation, u2.formation
        FROM league_schedule s
        JOIN users u1 ON s.home_id = u1.user_id
        JOIN users u2 ON s.away_id = u2.user_id
        WHERE s.status = "pending" AND s.tour_number = ?
    ''', (current_tour,))
    matches_to_run = c.fetchall()
    
    final_report = "🏟 <b>РЕЗУЛЬТАТЫ ТУРА ЛИГИ</b>\n\n"

    for m_id, h_id, a_id, h_club, a_club, h_form, a_form in matches_to_run:
        # АВТО-КИК забаненных и травмированных (ТВОЙ КОД)
        for uid in [h_id, a_id]:
            c.execute('''UPDATE squad SET slot_id = NULL, status = "bench" 
                         WHERE user_id = ? AND slot_id IS NOT NULL 
                         AND (is_banned > 0 OR injury_remaining > 0)''', (uid,))
        conn.commit()

        # 1. СЧИТАЕМ ЛЕГИТИМНЫХ ИГРОКОВ (ТВОЙ КОД)
        c.execute('SELECT COUNT(*) FROM squad WHERE user_id = ? AND slot_id IS NOT NULL AND is_banned = 0 AND injury_remaining = 0', (h_id,))
        h_count = c.fetchone()[0]
        c.execute('SELECT COUNT(*) FROM squad WHERE user_id = ? AND slot_id IS NOT NULL AND is_banned = 0 AND injury_remaining = 0', (a_id,))
        a_count = c.fetchone()[0]

        # 2. ЖЕСТКИЙ ТЕХНАРЬ (ТВОЙ КОД ИСПРАВЛЕННЫЙ)
        if h_count < 11 or a_count < 11:
            if h_count < 11 and a_count < 11:
                reason, h_res, a_res = "Обе команды не набрали состав", 0, 0
                c.execute('UPDATE users SET league_losses=league_losses+1 WHERE user_id IN (?,?)', (h_id, a_id))
            elif h_count < 11:
                reason, h_res, a_res = f"Некомплект у {h_club} ({h_count}/11)", 0, 3
                c.execute('UPDATE users SET league_wins=league_wins+1, league_goals=league_goals+3 WHERE user_id=?', (a_id,))
                c.execute('UPDATE users SET league_losses=league_losses+1 WHERE user_id=?', (h_id,))
            else:
                reason, h_res, a_res = f"Некомплект у {a_club} ({a_count}/11)", 3, 0
                c.execute('UPDATE users SET league_wins=league_wins+1, league_goals=league_goals+3 WHERE user_id=?', (h_id,))
                c.execute('UPDATE users SET league_losses=league_losses+1 WHERE user_id=?', (a_id,))

            c.execute('UPDATE league_schedule SET status = "finished" WHERE id = ?', (m_id,))
            conn.commit()

            tech_msg = (f"🏟 <b>ТЕХНИЧЕСКИЙ РЕЗУЛЬТАТ</b>\n\n"
                        f"⚔️ <b>{h_club}</b> {h_res}:{a_res} <b>{a_club}</b>\n"
                        f"————————————————————\n"
                        f"❌ {reason}")
            
            for user_id in [h_id, a_id]:
                try: await cb.bot.send_message(user_id, tech_msg, parse_mode="HTML")
                except: pass

            final_report += (f"<b>{h_club}</b> 🆚 <b>{a_club}</b>\n"
                             f"      ⚽️  <b>{h_res} : {a_res}</b>\n"
                             f"❌ <b>Техническое поражение!</b>\n"
                             f"ℹ️ {reason}\n🏁 ————————————————————\n\n")
            continue 

        # --- СИМУЛЯЦИЯ (ФИКС ВСЕХ ОШИБОК) ---
        h_ovr = get_squad_rating(h_id) 
        a_ovr = get_squad_rating(a_id)
        h_f = FORMATION_MODS.get(h_form, {"atk": 1.0, "def": 1.0})
        a_f = FORMATION_MODS.get(a_form, {"atk": 1.0, "def": 1.0})

        h_chance = (0.15 + (h_ovr - a_ovr) / 100) * h_f["atk"] / a_f["def"]
        a_chance = (0.15 + (a_ovr - h_ovr) / 100) * a_f["atk"] / h_f["def"]

        # Загружаем составы (с рейтингом для весов)
        keys = ['id', 'player_name', 'pos', 'rating']
        c.execute('SELECT id, player_name, pos, rating FROM squad WHERE user_id = ? AND slot_id IS NOT NULL', (h_id,))
        h_players = [dict(zip(keys, p)) for p in c.fetchall()]
        c.execute('SELECT id, player_name, pos, rating FROM squad WHERE user_id = ? AND slot_id IS NOT NULL', (a_id,))
        a_players = [dict(zip(keys, p)) for p in c.fetchall()]

        # Инициализируем скамейки ЗАРАНЕЕ (решает ошибку "is not defined")
        keys_bench = ['id', 'player_name', 'pos']
        c.execute('SELECT id, player_name, pos FROM squad WHERE user_id = ? AND slot_id IS NULL AND is_banned = 0 AND injury_remaining = 0', (h_id,))
        h_bench = [dict(zip(keys_bench, p)) for p in c.fetchall()]
        c.execute('SELECT id, player_name, pos FROM squad WHERE user_id = ? AND slot_id IS NULL AND is_banned = 0 AND injury_remaining = 0', (a_id,))
        a_bench = [dict(zip(keys_bench, p)) for p in c.fetchall()]

        h_score, a_score = 0, 0
        match_events = []
        # Копируем активные составы
        h_active = [dict(p) for p in h_players]
        a_active = [dict(p) for p in a_players]
        
        # Определяем вратарей через ключи
        h_gk_name = next((p['player_name'] for p in h_active if p['pos'] == 'GK'), "Вратарь")
        a_gk_name = next((p['player_name'] for p in a_active if p['pos'] == 'GK'), "Вратарь")

        played_ids = {p['id']: 1.0 for p in h_active + a_active}

        # Ограничиваем циклы до 12
        for _ in range(12): 
            minute = random.randint(1, 90)
            roll = random.random()
            
            h_count, a_count = len(h_active), len(a_active)
            # ФИКС ZeroDivisionError
            h_mod = (h_count / 11) ** 2 if h_count > 0 else 0
            a_mod = (a_count / 11) ** 2 if a_count > 0 else 0
            
            h_prob = max(0, h_chance * h_mod)
            a_prob = max(0, a_chance * a_mod)
            total_prob = h_prob + a_prob

            if total_prob > 0 and roll < total_prob:
                if roll < h_prob:
                    side, side_id, side_club, active, opp_gk = "home", h_id, h_club, h_active, a_gk_name
                else:
                    side, side_id, side_club, active, opp_gk = "away", a_id, a_club, a_active, h_gk_name
                
                # ФИКС IndexError
                if not active: continue
                
                p = get_weighted_scorer(active)
                event_roll = random.random()
                
                if event_roll < 0.35: 
                    match_events.append((minute, f"🧤 {minute}' <b>{opp_gk}</b> тащит удар от {p['player_name']}!"))
                else:
                    is_goal = False
                    is_penalty = False
                    goal_type_txt = ""
                    
                    if event_roll < 0.50: # ПЕНАЛЬТИ
                        if random.random() < 0.8: 
                            goal_type_txt = f"🥅 {minute}' Пенальти! Гол: <b>{p['player_name']}</b>"
                            is_goal, is_penalty = True, True
                        else: 
                            match_events.append((minute, f"❌ {minute}' {p['player_name']} мажет с пенальти!"))
                    elif event_roll < 0.60: # ШТРАФНОЙ
                        if random.random() < 0.3: 
                            goal_type_txt = f"🎯 {minute}' Прямой удар со штрафного! <b>{p['player_name']}</b>"
                            is_goal = True
                            assist_name = None 
                        else: 
                            match_events.append((minute, f"🧱 {minute}' {p['player_name']} попал в стенку."))
                    else: # ОБЫЧНЫЙ ГОЛ
                        goal_type_txt = f"⚽️ {minute}' Гол! <b>{p['player_name']}</b>"
                        is_goal = True

                    if is_goal:
                        if side == "home": h_score += 1
                        else: a_score += 1
                        
                        # ФИКС АССИСТОВ ПРИ ПЕНКАХ
                        p_asst = None
                        if not is_penalty:
                            p_asst = get_weighted_assister(active, p['id'])
                        
                        final_txt = f"{goal_type_txt} (пас: {p_asst['player_name']}) ({side_club})" if p_asst else f"{goal_type_txt} ({side_club})"
                        match_events.append((minute, final_txt))
                        
                        # Сохраняем статистику (используем ID как ключ словаря)
                        c.execute('UPDATE squad SET goals = goals + 1 WHERE id = ?', (p['id'],))
                        if p_asst: 
                            c.execute('UPDATE squad SET assists = assists + 1 WHERE id = ?', (p_asst['id'],))
                        
                        c.execute("INSERT OR IGNORE INTO league_stats (player_id, user_id) VALUES (?, ?)", (p['id'], side_id))
                        c.execute("UPDATE league_stats SET goals = goals + 1 WHERE player_id = ?", (p['id'],))
                        if p_asst:
                             c.execute("INSERT OR IGNORE INTO league_stats (player_id, user_id) VALUES (?, ?)", (p_asst['id'], side_id))
                             c.execute("UPDATE league_stats SET assists = assists + 1 WHERE player_id = ?", (p_asst['id'],))

            # --- 2. ЗАМЕНЫ (Шанс 15% - чаще голов, как в ирле) ---
            if random.random() < 0.15:
                side = random.choice([
                    {"active": h_active, "bench": h_bench, "club": h_club},
                    {"active": a_active, "bench": a_bench, "club": a_club}
                ])
                if side["bench"] and len(side["active"]) > 0:
                    # ФИКС: Драгушин не уйдет сразу. 
                    # Добавляем проверку: played_ids.get(p['id'], 0) должен быть равен 1.0 (играл с начала)
                    # Если значение 0.5 — значит он САМ вышел на замену и мы его не трогаем.
                    out_pool = [
                        p for p in side["active"] 
                        if p['pos'] != 'GK' and played_ids.get(p['id'], 0) == 1.0
                    ]
                    
                    # Если все в поле уже «свежие» запасные, берем любого не вратаря
                    if not out_pool:
                        out_pool = [p for p in side["active"] if p['pos'] != 'GK']
                        
                    if out_pool:
                        p_out = random.choice(out_pool)
                        sub = next((b for b in side["bench"] if b['pos'] == p_out['pos']), side["bench"][0])
                        
                        match_events.append((minute, f"🔄 Тактическая замена: {sub['player_name']} ⬆️ {p_out['player_name']} ⬇️ ({side['club']})"))
                        
                        # Помечаем обоих как «отыгравших часть матча»
                        played_ids[p_out['id']] = 0.5 
                        played_ids[sub['id']] = 0.5 # Теперь sub не попадет в out_pool при следующей проверке
                        
                        side["active"].remove(p_out)
                        side["active"].append(dict(sub))
                        side["bench"].remove(sub)

            # --- 3. ДИСЦИПЛИНА И ТРАВМЫ (Шанс 12% общ.) ---
            if random.random() < 0.12:
                side = random.choice([
                    {"id": h_id, "active": h_active, "bench": h_bench, "club": h_club},
                    {"id": a_id, "active": a_active, "bench": a_bench, "club": a_club}
                ])
                if side["active"]:
                    p_c = random.choice(side["active"])
                    sub_roll = random.random()

                    if sub_roll < 0.15: # ТРАВМА
                        dur = random.randint(2, 4) 
                        match_events.append((minute, f"🚑 {minute}' Травма! {p_c['player_name']} ({side['club']})"))
                        
                        # ЗАМЕНА b[2] на b['pos'] и p_c[2] на p_c['pos']
                        sub = next((b for b in side["bench"] if b['pos'] == p_c['pos']), None)
                        if not sub and side["bench"]: sub = side["bench"][0]
                        
                        if sub:
                            match_events.append((minute, f"🔄 Вынужденная замена: {sub['player_name']} ⬆️ {p_c['player_name']} ⬇️"))
                            played_ids[p_c['id']] = 0.5
                            played_ids[sub['id']] = 0.5
                            side["active"].remove(p_c)
                            side["active"].append(dict(sub))
                            side["bench"].remove(sub)
                        else:
                            match_events.append((minute, f"⚠️ {side['club']} в меньшинстве!"))
                            side["active"].remove(p_c)
                        
                        # ЗАМЕНА p_c[0] на p_c['id']
                        c.execute('UPDATE squad SET injury_remaining = ?, slot_id = NULL, status = "bench" WHERE id = ?', (dur, p_c['id']))
                    
                    elif sub_roll < 0.90: # ЖЕЛТАЯ КАРТОЧКА
                        match_events.append((minute, f"🟨 {minute}' ЖК: {p_c['player_name']} ({side['club']})"))
                        c.execute('UPDATE squad SET yellow_cards = yellow_cards + 1 WHERE id = ?', (p_c['id'],))
                        c.execute("UPDATE league_stats SET yellow_cards = yellow_cards + 1 WHERE player_id = ?", (p_c['id'],))
                    
                    else: # КРАСНАЯ КАРТОЧКА
                        match_events.append((minute, f"🟥 {minute}' Удаление! <b>{p_c['player_name']}</b> ({side['club']})"))
                        side["active"].remove(p_c)
                        c.execute('UPDATE squad SET is_banned = 2, slot_id = NULL, status = "bench" WHERE id = ?', (p_c['id'],))
                        c.execute("UPDATE league_stats SET red_cards = red_cards + 1 WHERE player_id = ?", (p_c['id'],))

        # --- ПОСЛЕ МАТЧА: ПРИМЕНЯЕМ УСТАЛОСТЬ ---
        base_fatigue = 15 
        for p_id, multiplier in played_ids.items():
            fatigue_to_add = int(base_fatigue * multiplier)

            c.execute('''
                UPDATE squad 
                SET stamina = MIN(50, stamina + ?) 
                WHERE id = ?
            ''', (fatigue_to_add, p_id))

        # 1. Определяем победителя
        winner_club = None
        if h_score > a_score: 
            winner_club = h_club
        elif a_score > h_score: 
            winner_club = a_club

        # 2. Вызываем ОДИН РАЗ с winner_club
        performers = get_match_performers(h_players, a_players, match_events, h_club, a_club, winner_club)
        
        mvp_text = " | ".join(performers)

        # --- MVP И ОФОРМЛЕНИЕ (КАК НА ФОТО) ---
        match_events.sort(key=lambda x: x[0])
        events_html = "\n".join([e[1] for e in match_events])
        
        # Выбираем 3 лучших (MVP)
        mvp_text = " | ".join(performers) 

        match_report = (f"<b>{h_club}</b> 🆚 <b>{a_club}</b>\n"
                        f"<code>┏━━━━━━━━━━━━━━━━━━━━┓</code>\n"
                        f"      ⚽️  <b>{h_score} : {a_score}</b>  ⚽️\n"
                        f"<code>┗━━━━━━━━━━━━━━━━━━━━┛</code>\n\n"
                        f"📝 <b>Хронология:</b>\n"
                        f"{events_html if events_html else '<i>— Без моментов</i>'}\n\n"
                        f"🌟 <b>Топ игроки:</b>\n"
                        f"{mvp_text}\n"
                        f"🏁 ————————————————————\n\n")
        final_report += match_report
        
        # === ВОТ ЭТОТ БЛОК НУЖНО ВСТАВИТЬ ===
        if h_score > a_score: # Победа хозяев
            c.execute('UPDATE users SET league_wins = league_wins + 1, league_goals = league_goals + ? WHERE user_id = ?', (h_score, h_id))
            c.execute('UPDATE users SET league_losses = league_losses + 1, league_goals = league_goals + ? WHERE user_id = ?', (a_score, a_id))
        elif a_score > h_score: # Победа гостей
            c.execute('UPDATE users SET league_wins = league_wins + 1, league_goals = league_goals + ? WHERE user_id = ?', (a_score, a_id))
            c.execute('UPDATE users SET league_losses = league_losses + 1, league_goals = league_goals + ? WHERE user_id = ?', (h_score, h_id))
        else: # Ничья
            c.execute('UPDATE users SET league_draws = league_draws + 1, league_goals = league_goals + ? WHERE user_id = ?', (h_score, h_id))
            c.execute('UPDATE users SET league_draws = league_draws + 1, league_goals = league_goals + ? WHERE user_id = ?', (a_score, a_id))
        # ===================================

        c.execute("""
            UPDATE league_schedule 
            SET status = 'completed' 
            WHERE home_id = ? AND away_id = ? AND tour_number = ?
        """, (h_id, a_id, current_tour))
        conn.commit()
        # ПУШИ ИГРОКАМ (ТВОЙ КОД)
        msg_text = (
            f"🏟 <b>МАТЧ ЗАВЕРШЕН!</b>\n\n"
            f"⚔️ <b>{h_club}</b> {h_score}:{a_score} <b>{a_club}</b>\n"
            f"<code>————————————————————</code>\n"
            f"📝 <b>События матча:</b>\n"
            f"{events_html if events_html else 'Тихая игра без острых моментов.'}\n\n"
            f"🌟 <b>Top Performers:</b>\n"
            f"{mvp_text}\n"
            f"————————————————————\n"
            f"📊 <i>Статистика обновлена в профиле.</i>"
        )

        for user_id in [h_id, a_id]:
            try: 
                # Отправляем сообщение с HTML-разметкой
                await cb.bot.send_message(user_id, msg_text, parse_mode="HTML")
            except Exception as e:
                print(f"Ошибка отправки пуша пользователю {user_id}: {e}")

    c.execute('UPDATE settings SET value = value + 1 WHERE key = "window_counter"')
    conn.commit()

    # 2. ДЕБАГ-ВЫВОД (чтобы ты видел реальный прогресс в консоли)
    c.execute('SELECT value FROM settings WHERE key = "window_counter"')
    new_tour_val = c.fetchone()[0]
    print(f"DEBUG: Все матчи тура {current_tour} завершены. Теперь в системе тур: {new_tour_val}")

    # 3. ВЫЧИТАЕМ ТУРЫ (Травмы и Дисквы)
    process_league_aftermath(conn) 

    from interviews import run_random_coach_interview
    await run_random_coach_interview(cb.bot, dp)

    conn.commit()
    conn.close()
    await cb.message.answer(final_report, parse_mode="HTML")

def get_match_performers(h_players, a_players, events, h_club, a_club, winner_club):
    stats = {}
    for p in h_players + a_players:
        # Базовый рейтинг для всех — МЕНЯЕМ p[1] на p['player_name']
        name = p['player_name'] 
        stats[name] = {"goals": 0, "rating": random.uniform(6.0, 7.2), "club": ""}
        
        # Записываем клуб игрока
        if p in h_players: 
            stats[name]["club"] = h_club
        else: 
            stats[name]["club"] = a_club

    # Считаем голы (твоя логика поиска по тексту событий)
    for _, txt in events:
        for name in stats:
            if f"Гол! {name}" in txt or f"Гол! <b>{name}</b>" in txt:
                stats[name]["goals"] += 1
                # Снижаем бонус: теперь гол дает от 1.0 до 1.4 к базе (оставил твой комментарий)
                stats[name]["rating"] += random.uniform(1.0, 1.1) # Тут поправил на 1.4 для разнообразия

    # Бонус игрокам победившей команды
    for name in stats:
        if stats[name]["club"] == winner_club:
            stats[name]["rating"] += random.uniform(0.6, 0.7)

    # Сортируем: сначала ГОЛЫ, потом РЕЙТИНГ
    sorted_p = sorted(stats.items(), key=lambda x: (x[1]['goals'], x[1]['rating']), reverse=True)

    # ПРИНУДИТЕЛЬНОЕ ЗАКРЕПЛЕНИЕ ТОП-ОЦЕНОК (твоя система медалей)
    results = []
    medals = ["🥇", "🥈", "🥉"]
    
    for i in range(min(3, len(sorted_p))): # Добавил min на случай, если игроков мало
        name, d = sorted_p[i]
        final_rating = d['rating']
        
        if i == 0 and d['goals'] > 0:
            final_rating = max(final_rating, random.uniform(8.4, 9.8))
        elif i == 1 and d['goals'] > 0:
            final_rating = max(final_rating, random.uniform(7.9, 8.3))
            
        final_rating = min(final_rating, 10.0) 
        
        goal_str = f" ⚽x{d['goals']}" if d['goals'] > 0 else ""
        results.append(f"{medals[i]} {name} ({round(final_rating, 1)}){goal_str}")
        
    return results

def process_league_aftermath(c):
    # 1. Уменьшаем срок травм
    c.execute('UPDATE squad SET injury_remaining = injury_remaining - 1 WHERE injury_remaining > 0')
    # 2. Возвращаем в строй тех, у кого срок вышел
    c.execute('''UPDATE squad 
                 SET injury_remaining = 0, status = 'active' 
                 WHERE injury_remaining <= 0 AND status != 'active' AND is_banned = 0''')
    # 3. Уменьшаем срок дисквалификаций
    c.execute('UPDATE squad SET is_banned = is_banned - 1 WHERE is_banned > 0')
    # 4. Если бан закончился — делаем активным
    c.execute("UPDATE squad SET status = 'active' WHERE is_banned = 0 AND injury_remaining = 0 AND status != 'active'")

# НОВАЯ ФУНКЦИЯ ДЛЯ МАТЧА ЛИГИ
async def run_league_match_logic(t1_id, t2_id, t1_name, t2_name, bot):
    # Запускаем движок
    res = await play_cup_match_full(t1_id, t2_id, t1_name, t2_name, bot, use_extra_time=False)
    
    conn = get_db(); c = conn.cursor()
    
    # --- 1. ОЧКИ (Все четко) ---
    if res['h_s'] > res['a_s']:
        c.execute("UPDATE users SET wins = wins + 1, points = points + 3 WHERE user_id = ?", (t1_id,))
        c.execute("UPDATE users SET losses = losses + 1 WHERE user_id = ?", (t2_id,))
    elif res['h_s'] < res['a_s']:
        c.execute("UPDATE users SET wins = wins + 1, points = points + 3 WHERE user_id = ?", (t2_id,))
        c.execute("UPDATE users SET losses = losses + 1 WHERE user_id = ?", (t1_id,))
    else:
        c.execute("UPDATE users SET draws = draws + 1, points = points + 1 WHERE user_id = ?", (t1_id,))
        c.execute("UPDATE users SET draws = draws + 1, points = points + 1 WHERE user_id = ?", (t2_id,))

    # --- 2. ЗАПИСЬ СТАТИСТИКИ (Голы, Ассисты, Карточки) ---
    # Убедись, что play_cup_match_full возвращает res['all_events'] как список словарей
    for event in res.get('all_events', []):
        p_id = event.get('player_id')
        u_id = event.get('user_id')
        e_type = event.get('type')

        if p_id and u_id:
            # Создаем запись, если игрока еще нет в таблице лиги
            c.execute("""INSERT OR IGNORE INTO league_stats 
                         (player_id, user_id, goals, assists, yellow_cards, red_cards) 
                         VALUES (?, ?, 0, 0, 0, 0)""", (p_id, u_id))
            
            # Маппинг событий на колонки
            stats_map = {
                'goal': 'goals',
                'assist': 'assists',
                'yellow_card': 'yellow_cards',
                'red_card': 'red_cards'
            }
            
            column = stats_map.get(e_type)
            if column:
                c.execute(f"UPDATE league_stats SET {column} = {column} + 1 WHERE player_id = ?", (p_id,))

    # --- 3. ПОСЛЕМАТЧЕВЫЕ ТРАВМЫ И УСТАЛОСТЬ ---
    # Передаем итоговое время игры (played_ids) если оно есть в res
    if 'played_ids' in res:
        for p_id, load in res['played_ids'].items():
            # Например, снижаем выносливость в зависимости от load (0.5 или 1.0)
            c.execute("UPDATE squad SET energy = MAX(0, energy - ?) WHERE id = ?", (int(load * 15), p_id))

    process_league_aftermath(c)
    
    conn.commit(); conn.close()
    
    # 3. КРАСИВОЕ ИНТЕРВЬЮ (как ты просил)
    interv_user = random.choice([t1_id, t2_id])
    sit = "win" if (interv_user == t1_id and res['h_s'] > res['a_s']) or (interv_user == t2_id and res['a_s'] > res['h_s']) else "loss"
    if res['h_s'] == res['a_s']: sit = "loss" # Ничью считаем за "недовольного" тренера для интереса
    
    await start_interview(bot, interv_user, sit)
    
    return res
    
@dp.message(F.text.casefold() == "отмена")
async def cancel_handler(message: types.Message, state: FSMContext):
    current_state = await state.get_state()
    if current_state is None:
        return
    await state.clear()
    await message.answer("🚫 Действие отменено. Состояние сброшено.", reply_markup=types.ReplyKeyboardRemove())

# --- ШАГ 1: Начало (Имя) ---
@dp.callback_query(F.data == "admin_drop_player")
async def admin_drop_start(cb: types.CallbackQuery, state: FSMContext):
    await state.set_state(AdminMarketStates.waiting_for_name)
    await cb.message.edit_text("👤 Шаг 1: Введите Имя и Фамилию игрока:\n(Или напишите отмена)")

# --- ШАГ 2: Рейтинг ---
@dp.message(AdminMarketStates.waiting_for_name)
async def admin_set_name(m: types.Message, state: FSMContext):
    await state.update_data(adm_name=m.text, adm_positions=[]) # Инициализируем пустой список позиций
    await m.answer(f"Ок, рейтинг для {m.text} (1-99):")
    await state.set_state(AdminMarketStates.waiting_for_rating)

# --- ШАГ 3: Позиции (Мультивыбор) ---
@dp.message(AdminMarketStates.waiting_for_rating)
async def admin_set_rating(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Введите число!")
    await state.update_data(adm_rat=int(m.text))
    await show_position_selection(m, state)

async def show_position_selection(message, state):
    data = await state.get_data()
    selected = data.get("adm_positions", [])
    
    kb = InlineKeyboardBuilder()
    # Список всех доступных позиций
    all_pos = ["GK", "DEF", "MID", "FWD"]
    
    for p in all_pos:
        # Если позиция выбрана, добавляем галочку
        text = f"✅ {p}" if p in selected else p
        kb.button(text=text, callback_data=f"adm_toggle_{p}")
    
    # Кнопка подтверждения (появляется, если выбрана хотя бы одна позиция)
    if selected:
        kb.button(text=f"➡️ Далее (выбрано: {len(selected)})", callback_data="adm_pos_confirm")
    
    kb.adjust(2)
    
    # Информационный текст
    current_str = "/".join(selected) if selected else "не выбраны"
    text = (
        f"🏃‍♂️ <b>Выбор позиций для игрока</b>\n"
        f"————————————————————\n"
        f"Текущие: <b>{current_str}</b>\n\n"
        f"<i>Можно выбрать от 1 до 3 позиций. Нажмите на кнопку еще раз, чтобы убрать.</i>"
    )
    
    if isinstance(message, types.CallbackQuery):
        await message.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    else:
        await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")
    
    await state.set_state(AdminMarketStates.waiting_for_pos)

# Логика переключения (Toggle)
@dp.callback_query(F.data.startswith("adm_toggle_"), AdminMarketStates.waiting_for_pos)
async def admin_toggle_pos(cb: types.CallbackQuery, state: FSMContext):
    pos = cb.data.split("_")[2]
    data = await state.get_data()
    selected = data.get("adm_positions", [])

    if pos in selected:
        # Если уже выбрана — убираем
        selected.remove(pos)
    elif len(selected) < 3: 
        # Если не выбрана и есть место (лимит 3) — добавляем
        selected.append(pos)
    else:
        # Если лимит исчерпан
        return await cb.answer("🚨 Максимум можно выбрать 3 позиции!", show_alert=True)

    await state.update_data(adm_positions=selected)
    await show_position_selection(cb, state)

# --- ШАГ 4: Цена ---
@dp.callback_query(F.data == "adm_pos_confirm", AdminMarketStates.waiting_for_pos)
async def admin_confirm_pos(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("Введите цену выставления (млн €):")
    await state.set_state(AdminMarketStates.waiting_for_price)

# --- ШАГ 5: Финал ---
@dp.message(AdminMarketStates.waiting_for_price)
async def admin_finish_drop(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Введите число!")
    
    price = int(m.text)
    data = await state.get_data()
    p_pos = "/".join(data.get("adm_positions")) # Склеиваем в строку GK/DEF
    
    conn = get_db(); c = conn.cursor()
    c.execute('''INSERT INTO squad (user_id, player_name, rating, pos, status, market_price, stamina) 
                 VALUES (0, ?, ?, ?, 'on_sale', ?, 0)''', 
              (data.get("adm_name"), data.get("adm_rat"), p_pos, price))
    conn.commit(); conn.close()

    kb = InlineKeyboardBuilder()
    kb.button(text="➕ Добавить еще", callback_data="admin_drop_player")
    kb.button(text="✅ Завершить", callback_data="admin_drop_finish")
    kb.adjust(1)

    await m.answer(f"✅ Игрок {data.get('adm_name')} ({p_pos}) добавлен!", reply_markup=kb.as_markup())

# Этот хендлер ловит нажатие на кнопку "Завершить"
@dp.callback_query(F.data == "admin_drop_finish")
async def admin_drop_final_exit(cb: types.CallbackQuery, state: FSMContext):
    # 1. Полностью сбрасываем состояние FSM
    await state.clear() 
    
    # 2. Убираем кнопки под сообщением, чтобы нельзя было нажать еще раз
    await cb.message.edit_text("📥 Наполнение рынка завершено.\nВсе агенты сохранены в базе!", parse_mode="HTML")
    
    # 3. Отвечаем серверу Telegram, чтобы убрать "часики" с кнопки
    await cb.answer()

@dp.callback_query(F.data == "admin_league_start")
async def admin_league_start(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: 
        return await cb.answer("Только для админов!", show_alert=True)
    
    conn = get_db(); c = conn.cursor()
    try:
        # 1. Сбор участников
        c.execute('''
            SELECT lp.user_id 
            FROM league_participants lp
            JOIN users u ON lp.user_id = u.user_id
            WHERE u.club IS NOT NULL AND u.club != ""
        ''')
        participants = [row[0] for row in c.fetchall()]
        
        random.shuffle(participants)
        n = len(participants)

        if n < 2:
            return await cb.message.answer("❌ Нужно минимум 2 команды!")
        
        if n % 2 != 0:
            return await cb.message.answer(f"❌ Нужно четное количество команд (сейчас {n}).")

        # 2. Очистка старых данных
        c.execute('DELETE FROM league_schedule')
        c.execute('UPDATE users SET league_wins=0, league_draws=0, league_losses=0, league_goals=0')

        # 3. Генерация туров (Алгоритм Бергера с чередованием)
        teams = participants[:]
        first_circle = []
        
        for tour in range(n - 1):
            tour_matches = []
            for i in range(n // 2):
                home = teams[i]
                away = teams[n - 1 - i]
                
                # ЧЕРЕДОВАНИЕ: Чтобы не было серий "все дома / все в гостях"
                # В каждом четном туре меняем местами первую пару
                if i == 0 and tour % 2 == 1:
                    tour_matches.append((away, home))
                else:
                    # В остальных парах тоже чередуем стороны для баланса
                    if (i + tour) % 2 == 0:
                        tour_matches.append((home, away))
                    else:
                        tour_matches.append((away, home))
                        
            first_circle.append(tour_matches)
            # Правильное вращение Round-robin: фиксируем первого, остальных сдвигаем
            teams = [teams[0]] + [teams[-1]] + teams[1:-1]

        # 2 круга: Второй круг — это первый, но со сменой сторон
        all_rounds = first_circle + [[(away, home) for home, away in t] for t in first_circle]

        # 4. Подготовка имен
        c.execute('SELECT user_id, club, username FROM users WHERE club IS NOT NULL')
        clubs_dict = {row[0]: (row[1] if row[1] else f"@{row[2]}") for row in c.fetchall()}

        match_data = []
        full_schedule_text = "📅 ПОЛНОЕ РАСПИСАНИЕ СЕЗОНА\n\n"

        for tour_idx, matches in enumerate(all_rounds, 1):
            full_schedule_text += f"Тур {tour_idx}:\n"
            for h_id, a_id in matches:
                match_data.append((h_id, a_id, tour_idx, "pending"))
                h_name = clubs_dict.get(h_id, f"ID:{h_id}")
                a_name = clubs_dict.get(a_id, f"ID:{a_id}")
                full_schedule_text += f"▫️ {h_name} — {a_name}\n"
            full_schedule_text += "\n"
        
        c.executemany('''INSERT INTO league_schedule (home_id, away_id, tour_number, status) 
                         VALUES (?, ?, ?, ?)''', match_data)
        
        conn.commit()

        # 5. Вывод
        summary = (
            f"🏆 <b>ЛИГА СФОРМИРОВАНА!</b>\n"
            f"————————————————————\n"
            f"✅ Команд: <b>{n}</b>\n"
            f"📅 Всего туров: <b>{len(all_rounds)}</b>\n"
            f"⚽️ Баланс сторон: <b>Соблюден</b>\n"
            f"————————————————————\n"
        )
        
        await cb.message.answer(summary, parse_mode="HTML")

        if len(full_schedule_text) > 6000:
            file_buf = io.BytesIO(full_schedule_text.encode())
            await cb.message.answer_document(
                types.BufferedInputFile(file_buf.getvalue(), filename="schedule.txt"), 
                caption="📄 Полное расписание (чередование сторон включено)"
            )
        else:
            await cb.message.answer(full_schedule_text, parse_mode="HTML")

    except Exception as e:
        print(f"Ошибка старта лиги: {e}")
        await cb.answer("Ошибка при генерации", show_alert=True)
    finally:
        conn.close()

@dp.callback_query(F.data == "admin_toggle_transfers")
async def admin_toggle_transfers(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    current = is_transfer_open()
    new_state = 0 if current else 1
    
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE settings SET value = ? WHERE key = "transfer_window"', (new_state,))
    
    clear_msg = ""
    if new_state == 0:
        c.execute('UPDATE squad SET status = "bench", market_price = 0 WHERE status = "on_sale"')
        clear_msg = "\n📦 Все лоты сняты с рынка и вернулись в составы!"
        msg_text = "🛑 ТРАНСФЕРНОЕ ОКНО ЗАКРЫТО!\nСделки больше не принимаются. Смена составов завершена."
    else:
        c.execute('UPDATE settings SET value = value + 1 WHERE key = "window_counter"')
        msg_text = "✅ ТРАНСФЕРНОЕ ОКНО ОТКРЫТО!\nВыставляйте игроков на рынок и укрепляйте составы!"

    conn.commit()

    # 1. Получаем список всех активных юзеров для рассылки
    c.execute('SELECT user_id FROM users')
    all_users = [row[0] for row in c.fetchall()]
    conn.close()

    # 2. Рассылка в ЛС (с защитой от банов)
    count = 0
    for uid in all_users:
        try:
            await cb.bot.send_message(uid, msg_text, parse_mode="HTML")
            count += 1
        except:
            continue # Пропускаем тех, кто удалил бота

    # 3. Рассылка в общий ЧАТ/КАНАЛ (если у тебя есть его ID в конфиге)
    # Если CHANNEL_ID не настроен, просто пропусти этот шаг
    try:
        await cb.bot.send_message(CHAT_ID, f"{msg_text}{clear_msg}", parse_mode="HTML")
    except:
        pass

    # Ответ админу в панель
    status_text = "ОТКРЫТО ✅" if new_state else "ЗАКРЫТО 🛑"
    await cb.message.answer(
        f"📢 <b>Окно: {status_text}</b>\n📨 Рассылка доставлена {count} пользователям.{clear_msg}", 
        parse_mode="HTML"
    )
    await cb.answer()

@dp.message(StateFilter("waiting_for_kick_id"))
async def confirm_kick_club(m: types.Message, state: FSMContext):
    if m.from_user.id not in ADMINS: 
        await state.clear()
        return

    # 1. Превращаем ID в число, чтобы SQL его понял
    try:
        target_id = int(m.text.strip())
    except ValueError:
        return await m.answer("❌ ID должен быть числом. Введи еще раз.")

    conn = get_db(); c = conn.cursor()
    
    # Проверяем, есть ли юзер в базе вообще
    c.execute('SELECT club FROM users WHERE user_id = ?', (target_id,))
    row = c.fetchone()
    
    if not row:
        conn.close()
        return await m.answer(f"❓ Юзер с ID {target_id} не найден в базе.")

    # 2. ПОЛНАЯ ЗАЧИСТКА
    # Удаляем состав
    c.execute('DELETE FROM squad WHERE user_id = ?', (target_id,))
    
    # Сбрасываем клуб в профиле
    c.execute('UPDATE users SET club = NULL WHERE user_id = ?', (target_id,))
    
    # УДАЛЯЕМ ЗАЯВКУ В ЛИГУ (то, что ты просил)
    c.execute('DELETE FROM league_participants WHERE user_id = ?', (target_id,))
    
    conn.commit()
    conn.close()

    # 3. УВЕДОМЛЕНИЯ
    await m.answer(
        f"👞 <b>ПОЛНЫЙ КИК:</b>\n"
        f"ID: <code>{target_id}</code>\n"
        f"✅ Клуб сброшен\n"
        f"✅ Состав удален\n"
        f"✅ Заявка на Лигу аннулирована", 
        parse_mode="HTML"
    )
    
    try:
        await bot.send_message(target_id, "⚠️ Вы были исключены из клуба и сняты с регистрации в Лиге.")
    except:
        pass # Если заблочил бота — плевать

    await state.clear()
    
@dp.callback_query(F.data == "admin_gen_random_fas")
async def admin_gen_random_fas(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return

    # 1. ОБНУЛЯЕМ ЛИМИТЫ У ВЫШИБАЛЫ (Middleware)
    limit_manager.already_caught.clear() 

    conn = get_db()
    c = conn.cursor()

    try:
        c.execute('UPDATE users SET daily_catch = 0') 
        conn.commit()
        
        await cb.answer("⏳ Агенты вылетают...")
        
        first_names = [
            "Luka", "Kevin", "Erling", "Kylian", "Jude", "Mo", "Harry", "Bruno", "Martin", "Leo",
            "Didier", "Diogo", "Moises", "Declan", "Bukayo", "Virgil", "Trent", "Marcus", "Phil", "Alisson",
            "Yan", "David", "Robert", "Angel", "Luis", "Karim", "Antoine", "Eden", "Zlatan", "Lamine",
            "Aaron", "Pedri", "Vinicius", "Rodrygo", "Federico", "Darwin", "Alexis", "Enzo", "Julian", "Lautaro",
            "Bernardo", "Ruben", "Ederson", "Kingsley", "Leroy", "Jamal", "Leon", "Joshua", "Manuel", "Ilkay", 
            "Hristo", "Gheorghe", "Pavel", "Andriy", "Ole", "Clarence", "Park", "Benni",
            "Gianfranco", "Henrik", "Jari", "Davor", "Youri", "Siniša", "Patrik", "Shunsuke", 
            "Juninho", "Royston", "Guti", "Esteban", "Mauro", "Gaizka", "Santi", "Alvaro", "Marek",
            "Vander", "Eidur", "Nwankwo", "Taribo", "Landon", "Timmy", "Cobi", "Lothar", "Bixente", "Jaap",
            "Fabien", "Dino", "Santiago", "Milan", "Dejan", "Tomas", "Hakan", "Emre"
        ]

        last_names = [
            "Smith", "Gomez", "Silva", "Muller", "Kane", "Sane", "Diaz", "Verratti", "Rowe", "Cantona",
            "Elneny", "Kiwior", "Tadic", "Stoichkov", "Hagi","Solskjaer", "Larsson", "Conceição", "Schjelderup", "Malacia"
            "Litmanen", "Šuker", "Djorkaeff", "Mihajlović", "Berger", "Nakamura", "Ji-sung", "McCarthy", "Pernambucano", "Drenthe",
            "Guti", "Cambiasso", "Camoranesi", "Mendieta", "Cazorla", "Negredo", "Hamšík", "Karpin", "Gudjohnsen", "Kanu",
            "West", "Donovan", "Cahill", "Jones", "Matthäus", "Lizarazu", "Stam", "Barthez", "Zoff", "Canizares",
            "Solari", "Zamorano", "Kean", "Baroš", "Stanković", "Rosický", "Yakin", "Belözoğlu", "Recoba", "Riquelme"                             
        ]
        
        for _ in range(3):
            name = f"{random.choice(first_names)} {random.choice(last_names)}"
            rat = random.randint(75, 86)
            pos = random.choice(["FWD", "MID", "DEF", "GK"])
            price = 0 if rat < 80 else (10 if rat < 85 else 15)
            
            c.execute('INSERT INTO squad (user_id, player_name, rating, pos, status, market_price, stamina) VALUES (0, ?, ?, ?, "free_agent", ?, 0)', 
                      (name, rat, pos, price))
            fa_id = c.lastrowid
            
            b = InlineKeyboardBuilder()
            b.button(text=f"⚡️ ЗАБРАТЬ ({price} млн)", callback_data=f"catch_{fa_id}")
            
            await bot.send_message(SET_CHAT_ID, f"🔥 АГЕНТ: <b>{name}</b> ({rat})\n 🏃 Позиция: <b>{pos}</b>\n 💰 Цена: {price} млн", reply_markup=b.as_markup(), parse_mode="HTML")
        
        conn.commit()
    finally:
        conn.close()

@dp.callback_query(F.data == "start_broadcast")
async def broadcast_callback_handler(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS:
        return await cb.answer("У тебя нет прав!", show_alert=True)
    
    await start_broadcast(cb.message)
    await cb.answer()

@dp.message(F.text == "/broadcast")
async def start_broadcast(m: types.Message):
    user_id = m.chat.id 
    broadcast_active.add(user_id) 
    
    await m.answer("✅ Режим рассылки включен!\n\n"
                   "Отправь сообщение (текст, фото, гиф, файл), которое нужно разослать всем.")

@dp.message(lambda m: m.from_user.id in ADMINS and m.from_user.id in broadcast_active)
async def perform_broadcast(m: types.Message):
    
    if m.text in ["/start", "/admin", "Отмена"]:
        broadcast_active.discard(m.from_user.id)
        return

    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id FROM users')
    users = c.fetchall()
    conn.close()

    count, blocked = 0, 0
    confirm_msg = await m.answer(f"🚀 Начинаю рассылку на {len(users)} пользователей...")

    for (uid,) in users:
        try:
            await m.copy_to(chat_id=uid)
            count += 1
            await asyncio.sleep(0.05) 
        except TelegramForbiddenError:
            blocked += 1
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after)
            await m.copy_to(chat_id=uid)
            count += 1
        except Exception:
            pass

    broadcast_active.discard(m.from_user.id)

    await confirm_msg.edit_text(
        f"✅ <b>Рассылка завершена!</b>\n\n"
        f"👤 Получили: {count}\n"
        f"🚫 Заблокировали: {blocked}",
        parse_mode="HTML"
    )

@dp.callback_query(F.data.startswith("catch_"))
async def catch_player(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    fa_id = int(cb.data.split("_")[1])
    
    conn = get_db(); c = conn.cursor()
    
    try:
        c.execute('SELECT balance FROM users WHERE user_id = ?', (user_id,))
        balance = c.fetchone()[0]
        
        c.execute('SELECT player_name, market_price, status FROM squad WHERE id = ?', (fa_id,))
        player = c.fetchone()

        if not player or player[2] != "free_agent":
            limit_manager.already_caught.remove(user_id)
            return await cb.answer("🏃 Игрока уже перехватили!")

        if balance < player[1]:
            limit_manager.already_caught.remove(user_id) 
            return await cb.answer(f"💸 Недостаточно денег!", show_alert=True)

        c.execute('UPDATE users SET balance = balance - ?, daily_catch = 1 WHERE user_id = ?', (player[1], user_id))
        c.execute('UPDATE squad SET user_id = ?, status = "active" WHERE id = ?', (user_id, fa_id))
        conn.commit()

        await cb.message.edit_text(f"✅ Контракт с <b>{player[0]}</b> подписан!", parse_mode="HTML")

    finally:
        conn.close()

# 1. Выбор клуба (Без изменений, тут все ок)
@dp.callback_query(F.data == "admin_upgrade_start")
async def admin_upgrade_clubs(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    builder = InlineKeyboardBuilder()
    for club in CLUBS.keys():
        builder.button(text=club, callback_data=f"adm_up_cl_{club}")
    builder.adjust(2)
    builder.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_main"))
    await cb.message.edit_text("⚙️ <b>Админ-апгрейд</b>\nВыберите клуб:", reply_markup=builder.as_markup(), parse_mode="HTML")

# 2. Список игроков клуба
@dp.callback_query(F.data.startswith("adm_up_cl_"))
async def admin_upgrade_players(cb: types.CallbackQuery):
    club_name = cb.data.replace("adm_up_cl_", "")
    conn = get_db(); c = conn.cursor()
    
    # ИСПРАВЛЕННЫЙ ЗАПРОС: Соединяем squad и users, чтобы найти игроков по названию клуба
    c.execute('''
        SELECT s.id, s.player_name, s.rating 
        FROM squad s
        JOIN users u ON s.user_id = u.user_id
        WHERE u.club = ?
    ''', (club_name,))
    
    players = c.fetchall()
    conn.close()
    
    if not players:
        return await cb.answer(f"❌ В клубе {club_name} нет игроков", show_alert=True)
    
    builder = InlineKeyboardBuilder()
    for p in players:
        builder.button(text=f"{p[1]} ({p[2]})", callback_data=f"adm_up_pl_{p[0]}")
    
    builder.adjust(1)
    builder.row(types.InlineKeyboardButton(text="⬅️ К клубам", callback_data="admin_upgrade_start"))
    await cb.message.edit_text(f"Игроки <b>{club_name}</b>:", reply_markup=builder.as_markup(), parse_mode="HTML")

# 3. Ввод числа (Добавлена проверка pid)
@dp.callback_query(F.data.startswith("adm_up_pl_"))
async def admin_ask_amount(cb: types.CallbackQuery, state: FSMContext):
    pid = cb.data.replace("adm_up_pl_", "")
    # Сохраняем pid игрока, чтобы использовать в следующем шаге
    await state.update_data(up_pid=pid)
    await cb.message.answer("🔢 На сколько поднять рейтинг? (введите число, например 5 или -3)")
    await state.set_state(AdminUpgrade.waiting_for_amount)

# 4. Применение (Полный фикс UPDATE)
@dp.message(AdminUpgrade.waiting_for_amount)
async def admin_apply_upgrade(m: types.Message, state: FSMContext):
    # Проверка на число (включая отрицательные)
    text = m.text.replace('-', '', 1) if m.text.startswith('-') else m.text
    if not text.isdigit(): 
        return await m.answer("❌ Введите целое число!")
    
    data = await state.get_data()
    # Защита от потери данных в state
    if 'up_pid' not in data:
        await state.clear()
        return await m.answer("❌ Ошибка: данные утеряны. Начните заново.")
        
    pid = data['up_pid']
    amount = int(m.text)
    
    conn = get_db(); c = conn.cursor()
    
    # Обновляем именно rating. Стамина не более 50. Сброс слота обязателен.
    c.execute('''UPDATE squad SET 
                 rating = rating + ?, 
                 stamina = CASE WHEN stamina > 50 THEN 50 ELSE stamina END,
                 slot_id = NULL,
                 status = "bench" 
                 WHERE id = ?''', (amount, pid))
    
    c.execute('SELECT player_name, rating FROM squad WHERE id = ?', (pid,))
    p = c.fetchone()
    conn.commit(); conn.close()
    
    if p:
        await m.answer(f"✅ Рейтинг {p[0]} изменен.\n📈 Новый рейтинг: {p[1]}\n🏃‍♂️ Статус: Переведен в запас")
    else:
        await m.answer("❌ Ошибка: Игрок не найден в базе.")
        
    await state.clear()

@dp.message(lambda m: m.text and m.text.split()[0].lower() == "!банан")
async def fun_banan(m: types.Message):
    args = m.text.split()
    target_username = None
    who = m.from_user.first_name
    
    # ПЕРЕНОСИМ ПЕРЕМЕННЫЕ В НАЧАЛО (Чтобы не было UnboundLocalError)
    years = random.randint(1, 50)
    banana_pic = "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcTZ-Sopu2OUA2L_smEuebdS1yGqSq3leOUWUg&s"

    # 1. Проверяем, указан ли юзер через пробел (например, !банан @melon)
    if len(args) > 1:
        target_username = args[1]
    # 2. Если ника нет, проверяем реплей
    elif m.reply_to_message:
        target = m.reply_to_message.from_user
        target_username = f"@{target.username}" if target.username else target.first_name
    
    # Если не нашли ни того, ни другого — даем подсказку
    if not target_username:
        return await m.answer("🍌 Кого бананим? Ответь на сообщение или напиши <code>!банан @юзер</code>")

    try:
        # Формируем текст
        text = f"🍌 <b>{who}</b> забананил <b>{target_username}</b> на <b>{years}</b> лет!"
        
        # Пробуем отправить фото
        await m.answer_photo(
            photo=banana_pic, 
            caption=text, 
            parse_mode="HTML"
        )
        
    except Exception as e:
        # Если фото не прошло, пишем текстом (используем уже готовые переменные)
        print(f"Ошибка банана: {e}")
        try:
            await m.answer(f"🍌 {who} забананил {target_username} на {years} лет! (Картинка потерялась по дороге)")
        except:
            pass
    finally:
        # Здесь можно закрывать соединение с БД, если ты его открывал
        pass

# Вспомогательная функция для парсинга времени
def parse_time(time_str: str):
    units = {
        'м': 'minutes', 'm': 'minutes', 'мин': 'minutes',
        'ч': 'hours', 'h': 'hours', 'час': 'hours',
        'д': 'days', 'd': 'days', 'день': 'days', 'дня': 'days'
    }
    match = re.match(r"(\d+)\s*([а-яА-Яa-zA-Z]+)", time_str)
    if not match: return None
    
    value_raw, unit = match.groups()
    value = int(value_raw)

    # Защита от гигантских чисел (максимум 100 лет, например)
    if value > 36500 and unit.lower()[:1] in ['д', 'd']:
        value = 36500 

    unit_norm = units.get(unit.lower()[:1]) 
    if not unit_norm: return None
    
    try:
        return timedelta(**{unit_norm: value})
    except OverflowError:
        return timedelta(days=36500) # Возвращаем кап, если всё равно летит ошибка

# --- 1. СПИСОК МУТОВ (ПЕРВЫМ) ---
@dp.message(F.text == "!муты")
async def show_punishments(m: types.Message):
    if m.from_user.id not in ADMINS and m.from_user.id not in MODERS: 
        return

    conn = get_db(); c = conn.cursor()
    # Для SQLite лучше сравнивать строки или использовать strftime
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    c.execute('SELECT full_name, user_id, type, until_date, reason FROM punishments WHERE until_date > ?', (now_str,))
    rows = c.fetchall()
    conn.close()

    if not rows:
        return await m.answer("✅ Список наказаний пуст.")

    text = "📂 <b>ТЕКУЩИЕ НАКАЗАНИЯ:</b>\n\n"
    for name, uid, p_type, until, reason in rows:
        icon = "🔇" if p_type == "MUTE" else "🚫"
        
        if p_type == "BAN":
            time_str = "Навсегда"
        else:
            try:
                # Парсим дату из базы для вычисления остатка времени
                until_dt = datetime.strptime(until.split('.')[0], '%Y-%m-%d %H:%M:%S')
                diff = until_dt - datetime.now()
                
                if diff.total_seconds() < 0:
                    continue 
                    
                hours, remainder = divmod(int(diff.total_seconds()), 3600)
                minutes, _ = divmod(remainder, 60)
                time_str = f"{hours}ч {minutes}м"
            except:
                time_str = until # Если ошибка, покажем дату как есть

        text += (f"{icon} <b>{p_type}</b> | {name}\n"
                 f"🆔 ID: <code>{uid}</code>\n"
                 f"⏰ Осталось: {time_str}\n"
                 f"📝 Причина: {reason}\n"
                 f"————————————————\n")

    await m.answer(text, parse_mode="HTML")

# --- 2. РАЗМУТ / РАЗБАН (ВТОРЫМ) ---
@dp.message(lambda m: m.text and (m.text.startswith("!размут") or m.text.startswith("!разбан")))
async def admin_unpunish(m: types.Message):
    user_id = m.from_user.id
    is_unban = "!разбан" in m.text

    # ЛОГИКА ПРАВ: 
    # Разбан — только Админам. Размут — Админам и Модерам.
    if is_unban and user_id not in ADMINS:
        return await m.answer("⚠️ Команда <b>!разбан</b> доступна только администраторам.", parse_mode="HTML")
    
    if not is_unban and (user_id not in ADMINS and user_id not in MODERS):
        return

    args = m.text.split()
    target_id, target_name = None, "Игрок"

    # 1. Определяем цель
    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
        target_name = m.reply_to_message.from_user.full_name
    elif len(args) > 1:
        username_raw = args[1].replace("@", "").strip()
        with get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT user_id FROM users WHERE LOWER(username) = LOWER(?)', (username_raw,))
            res = c.fetchone()
            if res: 
                target_id = res[0]
                target_name = args[1]
            elif args[1].isdigit():
                target_id = int(args[1])
    
    if not target_id:
        return await m.answer("❌ Не понял, кого простить. Ответь на смс или напиши @username.")

    # --- ПРОВЕРКА ИЕРАРХИИ ПРИ РАЗМУТЕ ---
    if not is_unban: # Если это размут
        if target_id in ADMINS and user_id not in ADMINS:
            return await m.answer("❌ Ты не можешь управлять статусом администратора.")
        if target_id in MODERS and user_id not in ADMINS:
            return await m.answer("❌ Модератор не может размутить другого модератора.")

    try:
        if is_unban:
            await m.chat.unban(user_id=target_id, only_if_banned=True)
            text = f"✅ <b>Разбанен:</b> {target_name}"
        else:
            await m.chat.restrict(user_id=target_id, permissions=types.ChatPermissions(
                can_send_messages=True, can_send_media_messages=True, 
                can_send_other_messages=True, can_add_web_page_previews=True))
            text = f"🔊 <b>Размучен:</b> {target_name}"

        # 2. Удаляем из таблицы
        with get_db() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM punishments WHERE user_id = ?', (target_id,))
            conn.commit()
        
        await m.answer(text, parse_mode="HTML")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

# --- 3. МУТ (ТРЕТЬИМ, ИСКЛЮЧАЕТ !муты) ---
@dp.message(lambda m: m.text and m.text.startswith('!мут') and not m.text.startswith('!муты'))
async def admin_mute(m: types.Message):
    user_id = m.from_user.id
    # Проверка прав: админ или модер
    if user_id not in ADMINS and user_id not in MODERS: return

    args = m.text.split(maxsplit=3)
    target_id = None
    target_name = None
    time_arg = ""
    reason = "Не указана"

    # 1. ОПРЕДЕЛЯЕМ ЦЕЛЬ
    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
        target_name = m.reply_to_message.from_user.full_name
        if len(args) > 1:
            time_arg = args[1]
            reason = " ".join(args[2:]) if len(args) > 2 else "Не указана"
    elif len(args) > 1 and args[1].startswith("@"):
        username = args[1].replace("@", "").strip()
        conn = get_db(); c = conn.cursor()
        c.execute('SELECT user_id, club FROM users WHERE LOWER(username) = LOWER(?)', (username.lower(),))
        res = c.fetchone()
        conn.close()
        
        if res:
            target_id = res[0]
            target_name = f"@{username} ({res[1]})"
            if len(args) > 2:
                time_arg = args[2]
                reason = " ".join(args[3:]) if len(args) > 3 else "Не указана"
        else:
            return await m.answer(f"❌ Игрок <b>@{username}</b> не найден в базе.", parse_mode="HTML")
    else:
        return await m.answer("❌ Используй реплей или: <code>!мут @user 30м причина</code>", parse_mode="HTML")

    # --- ПРОВЕРКА ИЕРАРХИИ ---
    if target_id in ADMINS:
        return await m.answer("❌ Нельзя мутить администратора!")
    
    if target_id in MODERS and user_id not in ADMINS:
        return await m.answer("❌ Модератор не может мутить другого модератора!")
    
    if target_id == user_id:
        return await m.answer("❌ Ты не можешь замутить самого себя.")

    # 2. ПРОВЕРКА ВРЕМЕНИ
    if not time_arg:
        return await m.answer("❌ Укажи время (например: 30м, 1ч)")
        
    duration = parse_time(time_arg)
    if not duration:
        return await m.answer("❌ Неверный формат! Используй: 30м, 1ч, 2д")

    until_date = datetime.now() + duration
    
    # 3. ИСПОЛНЕНИЕ
    try:
        await m.chat.restrict(
            user_id=target_id,
            permissions=types.ChatPermissions(
                can_send_messages=False,        
                can_send_media_messages=False, 
                can_send_other_messages=False,  
                can_add_web_page_previews=False,
                can_send_polls=False,
                can_invite_users=False,
                can_pin_messages=False,
                can_change_info=False,
            ),
            until_date=until_date
        )
        
        # Запись в таблицу наказаний
        conn = get_db(); c = conn.cursor()
        c.execute('DELETE FROM punishments WHERE user_id = ?', (target_id,))
        c.execute('INSERT INTO punishments VALUES (?, ?, ?, ?, ?, ?)', 
          (target_id, target_name, 'MUTE', reason, until_date.strftime("%Y-%m-%d %H:%M:%S"), m.from_user.id))
        conn.commit(); conn.close()

        await m.answer(
            f"🔇 <b>ИГРОК ИЗОЛИРОВАН</b>\n"
            f"👤 Кто: {target_name}\n"
            f"⏰ Срок: {time_arg}\n"
            f"📝 Причина: {reason}\n\n"
            f"🚫 <i>Сообщения запрещены.</i>", 
            parse_mode="HTML"
        )
    except Exception as e:
        await m.answer(f"❌ Ошибка Telegram: {e}")

# --- 4. БАН (БЕЗ ИЗМЕНЕНИЙ) ---
@dp.message(lambda m: m.text and m.text.split()[0] == "!бан")
async def admin_ban(m: types.Message):
    if m.from_user.id not in ADMINS: return
    args = m.text.split(maxsplit=2)
    target_id, target_name = None, None
    reason = "Нарушение правил"

    if m.reply_to_message:
        target_id = m.reply_to_message.from_user.id
        target_name = m.reply_to_message.from_user.full_name
        cmd_args = m.text.split(maxsplit=1)
        if len(cmd_args) > 1:
            reason = cmd_args[1]
    elif len(args) > 1:
        username = args[1].replace("@", "").strip()
        with get_db() as conn:
            c = conn.cursor()
            c.execute('SELECT user_id, club FROM users WHERE LOWER(username) = LOWER(?)', (username.lower(),))
            res = c.fetchone()
        if res:
            target_id, target_name = res[0], f"@{username} ({res[1]})"
            if len(args) > 2:
                reason = args[2]
        else:
            return await m.answer(f"❌ Юзер @{username} не найден в базе.")
    else:
        return await m.answer("❌ Ответь на сообщение или напиши: <code>!бан @user причина</code>", parse_mode="HTML")

    try:
        await m.chat.ban(user_id=target_id)
        with get_db() as conn:
            c = conn.cursor()
            c.execute('DELETE FROM punishments WHERE user_id = ?', (target_id,))
            forever_str = (datetime.now() + timedelta(days=36500)).strftime("%Y-%m-%d %H:%M:%S")
            c.execute('INSERT INTO punishments VALUES (?, ?, ?, ?, ?, ?)', 
                      (target_id, target_name, 'BAN', reason, forever_str, m.from_user.id))
            conn.commit()
        await m.answer(f"🚫 <b>БАН</b>\n👤 Игрок: {target_name}\n📝 Причина: {reason}", parse_mode="HTML")
    except Exception as e:
        await m.answer(f"Ошибка API: {e}")

# --- 5. АНТИСПАМ (В САМЫЙ НИЗ) ---
@dp.message() 
async def global_anti_spam_handler(m: types.Message):
    if not m.chat.id or m.from_user.id in ADMINS or m.from_user.id in MODERS:
        return

    user_id = m.from_user.id
    current_time = time.time()

    if user_id not in spam_tracker:
        spam_tracker[user_id] = deque(maxlen=10)

    spam_tracker[user_id].append(current_time)

    if len(spam_tracker[user_id]) == 10:
        time_diff = spam_tracker[user_id][-1] - spam_tracker[user_id][0]
        
        if time_diff <= 5:
            duration = timedelta(hours=2)
            until_date = datetime.now() + duration
            target_name = m.from_user.full_name
            reason = "Флуд / Спам командами"

            try:
                await m.chat.restrict(
                    user_id=user_id,
                    permissions=types.ChatPermissions(can_send_messages=False),
                    until_date=until_date
                )

                conn = get_db(); c = conn.cursor()
                c.execute('DELETE FROM punishments WHERE user_id = ?', (user_id,))
                c.execute('INSERT INTO punishments VALUES (?, ?, ?, ?, ?, ?)', 
                          (user_id, target_name, 'MUTE', reason, until_date.strftime("%Y-%m-%d %H:%M:%S"), 0))
                conn.commit(); conn.close()

                spam_tracker[user_id].clear()
                try: await m.delete()
                except: pass

                await m.answer(
                    f"🔇 <b>СИСТЕМА АНТИФЛУДА</b>\n"
                    f"👤 Нарушитель: {target_name}\n"
                    f"⏰ Срок: 2 часа\n"
                    f"📝 Причина: Чрезмерный спам", 
                    parse_mode="HTML"
                )
                return 
            except Exception as e:
                print(f"Ошибка антиспама: {e}")

@dp.message(F.text == "!топ")
async def show_top_messages(m: types.Message):
    try:
        conn = get_db(); c = conn.cursor()
        
        # Считаем общее количество сообщений за сутки
        c.execute('SELECT SUM(msg_count) FROM msg_stats WHERE msg_count > 0')
        total_msgs = c.fetchone()[0] or 0

        # Выбираем только реальных юзеров (убираем ID Telegram и ботов)
        c.execute('''SELECT full_name, msg_count FROM msg_stats 
                     WHERE msg_count > 0 AND user_id NOT IN (777000, 1087968824)
                     ORDER BY msg_count DESC LIMIT 10''')
        rows = c.fetchall()
        conn.close()

        if not rows:
            return await m.answer("📊 <b>Статистика пока пуста</b>")

        # Формируем заголовок в стиле скриншота
        text = "📊 <b>Статистика по общительным пользователям за сутки</b>\n\n"
        
        # Список лидеров
        for i, (name, count) in enumerate(rows, 1):
            # Экранируем спецсимволы в именах для безопасности HTML
            safe_name = name.replace("<", "&lt;").replace(">", "&gt;")
            text += f"<b>{i}.</b> {safe_name} — {count}\n"
        
        # Подвал с общим количеством
        text += f"\n<b>Всего сообщений:</b> {total_msgs}"
        
        await m.answer(text, parse_mode="HTML")

    except Exception as e:
        # Если база заблокирована (как на скриншоте), выводим понятную ошибку
        print(f"❌ Ошибка вывода топа: {e}")
        await m.answer("⚠️ База данных временно недоступна (locked).")

@dp.message(F.chat.id == -1003513118924)
async def count_messages(m: types.Message):
    # Игнорируем ботов и любые команды, начинающиеся с '!'
    if m.from_user.is_bot or (m.text and m.text.startswith('!')): 
        return
    
    try:
        today = str(datetime.now().date())
        conn = get_db(); c = conn.cursor()
        
        # Сброс, если день сменился
        c.execute('SELECT last_reset FROM msg_stats LIMIT 1')
        res = c.fetchone()
        if res and str(res[0]) != today:
            c.execute('UPDATE msg_stats SET msg_count = 0, last_reset = ?', (today,))
        
        # Обновление статистики игрока
        c.execute('''INSERT INTO msg_stats (user_id, full_name, msg_count, last_reset) 
                     VALUES (?, ?, 1, ?) 
                     ON CONFLICT(user_id) DO UPDATE SET 
                     msg_count = msg_count + 1, 
                     full_name = excluded.full_name,
                     last_reset = excluded.last_reset''', 
                  (m.from_user.id, m.from_user.full_name, today))
        
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"❌ Ошибка записи: {e}")

@dp.message(F.text == "!админ")
async def admin_help(m: types.Message):
    if m.from_user.id not in ADMINS: return
    
    admin_text = (
        "⚡️ <b>ПАНЕЛЬ УПРАВЛЕНИЯ АДМИНИСТРАТОРА</b>\n"
        "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
        "📊 <b>Просмотр:</b>\n"
        "└ <code>!муты</code> — Список всех активных наказаний\n"
        "└ <code>!инфо</code> — Меню клуба (Изменение игроков всез клубов)\n"
        "└ <code>!топ</code> — Самые активные игроки за 24 часа\n"
        "└ <code>!незаполнены</code> — Клубы с незаполненым составом\n"
        "└ <code>!починить_трени</code> — Починка зависших тренировок\n"
        "└ <code>!собрать</code> — Можешь собрать состав другому клубу\n\n"
        
        "🚫 <b>Наказания:</b>\n"
        "└ <code>!мут 30м причина</code> — Мут (в ответ на смс)\n"
        "└ <code>!мут @user 1ч причина</code> — Мут по юзернейму\n"
        "└ <code>!бан @user причина</code> — Бан и занесение в ЧС\n\n"
        
        
        "🔓 <b>Амнистия:</b>\n"
        "└ <code>!размут</code> — Снять ограничения (реплей/@user)\n"
        "└ <code>!разбан</code> — Разбанить игрока (реплей/@user/ID)\n"
        "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
        "<i>Соблюдайте регламент лиги при выдаче наказаний!</i>"
    )
    
    await m.answer(admin_text, parse_mode="HTML")

@dp.message(F.text == "🏛 Зал Славы")
async def show_hall_of_fame(m: types.Message):
    conn = get_db(); c = conn.cursor()
    c.execute('''
        SELECT achievement_type, player_name, u.club, date_awarded 
        FROM hall_of_fame h
        JOIN users u ON h.user_id = u.user_id
        ORDER BY date_awarded DESC LIMIT 15
    ''')
    rows = c.fetchall(); conn.close()
    
    text = "🏛 <b>ЗАЛ СЛАВЫ ВЕЛИКИХ</b>\n\n"
    if not rows:
        text += "Здесь пока пусто. Время творить историю!"
    else:
        for award, player, club, date_str in rows:
            # Парсим дату (исправлено с учетом твоего формата)
            try:
                d = dt.datetime.strptime(date_str.split('.')[0], '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%y')
            except:
                d = "??.??"
            text += f"🏆 {award}\n👤 <b>{player}</b> ({club})\n📅 <i>{d}</i>\n————————————————\n"

    # Кнопка админки ПОД сообщением
    kb = None
    if m.from_user.id in ADMINS:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⚙️ Вручить награду", callback_data="admin_give_award")],
            [types.InlineKeyboardButton(text="🗑 Удалить награду", callback_data="admin_delete_award_list")]
        ])
    
    await m.answer(text, reply_markup=kb, parse_mode="HTML")

# Этап 1: Выбор типа награды (Обновленный список)
@dp.callback_query(F.data == "admin_give_award", F.from_user.id.in_(ADMINS))
async def start_award(cb: types.CallbackQuery, state: FSMContext):
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        # КОМАНДНЫЕ / ТРЕНЕРСКИЕ (сохраняются сразу на клуб)
        [types.InlineKeyboardButton(text="🥇 Чемпион Лиги", callback_data="award_Победитель Лиги_team")],
        [types.InlineKeyboardButton(text="🏆 Обладатель Кубка", callback_data="award_Обладатель Кубка_team")],
        [types.InlineKeyboardButton(text="⭐ Победитель ЛЧ", callback_data="award_Победитель ЛЧ_team")],
        
        # ЛИЧНЫЕ (спросят имя конкретного игрока)
        [types.InlineKeyboardButton(text="🏅 Игрок сезона", callback_data="award_Игрок сезона_player")],
        [types.InlineKeyboardButton(text="🟡 Золотой Мяч", callback_data="award_Золотой Мяч_player")],
        [types.InlineKeyboardButton(text="⚽ Лучший бомбардир", callback_data="award_Лучший бомбардир_player")],
        
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")]
    ])
    await cb.message.edit_text("Выберите тип награды:", reply_markup=kb)
    await state.set_state(AwardStates.choosing_type)

# Этап 2: Выбор юзера из списка (без изменений)
@dp.callback_query(F.data.startswith("award_"), AwardStates.choosing_type)
async def award_type_selected(cb: types.CallbackQuery, state: FSMContext):
    parts = cb.data.split("_")
    award_name = parts[1]
    award_kind = parts[2] # team или player
    
    await state.update_data(current_award=award_name, award_kind=award_kind)
    
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id, username, club FROM users WHERE club IS NOT NULL')
    users = c.fetchall(); conn.close()
    
    kb_builder = types.InlineKeyboardMarkup(inline_keyboard=[])
    for u_id, username, club in users:
        kb_builder.inline_keyboard.append([
            types.InlineKeyboardButton(text=f"{club} (@{username or u_id})", callback_data=f"seluser_{u_id}")
        ])
    
    await cb.message.edit_text(f"🏆 {award_name}\nКому вручаем?", reply_markup=kb_builder)
    await state.set_state(AwardStates.choosing_user)

# Этап 3: Проверка и финализация
@dp.callback_query(F.data.startswith("seluser_"), AwardStates.choosing_user)
async def user_for_award_selected(cb: types.CallbackQuery, state: FSMContext):
    target_id = cb.data.split("_")[1]
    data = await state.get_data()
    
    if data['award_kind'] == 'player':
        # Для Игрока сезона / Золотого мяча спрашиваем имя
        await state.update_data(target_user_id=target_id)
        await cb.message.edit_text("Введите <b>имя футболиста</b> (игрок сезона):", parse_mode="HTML")
        await state.set_state(AwardStates.entering_data)
    else:
        # Для Победителя ЛЧ / Лиги / Кубка сохраняем сразу на тренера
        conn = get_db(); c = conn.cursor()
        # В поле имени игрока пишем "Главный тренер", так как это командный успех
        c.execute('INSERT INTO hall_of_fame (user_id, player_name, achievement_type, date_awarded) VALUES (?, ?, ?, ?)',
                  (int(target_id), "Главный тренер", data['current_award'], dt.datetime.now()))
        conn.commit(); conn.close()
        
        await cb.message.edit_text(f"✅ Достижение <b>{data['current_award']}</b> записано в историю клуба!")
        await state.clear()

# Этап 4: Сохранение если ввели имя (для Игрока сезона)
@dp.message(AwardStates.entering_data)
async def process_award_final(m: types.Message, state: FSMContext):
    data = await state.get_data()
    award_type = data['current_award']
    u_id = data['target_user_id']
    player_name = m.text.strip()
    
    conn = get_db(); c = conn.cursor()
    c.execute('INSERT INTO hall_of_fame (user_id, player_name, achievement_type, date_awarded) VALUES (?, ?, ?, ?)',
              (int(u_id), player_name, award_type, dt.datetime.now()))
    conn.commit(); conn.close()
    
    await m.answer(f"✅ Готово! {player_name} признан {award_type} и занесен в Зал Славы!")
    await state.clear()

@dp.callback_query(F.data == "admin_cancel")
async def cancel_admin_action(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("❌ Действие отменено.")

# 2. Хендлер вывода списка для удаления
@dp.callback_query(F.data == "admin_delete_award_list", F.from_user.id.in_(ADMINS))
async def list_awards_for_delete(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    # Получаем последние 15 записей
    c.execute('''
        SELECT h.id, achievement_type, player_name, u.club 
        FROM hall_of_fame h
        JOIN users u ON h.user_id = u.user_id
        ORDER BY date_awarded DESC LIMIT 15
    ''')
    rows = c.fetchall(); conn.close()

    if not rows:
        return await cb.answer("Зал Славы пуст, удалять нечего!", show_alert=True)

    kb_builder = types.InlineKeyboardMarkup(inline_keyboard=[])
    for row_id, award, player, club in rows:
        # Формируем текст кнопки: Тип | Игрок (Клуб)
        btn_text = f"🗑 {award} | {player} ({club})"
        kb_builder.inline_keyboard.append([
            types.InlineKeyboardButton(text=btn_text, callback_data=f"delaward_{row_id}")
        ])
    
    # Кнопка назад
    kb_builder.inline_keyboard.append([types.InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")])
    
    await cb.message.edit_text("<b>Выберите награду для удаления из БД:</b>", 
                               reply_markup=kb_builder, parse_mode="HTML")

# 3. Хендлер самого удаления
@dp.callback_query(F.data.startswith("delaward_"), F.from_user.id.in_(ADMINS))
async def process_delete_award(cb: types.CallbackQuery):
    award_id = cb.data.split("_")[1]
    
    conn = get_db(); c = conn.cursor()
    c.execute('DELETE FROM hall_of_fame WHERE id = ?', (int(award_id),))
    conn.commit(); conn.close()
    
    await cb.answer("✅ Запись удалена!", show_alert=True)
    # Возвращаем к чистому сообщению
    await cb.message.edit_text("Запись успешно удалена из Зала Славы.")


# --- 1. Исправленная логика автозаполнения ---
async def perform_autofill_logic(user_id, formation_name, c):
    # 1. Определяем схему
    if not formation_name:
        c.execute('SELECT formation FROM users WHERE user_id = ?', (user_id,))
        res = c.fetchone()
        formation_name = res[0] if res else "4-4-2"
    
    f_parts = [int(x) for x in formation_name.split('-')] if '-' in formation_name else [4, 4, 2]
    
    # 2. УЗНАЕМ, КТО УЖЕ СТОИТ В СОСТАВЕ (чтобы не трогать их)
    c.execute('SELECT slot_id, id FROM squad WHERE user_id = ? AND slot_id IS NOT NULL', (user_id,))
    already_placed = c.fetchall()
    occupied_slots = [row[0] for row in already_placed]
    used_player_ids = [row[1] for row in already_placed]

    # 3. ПЛАН РАССТАНОВКИ (Slot ID -> Позиция)
    # Распределяем слоты по позициям (обычно 1 - ГК, 2-5 - ЗАЩ и т.д.)
    formation_logic = [
        ("GK", 1, 1), # Позиция, лимит, начальный слот
        ("DEF", f_parts[0], 2),
        ("MID", f_parts[1], 2 + f_parts[0]),
        ("FWD", f_parts[2], 2 + f_parts[0] + f_parts[1])
    ]
    
    # 4. ЗАПОЛНЯЕМ ТОЛЬКО ПУСТЫЕ СЛОТЫ
    for pos_code, limit, start_slot in formation_logic:
        for i in range(limit):
            current_slot = start_slot + i
            
            # Если этот слот уже занят игроком (владельцем), пропускаем его
            if current_slot in occupied_slots:
                continue
                
            # Ищем лучшего доступного игрока на эту позицию
            query = f'''
                SELECT id FROM squad 
                WHERE user_id = ? AND pos LIKE ? 
                AND injury_remaining = 0 AND is_banned = 0 
                AND slot_id IS NULL 
                AND id NOT IN ({",".join(map(str, used_player_ids)) if used_player_ids else "0"})
                ORDER BY rating DESC LIMIT 1
            '''
            c.execute(query, (user_id, f"%{pos_code}%"))
            player = c.fetchone()
            
            if player:
                c.execute('UPDATE squad SET slot_id = ?, status = "active" WHERE id = ?', (current_slot, player[0]))
                used_player_ids.append(player[0])
                occupied_slots.append(current_slot)

    # 5. ФИНАЛЬНЫЙ ДОБОР (Если какие-то слоты до сих пор пусты — берем любых лучших)
    for slot in range(1, 12):
        if slot not in occupied_slots:
            c.execute(f'''
                SELECT id FROM squad 
                WHERE user_id = ? AND slot_id IS NULL 
                AND injury_remaining = 0 AND is_banned = 0
                AND id NOT IN ({",".join(map(str, used_player_ids)) if used_player_ids else "0"})
                ORDER BY rating DESC LIMIT 1
            ''', (user_id,))
            extra_player = c.fetchone()
            if extra_player:
                c.execute('UPDATE squad SET slot_id = ?, status = "active" WHERE id = ?', (slot, extra_player[0]))
                used_player_ids.append(extra_player[0])
                occupied_slots.append(slot)

# --- 2. ХЕНДЛЕР !собрать (с обязательным COMMIT) ---
@dp.message(F.text == "!собрать")
async def admin_mass_autofill_and_list(message: types.Message):
    if message.from_user.id not in ADMINS: return

    conn = get_db()
    c = conn.cursor()
    
    # Включаем WAL режим прямо перед массовой операцией для скорости
    c.execute("PRAGMA journal_mode=WAL")
    
    c.execute("SELECT user_id, club, formation FROM users WHERE club IS NOT NULL")
    all_users = c.fetchall()
    
    if not all_users:
        conn.close()
        return await message.answer("❌ В базе пока нет клубов.")

    count = 0
    for uid, c_name, form in all_users:
        try:
            await perform_autofill_logic(uid, form, c)
            count += 1
        except Exception as e:
            print(f"ОШИБКА В КЛУБЕ {uid}: {e}")
            continue
    
    # САМОЕ ВАЖНОЕ
    conn.commit() 
    conn.close()

    await message.answer(f"🚀 Собрано составов: {count}\nВсе изменения сохранены в БД.")
# --- 3. МЕНЮ КЛУБА (С ЗАЩИТОЙ ОТ INDEX ERROR) ---
@dp.callback_query(F.data.startswith("manage_club_"))
async def manage_specific_club(cb: types.CallbackQuery):
    # ЗАЩИТА: проверяем, что в callback_data реально есть ID
    data_parts = cb.data.split("_")
    if len(data_parts) < 3:
        return await cb.answer("Ошибка данных кнопки")
        
    target_uid = int(data_parts[2])
    
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT club, username FROM users WHERE user_id = ?', (target_uid,))
    user_data = c.fetchone()
    
    c.execute('''SELECT slot_id, player_name, rating, stamina 
                 FROM squad WHERE user_id = ? AND slot_id IS NOT NULL 
                 ORDER BY slot_id ASC''', (target_uid,))
    players = c.fetchall()
    conn.close()

    if not user_data: 
        return await cb.answer("Клуб не найден")
    
    club_name, owner_name = user_data
    text = (f"🏟 <b>{club_name}</b>\n"
            f"👤 Владелец: @{owner_name}\n"
            f"————————————————\n"
            f"📋 <b>Стартовый состав:</b>\n")
    
    for slot, name, rat, stam in players:
        fatigue = max(0, min(100, stam))
        if fatigue < 20: emoji = "🔋"
        elif fatigue < 50: emoji = "⚡️"
        elif fatigue < 80: emoji = "🪫"
        else: emoji = "💀"

        text += f"{slot}. {name} ({rat}) {emoji} <b>{fatigue}%</b>\n"

    builder = InlineKeyboardBuilder()
    builder.row(types.InlineKeyboardButton(text="⚡️ ПЕРЕСОБРАТЬ КЛУБ", callback_data=f"autofill_{target_uid}"))
    builder.row(types.InlineKeyboardButton(text="⬅️ Назад", callback_data="back_to_list"))
    
    await cb.message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    await cb.answer()

# --- 4. КНОПКА ПЕРЕСБОРА ВНУТРИ КЛУБА ---
@dp.callback_query(F.data.startswith("autofill_"))
async def process_autofill(cb: types.CallbackQuery):
    # Исправлено: забираем ID (он идет вторым после autofill_)
    target_uid = int(cb.data.split("_")[1])
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT formation FROM users WHERE user_id = ?', (target_uid,))
    res = c.fetchone()
    
    # Вызываем логику
    await perform_autofill_logic(target_uid, res[0] if res else "4-4-2", c)
    
    conn.commit(); conn.close()
    await cb.answer("✅ Клуб пересобран!")
    await manage_specific_club(cb)

# --- 5. НАЗАД ---
@dp.callback_query(F.data == "back_to_list")
async def back_to_list_handler(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT user_id, club FROM users WHERE club IS NOT NULL")
    all_clubs = c.fetchall(); conn.close() 
    builder = InlineKeyboardBuilder()
    for uid, club_name in all_clubs:
        builder.row(types.InlineKeyboardButton(text=f"🏘 {club_name}", callback_data=f"manage_club_{uid}"))
    await cb.message.edit_text("🛠 <b>АДМИН-ПАНЕЛЬ: Выбор клуба</b>", reply_markup=builder.as_markup(), parse_mode="HTML")

# --- 6. ПРОВЕРКА НЕЗАПОЛНЕННЫХ ---
@dp.message(F.text == "!незаполнены")
async def check_empty_squads(message: types.Message):
    if message.from_user.id not in ADMINS: return

    conn = get_db(); c = conn.cursor()
    c.execute('''
        SELECT u.club, u.user_id, COUNT(s.id) as starters
        FROM users u
        LEFT JOIN squad s ON u.user_id = s.user_id AND s.slot_id IS NOT NULL
        WHERE u.club IS NOT NULL
        GROUP BY u.user_id
        HAVING starters < 11
    ''')
    results = c.fetchall()
    conn.close()

    if not results:
        return await message.answer("✅ У всех клубов полные составы (11/11).")

    text = "📋 <b>КЛУБЫ С НЕПОЛНЫМ СОСТАВОМ:</b>\n\n"
    for club, uid, count in results:
        text += f"▪️ {club} — <b>{count}/11</b>\n"

    await message.answer(text, parse_mode="HTML")

# 3. Моментальная очистка всей базы (Полный вайп)
@dp.callback_query(F.data == "admin_full_reset")
async def confirm_full_reset(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    c.execute('DELETE FROM users')
    c.execute('DELETE FROM squad')
    # Если есть таблица рынка, её тоже чистим
    # c.execute('DELETE FROM market') 
    conn.commit(); conn.close()
    
    await cb.message.answer("🧨 <b>БАЗА ДАННЫХ ПОЛНОСТЬЮ ОЧИЩЕНА</b>\nВсе пользователи и игроки удалены.", parse_mode="HTML")
    await cb.answer()


async def main():
    # 1. Сначала СТРОГО инициализация
    init_db() 
    print("✅ База данных инициализирована")

    dp.include_router(interview_router)
    
    # КРИТИЧЕСКИ ВАЖНО: Дай SQLite время прописать таблицы на диск
    await asyncio.sleep(1)

    await bot.delete_webhook(drop_pending_updates=True)

    # 2. Теперь, когда таблицы ТОЧНО есть, запускаем остальное
    try:
        # Восстановление тренировок
        await restore_training_tasks(bot) 
        print("✅ Задачи тренировок восстановлены из БД")
        
        # Рековери процессов
        asyncio.create_task(process_recovery(get_db)) 
    except Exception as e:
        print(f"❌ Ошибка при старте сервисов: {e}")

    # 3. Планировщик
    if not scheduler.running:
        scheduler.start() 
    
    from interviews import check_scandal_event
    scheduler.add_job(check_scandal_event, "interval", hours=6, args=(bot, dp))

    # 4. Запуск бота
    print("🚀 Бот запущен...")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query", "chat_member", "inline_query"])

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Бот выключен")
