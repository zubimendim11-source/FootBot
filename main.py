import asyncio, sqlite3, logging, random, time, tired, injured, types, recovery, datetime, io
from datetime import timedelta
from clubs import CLUBS
from typing import Union # Чтобы не было ошибок с типами
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
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

scheduler = AsyncIOScheduler()

processing_catches = set()
already_caught = set()
router = Router()
broadcast_active = set() 

class AdminMarketStates(StatesGroup):
    waiting_for_name = State()
    waiting_for_rating = State()
    waiting_for_pos = State()
    waiting_for_price = State()

class AdminUpgrade(StatesGroup):
    waiting_for_club = State()
    waiting_for_player = State()
    waiting_for_amount = State()

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
    conn = get_db()
    c = conn.cursor()

    # 1. ТАБЛИЦА ЮЗЕРОВ
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        club TEXT,
        balance INTEGER DEFAULT 100,
        formation TEXT DEFAULT "4-3-3",
        wins INTEGER DEFAULT 0,
        draws INTEGER DEFAULT 0,
        losses INTEGER DEFAULT 0,
        goals_scored INTEGER DEFAULT 0,
        assists INTEGER DEFAULT 0,
        yellow_cards INTEGER DEFAULT 0,
        red_cards INTEGER DEFAULT 0,
        daily_catch INTEGER DEFAULT 0,
        last_match TEXT,
        last_recovery TEXT,
        chat_id INTEGER,
        league_wins INTEGER DEFAULT 0,
        league_draws INTEGER DEFAULT 0,
        league_losses INTEGER DEFAULT 0,
        league_goals INTEGER DEFAULT 0
    )''')

    # 2. ТАБЛИЦА СОСТАВА (SQUAD)
    c.execute('''CREATE TABLE IF NOT EXISTS squad (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        player_name TEXT,
        rating INTEGER,
        pos TEXT,
        status TEXT DEFAULT "bench",
        slot_id INTEGER DEFAULT NULL,
        market_price INTEGER DEFAULT 0,
        goals INTEGER DEFAULT 0,
        assists INTEGER DEFAULT 0,
        yellow_cards INTEGER DEFAULT 0,
        red_cards INTEGER DEFAULT 0,
        is_banned INTEGER DEFAULT 0,
        stamina INTEGER DEFAULT 0,
        injury_type TEXT DEFAULT NULL,
        injury_remaining INTEGER DEFAULT 0,
        chat_id INTEGER,
        original_owner_id INTEGER DEFAULT NULL,
        loan_expires_window INTEGER DEFAULT 0,
        loan_to INTEGER DEFAULT NULL,
        loan_term INTEGER DEFAULT 0
    )''')

    # 3. ТАБЛИЦА КУБКА
    c.execute("DROP TABLE IF EXISTS cup_bracket")
    c.execute('''CREATE TABLE IF NOT EXISTS cup_bracket (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        stage TEXT,
        t1_id INTEGER,
        t1_name TEXT,
        t2_id INTEGER,
        t2_name TEXT,
        winner_id INTEGER DEFAULT NULL,
        h_score INTEGER DEFAULT 0,
        a_score INTEGER DEFAULT 0,
        h_pen INTEGER DEFAULT NULL,
        a_pen INTEGER DEFAULT NULL,
        first_leg_score TEXT DEFAULT NULL
    )''')

    # 4. ТАБЛИЦЫ ЛИГИ И СТАТИСТИКИ
    c.execute('''CREATE TABLE IF NOT EXISTS league_stats (
        player_id INTEGER PRIMARY KEY,
        user_id INTEGER,
        goals INTEGER DEFAULT 0,
        assists INTEGER DEFAULT 0,
        yellow_cards INTEGER DEFAULT 0,
        red_cards INTEGER DEFAULT 0
    )''')

    c.execute('CREATE TABLE IF NOT EXISTS league_participants (user_id INTEGER PRIMARY KEY)')
    
    c.execute('''CREATE TABLE IF NOT EXISTS league_schedule (
        id INTEGER PRIMARY KEY AUTOINCREMENT, 
        home_id INTEGER, 
        away_id INTEGER, 
        tour_number INTEGER, 
        status TEXT DEFAULT "pending"
    )''')

    # 5. ТАБЛИЦА НАСТРОЕК
    c.execute('CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value INTEGER)')

    # --- МИГРАЦИИ (ПРОВЕРКА КОЛОНОК) ---
    
    # 1. Миграции для SQUAD
    c.execute("PRAGMA table_info(squad)")
    squad_cols = [col[1] for col in c.fetchall()]
    squad_migrations = [
        ('original_owner_id', 'INTEGER DEFAULT NULL'),
        ('loan_expires_window', 'INTEGER DEFAULT 0'),
        ('is_banned', 'INTEGER DEFAULT 0'),
        ('injury_remaining', 'INTEGER DEFAULT 0'),
        ('loan_to', 'INTEGER DEFAULT NULL'),
        ('loan_term', 'INTEGER DEFAULT 0'),
        ('training_until', 'TEXT DEFAULT NULL')
    ]
    for col_name, col_type in squad_migrations:
        if col_name not in squad_cols:
            c.execute(f'ALTER TABLE squad ADD COLUMN {col_name} {col_type}')

    # 2. Миграции для USERS (колонки Лиги)
    c.execute("PRAGMA table_info(users)")
    user_cols = [col[1] for col in c.fetchall()]
    user_migrations = [
        ('league_wins', 'INTEGER DEFAULT 0'),
        ('league_draws', 'INTEGER DEFAULT 0'),
        ('league_losses', 'INTEGER DEFAULT 0'),
        ('league_goals', 'INTEGER DEFAULT 0')
    ]
    for col_name, col_type in user_migrations:
        if col_name not in user_cols:
            c.execute(f'ALTER TABLE users ADD COLUMN {col_name} {col_type}')

    # --- ИНИЦИАЛИЗАЦИЯ НАСТРОЕК ---
    c.execute('INSERT OR IGNORE INTO settings (key, value) VALUES ("transfer_window", 0)')
    c.execute('INSERT OR IGNORE INTO settings (key, value) VALUES ("current_half", 1)')
    c.execute('INSERT OR IGNORE INTO settings (key, value) VALUES ("window_counter", 1)')
    c.execute('INSERT OR IGNORE INTO settings (key, value) VALUES ("main_chat_id", 0)')

    conn.commit()
    conn.close()
    print("✅ БАЗА РАБОТАЕТ!")

# --- КЛАВИАТУРЫ ---
def get_main_kb(user_id: int):
    b = ReplyKeyboardBuilder()
    b.button(text="💰 Баланс"); b.button(text="📋 Состав")
    b.button(text="📋 Весь состав"); b.button(text="📦 Вне состава")
    b.button(text="🚀 Рынок"); b.button(text="⚽️ Играть (Бот)")
    b.button(text="🏋️‍♂️ Отправить на тренировку")
    b.button(text="📊 Статистика"); b.button(text="📝 Записаться в Лигу")
    b.button(text="🏆 Таблица"); b.button(text="📅 Мои матчи")
    b.button(text="🖼 Сетка Кубка")
    
    if user_id in ADMINS: 
        b.button(text="🛠 Админка")
        
    b.adjust(3, 3, 3, 3)
    return b.as_markup(resize_keyboard=True)
    

# --- МЕХАНИКА ЖЕСТКОГО ЛИМИТА ---
class CatchLimitMiddleware(BaseMiddleware):
    def __init__(self):
        # Список ID тех, кто уже купил игрока
        self.already_caught = set()

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        # Проверяем только нажатия на кнопку "ЗАБРАТЬ"
        if isinstance(event, CallbackQuery) and event.data.startswith("catch_"):
            user_id = event.from_user.id
            
            if user_id in self.already_caught:
                return await event.answer("🚫 Твой лимит: 1 игрок за выброс!", show_alert=True)
            
            # Если лимит не превышен — пускаем дальше и СРАЗУ блокируем
            result = await handler(event, data)
            self.already_caught.add(user_id)
            return result
            
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
TOKEN = "8784991908:AAEBvprrJSu2SWidbaBlB8uoo265TfPRLTs"
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())
dp.callback_query.outer_middleware(limit_manager)
ADMINS = [5611356552, 1812184322, 8298736255]
SET_CHAT_ID = -1003513118924  
CHAT_ID = 5611356552    
# -1003556034012, - тест чат
# -1003345980096 -нищ лига
# -5137303209 - моя лига
# 5611356552 - Я
from aiogram.client.session.aiohttp import AiohttpSession

# Создаем сессию с указанием прокси PythonAnywhere
# session = AiohttpSession(proxy="http://proxy.server:3128")

# Инициализируем бота с этой сессией
# bot = Bot(token=TOKEN, session=session)


matches_data = {}

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

@dp.message(F.text == "🖼 Сетка Кубка")
async def show_cup_grid_message(m: types.Message):
    # Здесь вызываешь ту функцию показа сетки, которую мы писали
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT stage, t1_name, t2_name, winner_id FROM cup_bracket')
    rows = c.fetchall()
    conn.close()
    
    if not rows:
        return await m.answer("🏆 Кубок еще не начался или сетка не сформирована.")
    
    res = "🏆 <b>СЕТКА КУБКА</b>\n\n"
    for r in rows:
        status = "✅" if r[3] else "⏳"
        res += f"{status} {r[0]}: {r[1]} vs {r[2]}\n"
        
    await m.answer(res, parse_mode="HTML")

@dp.callback_query(F.data == "admin_init_cup")
async def admin_init_cup(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return

    teams = [(i, f"Клуб {i}") for i in range(1, 21)]
    
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id, club FROM users ORDER BY (wins*3 + draws) DESC')
    teams = c.fetchall()
    
    if len(teams) < 20:
        return await cb.answer(f"Нужно 20 команд! (У нас {len(teams)})", show_alert=True)

    c.execute('DELETE FROM cup_bracket') 
    
    
    pi_teams = teams[12:]
    for i in range(0, 8, 2):
        c.execute('''INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) 
                     VALUES ('Play-In', ?, ?, ?, ?)''', 
                  (pi_teams[i][0], pi_teams[i][1], pi_teams[i+1][0], pi_teams[i+1][1]))
    
    conn.commit(); conn.close()
    await cb.message.answer("🏆 <b>Кубок инициализирован!</b>\nСформированы пары Плей-ин.", parse_mode="HTML")

async def training_done_callback(bot, user_id, player_id, old_rating):
    conn = get_db(); c = conn.cursor()
    
    # 1. Получаем актуальные данные игрока (используем rating вместо rat)
    c.execute('SELECT player_name, rating FROM squad WHERE id = ?', (player_id,))
    player = c.fetchone()
    
    if player:
        name, current_rating = player
        new_rating = current_rating + 1
        
        # 2. Повышаем рейтинг и снимаем метку тренировки
        c.execute('UPDATE squad SET rating = ?, training_until = NULL WHERE id = ?', (new_rating, player_id))
        conn.commit()
        
        # 3. Отправляем пуш-уведомление
        text = (f"✅ <b>ВНИМАНИЕ!</b>\n\n"
                f"👤 <b>{name}</b> закончил курс тренировок и вернулся в состав!\n"
                f"📈 Улучшение: {old_rating} ➡️ <b>{new_rating}</b>")
        
        try:
            await bot.send_message(user_id, text, parse_mode="HTML")
        except Exception as e:
            print(f"Не удалось отправить пуш: {e}")
            
    conn.close()

@dp.message(F.text == "🏋️‍♂️ Отправить на тренировку")
async def training_selection_list(message: types.Message):
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
    # Разбираем данные из callback_data
    data = cb.data.split("_")
    pid = data[2]
    hours = float(data[3]) # Исправлено: float для корректной обработки 0.5 ч.
    price = int(data[4])
    uid = cb.from_user.id

    conn = get_db(); c = conn.cursor()
    
    # 1. Проверяем баланс юзера
    c.execute('SELECT balance FROM users WHERE user_id = ?', (uid,))
    user_data = c.fetchone()
    
    if not user_data or user_data[0] < price:
        conn.close()
        return await cb.answer("❌ Недостаточно монет для тренировки!", show_alert=True)

    # 2. Проверяем, существует ли игрок
    c.execute('SELECT player_name, rating FROM squad WHERE id = ? AND user_id = ?', (pid, uid))
    player = c.fetchone()
    
    if not player:
        conn.close()
        return await cb.answer("❌ Игрок не найден!", show_alert=True)

    # 3. Списываем деньги и ставим игрока на тренировку
    name, start_rat = player
    # Расчет времени для планировщика и БД
    finish_dt = datetime.datetime.now() + datetime.timedelta(hours=hours)
    finish_time = finish_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    # Списываем баланс
    c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (price, uid))
    
    # Обновляем игрока
    c.execute('''UPDATE squad SET 
                 slot_id = NULL, 
                 status = "bench", 
                 training_until = ? 
                 WHERE id = ?''', (finish_time, pid))
    
    conn.commit(); conn.close()

    # Добавляем задачу в планировщик
    scheduler.add_job(
        training_done_callback, 
        'date', 
        run_date=finish_dt, 
        args=[cb.bot, uid, pid, start_rat]
    )

    await cb.message.edit_text(
        f"✅ Тренировка игрока <b>{name}</b> началась!\n"
        f"⏳ Он вернется через {hours} ч. ({finish_time})",
        parse_mode="HTML"
    )
    await cb.answer("Оплата прошла успешно!")

@dp.message(F.text.lower() == "!выйти")
async def quit_club(m: types.Message):
    uid = m.from_user.id
    conn = get_db(); c = conn.cursor()
    
    # 1. Проверяем наличие клуба
    c.execute('SELECT club FROM users WHERE user_id = ?', (uid,))
    res = c.fetchone()
    
    if not res or not res[0]:
        conn.close()
        return await m.answer("❌ Ты и так вольный агент. Вступать не во что, выходить не откуда.")

    club_name = res[0]
    
    # 2. Убираем привязку к клубу в профиле
    c.execute('UPDATE users SET club = NULL WHERE user_id = ?', (uid,))
    
    # 3. Снимаем всех игроков с позиций (отправляем в запас)
    c.execute('UPDATE squad SET slot_id = NULL, status = "bench" WHERE user_id = ?', (uid,))
    
    # 4. УДАЛЯЕМ ИЗ ЛИГИ (очистка расписания)
    # Удаляем все предстоящие матчи этого юзера, которые еще не сыграны
    c.execute('DELETE FROM league_schedule WHERE (home_id = ? OR away_id = ?) AND status = "pending"', (uid, uid))
    
    # 5. Очищаем статистику лиги для этого юзера (опционально, если хочешь обнулить победы/поражения)
    c.execute('''UPDATE users SET 
                 league_wins = 0, league_draws = 0, league_losses = 0, 
                 league_goals = 0 WHERE user_id = ?''', (uid,))

    conn.commit(); conn.close()
    
    await m.answer(
        f"🏃 <b>ПОЛНЫЙ ВЫХОД</b>\n\n"
        f"Ты покинул клуб <b>{club_name}</b>.\n"
        f"⚠️ Все твои несыгранные матчи в лиге аннулированы, игроки отправлены в запас.\n\n"
        f"Теперь ты — свободный агент.", 
        parse_mode="HTML"
    )

@dp.message(F.text == "!хелп")
async def help_command(m: types.Message):
    help_text = (
        "📖 <b>СПИСОК КОМАНД БОТА</b>\n\n"
        "⚽️ <b>Игровые:</b>\n"
        "└ <code>!клубы</code> — Посмотреть список всех доступных клубов\n"
        "└ <code>!выйти</code> — Покинуть текущий клуб\n\n"
        "🛠 <b>Управление:</b>\n"
        "└ Используй кнопку 🚀 <b>Рынок</b> для торговли\n"
        "└ Используй кнопку 📋 <b>Состав</b> для управления командой\n\n"
        "<i>Инструкция: чтобы выбрать игрока, нажми на пустой слот в меню состава.</i>"
    )
    await m.answer(help_text, parse_mode="HTML")

async def play_cup_match_full(t1_id, t2_id, t1_name, t2_name, bot, prev_score=(0, 0), use_extra_time=True):
    conn = get_db(); c = conn.cursor()
    
    # Достаем составы обеих команд (здоровые и не забаненные)
    c.execute("SELECT id, player_name, pos FROM squad WHERE user_id = ? AND injury_remaining = 0 AND is_banned = 0", (t1_id,))
    squad1 = c.fetchall()
    c.execute("SELECT id, player_name, pos FROM squad WHERE user_id = ? AND injury_remaining = 0 AND is_banned = 0", (t2_id,))
    squad2 = c.fetchall()

    res = {"h_s": 0, "a_s": 0, "h_p": None, "a_p": None, "events": []}

    # --- 1. ОСНОВНОЕ ВРЕМЯ (90 МИНУТ) ---
    for _ in range(12):
        minute = random.randint(1, 90)
        event_roll = random.random()
        
        # Шанс на гол (12%)
        if event_roll < 0.12:
            team = 1 if random.random() < 0.5 else 2
            curr_squad = squad1 if team == 1 else squad2
            if curr_squad:
                scorer = random.choice(curr_squad)
                # Ищем ассистента (любой другой из состава)
                potential_passers = [p for p in curr_squad if p[0] != scorer[0]]
                passer = random.choice(potential_passers) if potential_passers else (None, "---")
                
                if team == 1: res["h_s"] += 1
                else: res["a_s"] += 1
                
                res["events"].append(f"⚽ {minute}' <b>Гол!</b> {scorer[1]} (пас: {passer[1]}) — {t1_name if team==1 else t2_name}")
                c.execute("UPDATE squad SET goals = goals + 1 WHERE id = ?", (scorer[0],))
                if passer[0]: c.execute("UPDATE squad SET assists = assists + 1 WHERE id = ?", (passer[0],))

        # Шанс на карточку (5%)
        elif event_roll < 0.17:
            team = 1 if random.random() < 0.5 else 2
            curr_squad = squad1 if team == 1 else squad2
            if curr_squad:
                player = random.choice(curr_squad)
                if random.random() < 0.15: # Красная (15% от шанса карты)
                    c.execute("UPDATE squad SET red_cards = red_cards + 1, is_banned = 2 WHERE id = ?", (player[0],))
                    res["events"].append(f"🟥 {minute}' <b>Удаление!</b> {player[1]} ({t1_name if team==1 else t2_name})")
                else: # Желтая
                    c.execute("UPDATE squad SET yellow_cards = yellow_cards + 1 WHERE id = ?", (player[0],))
                    res["events"].append(f"🟨 {minute}' ЖК: {player[1]} ({t1_name if team==1 else t2_name})")

        # Шанс на травму (2%)
        elif event_roll < 0.19:
            team = 1 if random.random() < 0.5 else 2
            curr_squad = squad1 if team == 1 else squad2
            if curr_squad:
                player = random.choice(curr_squad)
                dur = random.randint(1, 3)
                c.execute("UPDATE squad SET injury_remaining = ? WHERE id = ?", (dur, player[0]))
                res["events"].append(f"🚑 {minute}' <b>Травма!</b> {player[1]} выбыл на {dur} т.")

    # --- ЛОГИКА ОПРЕДЕЛЕНИЯ НИЧЬИ ПО СУММЕ ДВУХ МАТЧЕЙ ---
    total_h = res["h_s"] + prev_score[0]
    total_a = res["a_s"] + prev_score[1]

    # Если по сумме встреч ничья И нам разрешено доп. время (во втором матче или в обычном раунде)
    if total_h == total_a and use_extra_time:
        
        # --- 2. ДОПОЛНИТЕЛЬНОЕ ВРЕМЯ (30 МИНУТ) ---
        res["events"].append("⏳ <b>Дополнительное время!</b>")
        for minute in [105, 120]:
            if random.random() < 0.08: # Шанс гола в ОТ чуть ниже
                team = 1 if random.random() < 0.5 else 2
                if team == 1: res["h_s"] += 1
                else: res["a_s"] += 1
                res["events"].append(f"⚽ {minute}' <b>ГОЛ В ОТ!</b>")
        
        # Пересчитываем итог после ОТ
        total_h = res["h_s"] + prev_score[0]
        total_a = res["a_s"] + prev_score[1]

        # --- 3. СЕРИЯ ПЕНАЛЬТИ (Если всё еще ничья по сумме) ---
        if total_h == total_a:
            res["events"].append("🎯 <b>СЕРИЯ ПЕНАЛЬТИ!</b>")
            res["h_p"], res["a_p"] = 0, 0
            # Сначала по 5 ударов
            for _ in range(5):
                if random.random() < 0.7: res["h_p"] += 1
                if random.random() < 0.7: res["a_p"] += 1
            # Если ничья — до первого промаха
            while res["h_p"] == res["a_p"]:
                if random.random() < 0.7: res["h_p"] += 1
                if random.random() < 0.7: res["a_p"] += 1

    conn.commit(); conn.close()
    return res

@dp.callback_query(F.data == "view_cup_menu")
async def view_cup_menu(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT stage, t1_name, t2_name, h_s, a_s, h_p, a_p, winner_id FROM cup_bracket')
    rows = c.fetchall(); conn.close()
    
    if not rows:
        return await cb.answer("Кубок еще не начался!", show_alert=True)

    res = "🏆 <b>ТУРНИРНАЯ СЕТКА КУБКА</b>\n\n"
    for r in rows:
        st, t1, t2, hs, ascore, hp, ap, win = r
        status = "✅" if win else "⏳"
        score_str = f"{hs}:{ascore}"
        if hp is not None: score_str += f" ({hp}:{ap} пен.)"
        
        res += f"{status} <b>{st}</b>: {t1} vs {t2} | {score_str if win else 'Ожидание'}\n"

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🔄 Обновить", callback_data="view_cup_menu")],
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="main_menu")]
    ])
    await cb.message.edit_text(res, reply_markup=kb, parse_mode="HTML")

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
class AdminStates(StatesGroup):
    waiting_for_fa_name = State()
    waiting_for_fa_rat = State()
    waiting_for_fa_pos = State()
    waiting_for_fa_price = State()
    target_id = State()
    player_data = State()

class GameStates(StatesGroup):
    choosing_club = State()
    setting_price = State()

# Классы состояний (проверь, чтобы не дублировались)
class MatchStates(StatesGroup):
    live = State()
    half_time = State()
    waiting_for_loan_price = State()

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

# Помести это в начало файла или в настройки
FORMATION_MODS = {
    "4-4-2": {"atk": 1.0, "def": 1.0},  # Сбалансированная
    "4-3-3": {"atk": 1.2, "def": 0.9},  # Атакующая (больше забиваем, чуть больше пропускаем)
    "3-4-3": {"atk": 1.3, "def": 0.7},  # Ва-банк (много атаки, дыры в защите)
    "5-3-2": {"atk": 0.8, "def": 1.3},  # Автобус (сложно забить нам, но и мы редко атакуем)
    "3-5-2": {"atk": 1.1, "def": 1.1},  # Центр поля (небольшой бонус ко всему)
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

@dp.message(F.text == "!id")
async def get_chat_id(m: types.Message):
    await m.answer(f"ID этого чата: {m.chat.id}")

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

@dp.message(Command("!Games"))
@dp.message(F.text == "📅 Мои матчи")
async def show_fixtures(m: types.Message):
    user_id = m.from_user.id
    conn = get_db(); c = conn.cursor()

    # 1. Получаем название клуба пользователя
    c.execute('SELECT club FROM users WHERE user_id = ?', (user_id,))
    user_club = c.fetchone()
    if not user_club or not user_club[0]:
        conn.close()
        return await m.answer("❌ У вас еще нет клуба! Создайте его, чтобы видеть расписание.")

    user_club_name = user_club[0]

    # 2. Ищем все предстоящие матчи этого юзера (где он home или away)
    c.execute('''
        SELECT s.tour_number, u1.club, u2.club, s.home_id
        FROM league_schedule s
        JOIN users u1 ON s.home_id = u1.user_id
        JOIN users u2 ON s.away_id = u2.user_id
        WHERE (s.home_id = ? OR s.away_id = ?) AND s.status = "pending"
        ORDER BY s.tour_number ASC
    ''', (user_id, user_id))
    
    fixtures = c.fetchall()
    conn.close()

    if not fixtures:
        return await m.answer(f"🏟 {user_club_name}\nНа этот сезон матчей не запланировано или все игры уже сыграны.")

    # 3. Формируем красивый список
    text = f"📅 <b>РАСПИСАНИЕ: {user_club_name.upper()}</b>\n"
    text += "————————————————————\n"

    for i, (tour, home_name, away_name, h_id) in enumerate(fixtures):
        # Помечаем, где играет юзер
        role = "🏠 Дома" if h_id == user_id else "✈️ В гостях"
        
        # Выделяем жирным ближайший матч
        if i == 0:
            text += f"🆕 <b>Тур {tour} ({role}):</b>\n"
            text += f"👉 <code>{home_name} — {away_name}</code>\n\n"
            if len(fixtures) > 1:
                text += "<b>Далее:</b>\n"
        else:
            # Остальные матчи компактно
            text += f"▫️ Тур {tour}: <code>{home_name} — {away_name}</code>\n"
        
        # Ограничим вывод, чтобы сообщение не было слишком длинным (например, топ-10 игр)
        if i == 10:
            text += f"\n<i>... и еще {len(fixtures) - 11} матчей</i>"
            break

    text += "\n————————————————————\n"
    text += "<i>Чтобы обновить состав перед туром, используй /squad</i>"

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
            f"Тактика: {data.get('tactic', 'Сбалансированная')}\n"
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

    # --- НАСТРОЙКИ ИГРЫ ---
MATCH_COOLDOWN = 60  # Кулдаун в минутах (сколько ждать между играми)
WIN_REWARD = 5     # Награда за победу (млн €)
DRAW_REWARD = 1    # Награда за ничью (млн €)

# --- ЛОГИКА ---
async def edit_squad_message(message: types.Message, user_id: int, chat_id: int, viewer_id: int = None):
    # Если viewer_id не передан, считаем, что смотрит сам владелец
    if viewer_id is None:
        viewer_id = user_id
    
    is_owner = (user_id == viewer_id)

    # 1. Восстановление стамины (только для владельца при просмотре)
    if is_owner:
        try:
            tired.process_stamina_recovery(user_id) 
        except Exception as e:
            print(f"Ошибка восстановления стамины: {e}")

    conn = get_db()
    c = conn.cursor()
    
    c.execute('SELECT club, formation FROM users WHERE user_id = ?', (user_id,))
    user_data = c.fetchone()
    
    if not user_data or not user_data[0]:
        conn.close()
        text = "❌ Клуб не найден. Начните /start"
        if isinstance(message, types.CallbackQuery):
            return await message.answer(text, show_alert=True)
        return await message.answer(text)

    club_name, formation_name = user_data

    # Расчет схемы
    try:
        f_parts = [int(x) for x in formation_name.split('-')]
        formation_layout = [1] + f_parts
    except:
        formation_layout = [1, 4, 3, 3]
        formation_name = "4-3-3"

    c.execute('''SELECT id, player_name, rating, pos, slot_id, stamina, injury_type 
                 FROM squad 
                 WHERE user_id = ? AND slot_id IS NOT NULL 
                 ORDER BY slot_id ASC''', (user_id,))
    
    slots_dict = {row[4]: row for row in c.fetchall()}
    conn.close()

    current_rating = get_squad_rating(user_id)

    text = (
        f"🏟 <b>{club_name}</b>\n"
        f"⏤⏤⏤⏤⏤⏤⏤⏤⏤⏤\n"
        f"📐 Схема: <b>{formation_name}</b> | ⭐ РТГ: <b>{current_rating}</b>\n\n"
        f"📋 <b>Стартовый состав:</b>\n"
    )

    builder = InlineKeyboardBuilder()
    current_slot = 1
    pos_names = ["GK", "DEF", "MID", "FWD"]
    
    for i, count in enumerate(formation_layout):
        line_pos = pos_names[i]
        for _ in range(count):
            if current_slot in slots_dict:
                pid, name, rat, pos, _, stam, inj = slots_dict[current_slot]
                icon = "🚑" if inj else "✅"
                
                # Если не владелец — кнопка ведет на уведомление
                cb_data = f"pl_{pid}" if is_owner else "view_only_info"
                builder.button(text=icon, callback_data=cb_data)
                
                inj_info = f" [🤕 {inj}]" if inj else ""
                text += f"<code>{current_slot}.</code> {name} ({rat}) 🔋{stam}%{inj_info}\n"
            else:
                # Если не владелец — пустая кнопка вместо плюсика
                cb_data = f"selectpos_{line_pos}_{current_slot}" if is_owner else "view_only_info"
                btn_text = "➕" if is_owner else "▫️"
                builder.button(text=btn_text, callback_data=cb_data)
                text += f"<code>{current_slot}.</code> ——— <i>Пусто ({line_pos})</i> ———\n"
            
            current_slot += 1
            
    # Кнопки управления добавляем ТОЛЬКО владельцу
    if is_owner:
        builder.row(
            types.InlineKeyboardButton(text="⚡️ Автосбор", callback_data="autofill"),
            types.InlineKeyboardButton(text="🗑 Очистить", callback_data="clear_squad"),
            types.InlineKeyboardButton(text="📐 Схемы", callback_data="open_formations")
        )
        builder.row(types.InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="back_to_main"))
    else:
        # Для чужого человека кнопка возврата в профиль того игрока
        builder.row(types.InlineKeyboardButton(text="⬅️ Назад в профиль", callback_data=f"view_profile_{user_id}"))

    builder.adjust(*formation_layout, 2 if is_owner else 1, 1)

    try:
        target = message.message if isinstance(message, types.CallbackQuery) else message
        await target.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")
    except Exception as e:
        if "message is not modified" not in str(e):
            print(f"Ошибка отрисовки состава: {e}")

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

@dp.callback_query(F.data.startswith("setslot_"))
async def set_player_to_slot(cb: types.CallbackQuery):
    # Разбираем: pid - id нового игрока, slot_id - номер места на поле (1-11)
    _, pid, slot_id = cb.data.split("_")
    uid = cb.from_user.id

    if not await check_ownership(cb, pid): return
    
    conn = get_db()
    c = conn.cursor()
    
    # 1. Сначала ВСЕХ игроков этого юзера, у которых стоит этот slot_id, отправляем в запас
    # Это уберет того самого "лишнего" игрока, который там сидел
    c.execute('UPDATE squad SET slot_id = NULL, status = "bench" WHERE user_id = ? AND slot_id = ?', (uid, slot_id))
    
    # 2. Теперь проверяем, не стоит ли выбранный НОВЫЙ игрок уже в каком-то другом слоте
    # (Чтобы один и тот же чел не играл на двух позициях)
    c.execute('UPDATE squad SET slot_id = NULL, status = "bench" WHERE id = ? AND user_id = ?', (pid, uid))
    
    # 3. И только теперь ставим нового игрока в нужный слот
    c.execute('UPDATE squad SET slot_id = ?, status = "active" WHERE id = ? AND user_id = ?', (slot_id, pid, uid))
    
    conn.commit()
    conn.close()
    
    await cb.answer("✅ Состав обновлен!")
    # Перерисовываем меню состава, чтобы увидеть изменения
    await edit_squad_message(cb.message, uid, cb.message.chat.id)

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
    b.row(types.InlineKeyboardButton(text="⬅️ Назад к составу", callback_data=""))
    
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
        # 1. Проверка: не занял ли кто-то клуб, пока мы думали
        c.execute('SELECT username FROM users WHERE club = ? AND user_id != ?', (selected_club, uid))
        owner = c.fetchone()
        
        if owner:
            return await cb.message.answer(f"❌ Клуб {selected_club} уже занят менеджером @{owner[0]}!")

        # 2. Берем бюджет из нашего файла balances.py
        # Используем именно selected_club (то, что пришло из кнопки)
        start_balance = TEAM_BALANCES.get(selected_club, 20_000_000) 

        # 3. Чистим старые данные игрока (если он решил сменить клуб)
        c.execute('DELETE FROM users WHERE user_id = ?', (uid,))
        c.execute('DELETE FROM squad WHERE user_id = ?', (uid,))
        
        # 4. Регистрируем пользователя с НОВЫМ балансом
        c.execute('INSERT INTO users (user_id, username, club, balance) VALUES (?, ?, ?, ?)', 
                  (uid, uname, selected_club, start_balance))

        # 5. Заполняем состав игроками выбранного клуба
        for p in CLUBS[selected_club]["players"]:
            # Если позиция — список или строка MID/DEF, сохраняем как есть
            pos_display = p['pos'] if isinstance(p['pos'], str) else "/".join(p['pos'])
            
            c.execute('''
                INSERT INTO squad (user_id, player_name, rating, pos, status, is_banned) 
                VALUES (?, ?, ?, ?, "bench", 0)
            ''', (uid, p['name'], p['rating'], pos_display))
        
        conn.commit()
        
        # Красивый вывод с форматированием суммы (180,000,000)
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
        await cb.message.answer("Произошла ошибка при регистрации клуба. Попробуй еще раз.")
    finally:
        conn.close()
    
@dp.message(F.text == "📋 Состав")
@dp.message(Command("squad")) 
async def show_squad(m: types.Message):
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
                b.button(text="🤝 Сдать в аренду", callback_data=f"pre_loan_{pid_str}")
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
            now = datetime.datetime.now()
            end_t = datetime.datetime.strptime(t_until, "%Y-%m-%d %H:%M:%S")
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

@dp.message(F.text == "📜 Весь состав")
async def show_full_squad(m: types.Message):
    uid = m.from_user.id
    
    conn = get_db(); c = conn.cursor()
    # SQL-запрос сам фильтрует чужой контент через "WHERE user_id = ?"
    c.execute('''SELECT player_name, rating, pos, status, stamina, injury_remaining 
                 FROM squad 
                 WHERE user_id = ? 
                 ORDER BY rating DESC''', (uid,))
    players = c.fetchall()
    conn.close()

    if not players:
        return await m.answer("📭 Ваш состав пуст.")

    text = "📋 <b>Ваш полный состав:</b>\n\n"
    
    for name, rat, pos, status, stam, inj in players:
        # Формируем статусную иконку
        if inj > 0:
            st = "🚑"
        elif status == "on_sale":
            st = "💰"
        elif status == "bench":
            st = "📥"
        else:
            st = "🟢"
            
        text += f"{st} {name} (⭐{rat}) | {pos} | 🔋{stam}%\n"

    await m.answer(text, parse_mode="HTML")

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
            
            query = f'''SELECT id, player_name, rating FROM squad 
                        WHERE user_id = ? 
                        AND pos LIKE ? 
                        AND status = "bench" 
                        AND injury_remaining = 0 
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
    min_p = 4
    max_p = 250

    if rat >= 95: 
        min_p, max_p = 150, 250
    elif rat >= 90: 
        min_p, max_p = 100, 250
    elif rat >= 85: 
        min_p, max_p = 60, 150
    elif rat >= 80: 
        min_p, max_p = 30, 100
    elif rat >= 75: 
        min_p, max_p = 15, 60
    elif rat >= 70: 
        min_p, max_p = 5, 20
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

# @dp.callback_query(F.data.startswith("pre_loan_"))
# async def pre_loan(cb: types.CallbackQuery, state: FSMContext):
#     if not is_transfer_open():
#         return await cb.answer("🛑 Рынок закрыт!", show_alert=True)
    
#     pid = cb.data.split("_")[2]
#     await state.update_data(loan_pid=pid)
    
#     b = InlineKeyboardBuilder()
#     b.button(text="⏳ Полгода (до след. ТО)", callback_data="loan_dur_1")
#     b.button(text="🗓 Год (через одно ТО)", callback_data="loan_dur_2")
#     await cb.message.edit_text("Выберите срок аренды:", reply_markup=b.as_markup())

# @dp.callback_query(F.data.startswith("loan_dur_"))
# async def set_loan_price(cb: types.CallbackQuery, state: FSMContext):
#     duration = int(cb.data.split("_")[2])
#     await state.update_data(loan_duration=duration)
#     await cb.message.answer("Введите стоимость аренды (млн €):")
#     await state.set_state("waiting_for_loan_price")

@dp.message(F.state == "waiting_for_loan_price")
async def process_loan_market(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Введите число!")
    price = int(m.text)
    data = await state.get_data()
    pid = data.get("loan_pid")
    dur = data.get("loan_duration") # 1 или 2 окна
    
    conn = get_db(); c = conn.cursor()
    # Ставим статус loan_sale (на рынке аренды)
    c.execute('''UPDATE squad SET status = "loan_sale", market_price = ?, 
                 loan_expires_window = ? WHERE id = ?''', (price, dur, pid))
    conn.commit(); conn.close()
    
    await m.answer(f"✅ Игрок выставлен в аренду за {price} млн €!")
    await state.clear()

@dp.message(F.text == "🚀 Рынок")
async def show_market(m: types.Message):
    if not is_transfer_open():
        return await m.answer("🛒 <b>Рынок закрыт.</b>\nДождитесь открытия трансферного окна!", parse_mode="HTML")
    
    conn = get_db(); c = conn.cursor()
    
    # Добавили s.status в запрос, чтобы различать типы сделок
    c.execute('''
        SELECT s.id, s.player_name, s.rating, s.market_price, u.club, s.user_id, s.pos, s.status 
        FROM squad s 
        LEFT JOIN users u ON s.user_id = u.user_id 
        WHERE s.market_price > 0 AND s.status IN ('on_sale', 'loan_sale')
    ''')
    lots = c.fetchall()
    conn.close()

    if not lots:
        return await m.answer("🛒 На рынке пока пусто.")

    for lid, name, rat, price, club_name, seller_id, pos, status in lots:
        if seller_id == 0:
            club_display = "Свободный агент 🌍"
        else:
            club_display = club_name if club_name else "Интер" 
        
        text = (
            f"👤 <b>{name}</b> [{rat}]\n"
            f"🏃 Позиция: <b>{pos}</b>\n"
            f"🏟 Клуб: <b>({club_display})</b>\n"
            f"💰 Цена: <b>{price} млн €</b>"
        )
        
        b = InlineKeyboardBuilder()

        # --- ТА САМАЯ ЛОГИКА КНОПКИ ---
        if status == "loan_sale":
            # Если это аренда, меняем текст кнопки
            b.button(text="🤝 Взять в аренду", callback_data=f"buy_{lid}")
        else:
            # Если обычная продажа
            b.button(text="✅ Купить", callback_data=f"buy_{lid}")
        
        b.button(text="🤝 Торг", callback_data=f"bargain_{lid}")
        
        if seller_id != 0:
            b.button(text="💬 Чат", callback_data=f"chat_{seller_id}")
        else:
            b.button(text="ℹ️ Инфо", callback_data=f"player_info_{lid}")
        
        b.adjust(2, 1) 
        await m.answer(text, reply_markup=b.as_markup(), parse_mode="HTML")

class MarketStates(StatesGroup):
    waiting_for_sell_price = State() 
    waiting_for_bid_price = State()  
    waiting_for_trade_player = State() 
    waiting_for_trade_cash = State()   

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
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT value FROM settings WHERE key = "transfer_window"')
    res = c.fetchone()
    conn.close()
    return res[0] == 1 if res else False

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
        market_min, market_max = 100, 250
    elif rat >= 85: 
        market_min, market_max = 60, 150
    elif rat >= 80: 
        market_min, market_max = 30, 100
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
    
    # 1. Находим всех, у кого закончилась аренда (счетчик стал 1 и мы его сейчас обнулим)
    # original_owner_id — это тот, кому возвращаем
    c.execute('''SELECT id, user_id, original_owner_id, player_name 
                 FROM squad 
                 WHERE original_owner_id IS NOT NULL AND loan_expires_window = 1''')
    expired_loans = c.fetchall()

    for loan_id, current_renter, owner_id, p_name in expired_loans:
        # Возвращаем игрока владельцу, сбрасываем аренду и убираем из состава арендодателя
        c.execute('''UPDATE squad 
                     SET user_id = ?, original_owner_id = NULL, loan_expires_window = 0, 
                         status = "bench", slot_id = NULL 
                     WHERE id = ?''', (owner_id, loan_id))
        
        # Уведомляем владельца
        try:
            await bot.send_message(owner_id, f"✅ Срок аренды истек! Игрок <b>{p_name}</b> вернулся в ваш клуб.", parse_mode="HTML")
            # Уведомляем того, кто арендовал
            await bot.send_message(current_renter, f"⌛ Срок аренды игрока <b>{p_name}</b> истек. Он вернулся к владельцу.", parse_mode="HTML")
        except: pass

    # 2. Уменьшаем счетчик на 1 для всех остальных активных аренд
    c.execute('''UPDATE squad 
                 SET loan_expires_window = loan_expires_window - 1 
                 WHERE original_owner_id IS NOT NULL AND loan_expires_window > 1''')
    
    conn.commit(); conn.close()
    print(f"🔄 Проверка аренд завершена. Вернулось игроков: {len(expired_loans)}")

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
        c.execute('UPDATE squad SET market_price = ? WHERE id = ?', (price, pid))
        conn.commit()
        await m.answer(f"✅ {p_name} на рынке за {price} млн €!")
    
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
    
    # КОНВЕРТАЦИЯ: если в базе балансы типа 90.000.000, а цена 50
    full_price = price_short * 1000000 

    if seller_id == buyer_id:
        conn.close()
        return await cb.answer("🚫 Это твой собственный игрок!", show_alert=True)
    
    # Проверяем баланс покупателя
    c.execute('SELECT balance FROM users WHERE user_id = ?', (buyer_id,))
    buyer_res = c.fetchone()
    if not buyer_res:
        conn.close()
        return await cb.answer("Ошибка: ты не зарегистрирован!")
    
    buyer_bal = buyer_res[0]
    
    if buyer_bal < full_price:
        conn.close()
        # Показываем в алерте понятные миллионы
        return await cb.answer(f"💰 Недостаточно денег! Нужно {price_short} млн €, а у тебя {buyer_bal // 1000000} млн €.", show_alert=True)

    # СЧИТАЕМ НАЛОГ (только если продавец не система)
    tax = int(full_price * 0.10) 
    final_seller_money = full_price - tax

    try:
        # --- ФИКС БАЛАНСОВ ---
        # С покупателя списываем всегда
        c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (full_price, buyer_id))
        
        # Продавцу начисляем только если это реальный игрок (не 0)
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
            c.execute('''
                UPDATE squad 
                SET user_id = ?, original_owner_id = NULL, status = "bench", 
                    market_price = 0, slot_id = NULL, loan_expires_window = 0
                WHERE id = ?
            ''', (buyer_id, lot_id))
            deal_type = "навсегда"

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
                    f"Сумма: <b>{price_short} млн €</b>\n"
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
    viewer_id = m.from_user.id
    # Если зашли через профиль другого игрока, target_user_id будет не None
    owner_id = target_user_id if target_user_id else viewer_id
    is_owner = (viewer_id == owner_id)

    conn = get_db()
    c = conn.cursor()
    c.execute('''SELECT id, player_name, rating, pos, status, original_owner_id 
                 FROM squad 
                 WHERE user_id = ? 
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

    footer = "\n⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n🏃 — старт | 🪑 — запас | 💰 — рынок | 🎭 — аренда"
    
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
                 VALUES (0, ?, ?, ?, "free_agent", ?, 100)''', 
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
        if rat >= 90: min_p = 50
        elif rat >= 85: min_p = 30
        elif rat >= 80: min_p = 15

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
        conn.close()
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
    conn.close()

    # Формируем данные для матча
    my_players = []
    lineup_details = "" 
    
    for p in players:
        # Индексы: 0:id, 1:name, 2:rating, 3:goals, 4:assists, 5:pos, 6:stamina
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

    avg_rating = get_squad_rating(uid)
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
        "minute": 1, "tactic": "Сбалансированная",
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
    
    # --- 1. ПРОВЕРКА КД (КУЛДАУНА) ---
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT last_match FROM users WHERE user_id = ?', (uid,))
    row = c.fetchone()
    
    now = datetime.datetime.now()
    cooldown_minutes = 30 # Установи здесь сколько минут ждать (например, 30)

    if row and row[0]:
        try:
            last_match_dt = datetime.datetime.fromisoformat(row[0])
            next_match_dt = last_match_dt + datetime.timedelta(minutes=cooldown_minutes)
            
            if now < next_match_dt:
                diff = next_match_dt - now
                mins_left = int(diff.total_seconds() // 60)
                conn.close()
                return await cb.answer(f"⏳ Команда восстанавливается! Подожди {mins_left} мин.", show_alert=True)
        except ValueError:
            pass # Если формат даты в базе кривой, просто пропускаем

    # --- 2. ОБНОВЛЯЕМ ВРЕМЯ МАТЧА В БАЗЕ ---
    c.execute('UPDATE users SET last_match = ? WHERE user_id = ?', (now.isoformat(), uid))
    conn.commit(); conn.close()

    # --- ТВОЙ ОРИГИНАЛЬНЫЙ КОД БЕЗ ИЗМЕНЕНИЙ ---
    # 1. Проверяем данные в словаре
    if uid not in matches_data:
        return await cb.answer("❌ Ошибка: данные матча устарели. Нажми 'Играть' снова.", show_alert=True)

    # 2. Убираем кнопки у старого сообщения
    await cb.message.edit_reply_markup(reply_markup=None)

    # 3. Подгружаем клуб и соперника
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT club FROM users WHERE user_id = ?', (uid,))
    u_row = c.fetchone()
    my_club = u_row[0] if u_row else "Мой Клуб"
    conn.close()

    # Если соперник еще не выбран — выбираем
    if not matches_data[uid]["opp_name"]:
        opp_name = random.choice([k for k in CLUBS.keys() if k != my_club])
        matches_data[uid]["opp_name"] = opp_name
        matches_data[uid]["opp_players"] = CLUBS[opp_name]['players']

    # 4. СРАЗУ ЗАПУСКАЕМ СИМУЛЯЦИЮ
    await run_match_simulation(cb.message, uid)

async def run_match_simulation(msg, uid):
    data = matches_data[uid]
    
    # Инициализируем шансы
    current_goal_chance = 0.10
    current_card_chance = 0.12
    
    my_ovr = get_squad_rating(uid)
    opp_ovr = data.get("opp_rating", 85)
    
    tactic_mods = {"Атакующая": (1.5, 1.6), "Сбалансированная": (1.0, 1.0), "Защитная": (0.6, 0.5)}
    mod_goal, mod_miss = tactic_mods.get(data["tactic"], (1.0, 1.0))

    start_min = data["minute"]
    end_min = 45 if start_min < 45 else 90
    current_min = 5 if start_min == 1 else start_min

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

    # ДОСТАЕМ ИМЯ КЛУБА ИЗ БД (чтобы не было "Клуб")
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT club FROM users WHERE user_id = ?', (uid,))
    u_row = c.fetchone()
    club_name = u_row[0] if u_row else "Мой Клуб"
    conn.close()
    
    b = InlineKeyboardBuilder()
    b.row(
        types.InlineKeyboardButton(text="⚔️ Атака", callback_data="m_tactic_Атакующая"),
        types.InlineKeyboardButton(text="⚖️ Баланс", callback_data="m_tactic_Сбалансированная"),
        types.InlineKeyboardButton(text="🛡 Защита", callback_data="m_tactic_Защитная")
    )
    b.row(types.InlineKeyboardButton(text="🔄 Сделать замены", callback_data="sub_list"))
    b.row(types.InlineKeyboardButton(text="▶️ Продолжить матч", callback_data="continue_match"))

    text = (
        f"⚙️ <b>Управление: {club_name}</b>\n"
        f"⚽️ Счет: <b>{data['score_me']}:{data['score_opp']}</b> | ⏱ {data['minute']}'\n"
        f"Установка: <b>{data.get('tactic', 'Сбалансированная')}</b>\n\n"
        f"<i>Настройте состав и нажмите 'Продолжить'</i>"
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
        
@dp.message(F.text == "📝 Записаться в Лигу")
async def process_league_join(message: types.Message):
    uid = message.from_user.id
    
    with get_db() as conn: # Автоматически закроет соединение
        c = conn.cursor()
        
        # 1. Проверяем клуб
        c.execute('SELECT club FROM users WHERE user_id = ?', (uid,))
        user_data = c.fetchone()
        
        if not user_data or not user_data[0]:
            return await message.answer("❌ Сначала создайте клуб!")
        
        # 2. Проверяем состав (считаем только живых)
        c.execute('SELECT COUNT(*) FROM squad WHERE user_id = ? AND is_banned = 0 AND injury_remaining = 0', (uid,))
        total_players = c.fetchone()[0]
        
        if total_players < 11:
            return await message.answer(f"❌ Нужно 11 здоровых игроков! У вас: {total_players}")
        
        # 3. Запись в лигу
        try:
            c.execute('INSERT INTO league_participants (user_id) VALUES (?)', (uid,))
            conn.commit()
            await message.answer(f"🏟 <b>Заявка принята!</b>\nКлуб: <b>{user_data[0]}</b>", parse_mode="HTML")
        except sqlite3.IntegrityError:
            await message.answer("⚠️ Вы уже в списке участников.")

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
    pos_needed = parts[1] # Это будет GK, DEF, MID или FWD
    slot_idx = parts[2]
    uid = cb.from_user.id
    
    conn = get_db(); c = conn.cursor()
    
    # Делаем поиск максимально гибким:
    # 1. Приводим всё к ВЕРХНЕМУ регистру (UPPER)
    # 2. Ищем вхождение строки
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
        # Если пусто, давай выведем отладочное сообщение в алерт, чтобы понять, что видит бот
        return await cb.answer(f"❌ Нет свободных игроков для {pos_needed}.\nПроверьте, не стоят ли они уже в составе.", show_alert=True)

    b = InlineKeyboardBuilder()
    for pid, name, rat, p_pos, stam in all_subs:
        # Показываем реальную позицию из базы, например [FWD/MID/DEF]
        b.button(text=f"[{p_pos}] {name} ({rat}) 🔋{stam}%", callback_data=f"setslot_{pid}_{slot_idx}")
    
    b.adjust(1)
    b.row(types.InlineKeyboardButton(text="⬅️ К составу", callback_data="back_to_squad"))
    
    await cb.message.edit_text(
        f"📥 <b>Выбор для позиции {pos_needed}:</b>\nУниверсалы тоже в списке!", 
        reply_markup=b.as_markup(), 
        parse_mode="HTML"
    )

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
    
    c.execute('''SELECT player_name, pos, injury_remaining, is_banned, training_until 
                 FROM squad 
                 WHERE user_id = ? AND (injury_remaining > 0 OR is_banned > 0 OR training_until IS NOT NULL)''', (user_id,))
    players = c.fetchall()
    conn.close()
    
    res = "🏥 <b>МЕДИЦИНСКИЙ ЦЕНТР</b>\n————————————————————\n\n"
    
    now = datetime.datetime.now()
    training, injured, banned = [], [], []

    for p in players:
        name, pos, inj, ban, t_until = p
        if t_until:
            end = datetime.datetime.strptime(t_until, "%Y-%m-%d %H:%M:%S")
            if end > now:
                rem = end - now
                training.append(f"🏋️‍♂️ {name} ({pos}) — {rem.seconds // 3600}ч. {(rem.seconds//60)%60}м.")
        if inj > 0:
            injured.append(f"🚑 {name} ({pos}) — {inj} тур(а)")
        if ban > 0:
            banned.append(f"🟥 {name} ({pos}) — Бан")

    res += "<b>🏋️‍♂️ На тренировке:</b>\n" + ("\n".join(training) if training else "<i>— Никого</i>") + "\n\n"
    res += "<b>🚑 Травмированные:</b>\n" + ("\n".join(injured) if injured else "<i>— Пусто</i>") + "\n\n"
    res += "<b>🟥 Дисквалификации:</b>\n" + ("\n".join(banned) if banned else "<i>— Чисто</i>")
    
    await message.answer(res, parse_mode="HTML")

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
    from datetime import datetime
    
    # Обновляем баланс, голы и дату
    c.execute('''UPDATE users 
                 SET balance = balance + ?, 
                     goals_scored = goals_scored + ?, 
                     last_match = ? 
                 WHERE user_id = ?''', 
              (reward, score_me, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), uid))
    
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

@dp.callback_query(F.data.startswith("st_"))
async def player_stats_callback(cb: types.CallbackQuery):
    action = cb.data.split("_")[1]
    uid = cb.from_user.id
    conn = get_db(); c = conn.cursor()
    if action == "goals":
        c.execute('SELECT player_name, goals FROM squad WHERE user_id = ? AND goals > 0 ORDER BY goals DESC LIMIT 10', (uid,))
        title = "⚽️ <b>Топ бомбардиров:</b>"
    elif action == "assists":
        c.execute('SELECT player_name, assists FROM squad WHERE user_id = ? AND assists > 0 ORDER BY assists DESC LIMIT 10', (uid,))
        title = "🅰️ <b>Топ ассистентов:</b>"
    elif action == "yellow":
        c.execute('SELECT player_name, yellow_cards FROM squad WHERE user_id = ? AND yellow_cards > 0 ORDER BY yellow_cards DESC', (uid,))
        title = "🟨 <b>Желтые карточки:</b>"
    elif action == "red":
        c.execute('SELECT player_name FROM squad WHERE user_id = ? AND is_banned = 1', (uid,))
        title = "🟥 <b>Красные (в бане):</b>"
    data = c.fetchall(); conn.close()
    if not data: return await cb.answer("Статистики пока нет!", show_alert=True)
    res_text = f"{title}\n\n"
    for i, row in enumerate(data, 1):
        val = row[1] if len(row) > 1 else "В бане"
        res_text += f"{i}. {row[0]} — {val}\n"
    await cb.message.answer(res_text, parse_mode="HTML")
    await cb.answer()

@dp.message(F.text == "🏆 Таблица")
async def show_leaderboard(m: types.Message):
    conn = get_db(); c = conn.cursor()
    
    c.execute('SELECT COUNT(*) FROM league_schedule')
    has_league = c.fetchone()[0]
    
    if has_league == 0:
        conn.close()
        return await m.answer(
            "🏆 <b>ТУРНИРНАЯ ТАБЛИЦА</b>\n"
            "————————————————————\n"
            "⏳ Сезон завершен или еще не начат!\n"
            "Ждите объявления нового набора в Лигу.", 
            parse_mode="HTML"
        )

    # Запрос по лиговым колонкам
    c.execute('''
        SELECT club, league_wins, league_draws, league_losses, league_goals,
               (league_wins + league_draws + league_losses) as played,
               (league_wins * 3 + league_draws) as pts 
        FROM users 
        WHERE (league_wins + league_draws + league_losses) > 0
        ORDER BY pts DESC, league_goals DESC LIMIT 15
    ''')
    
    rows = c.fetchall()
    conn.close()

    if not rows:
        return await m.answer("🏆 Лига готова к старту!\nПервые матчи тура еще не сыграны.")

    text = "🏆 <b>ТУРНИРНАЯ ТАБЛИЦА (ЛИГА)</b>\n"
    text += "<code> №  Клуб         И  В-Н-П  Г   О</code>\n"
    text += "<code>——————————————————————————————</code>\n"

    for i, (club, w, d, l, gs, pld, pts) in enumerate(rows, 1):
        club_name = (club[:10] + '..') if len(club) > 10 else club.ljust(12)
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i:<2}"
        text += f"<code>{medal} {club_name} {pld:<2} {w}-{d}-{l}  {gs:<3} {pts}</code>\n"

    text += "<code>——————————————————————————————</code>\n"
    text += "<i>Обновлено после завершения тура</i>"
    await m.answer(text, parse_mode="HTML")

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
    b.button(text="📅 Сменить полугодие", callback_data="next_half_season")
    b.button(text="🚫 Выгнать", callback_data="admin_kick_user")
    b.button(text="💰 Выдать монеты", callback_data="admin_give_money")
    b.button(text="🏆 Начать Лигу (Генерация)", callback_data="admin_league_start")
    b.button(text="⚽️ Провести ТУР", callback_data="admin_league_run_tour")
    b.button(text="📰 Выпустить газету", callback_data="admin_post_news")
    b.button(text="👞 Исключить из клуба", callback_data="admin_kick_club")
    b.button(text="💎 Апгрейд рейтинга", callback_data="admin_upgrade_start")
    b.button(text="🆕 Начать Кубок (20 команд)", callback_data="admin_init_cup")
    b.button(text="⚽️ Запустить тур Кубка", callback_data="run_cup_stage")
    b.button(text="🏆 Провести ФИНАЛ", callback_data="run_cup_final")
    b.button(text="🚀 Выбросить ТОП-игрока", callback_data="admin_drop_player")
    b.button(text="📢 Сделать рассылку", callback_data="start_broadcast")
    b.button(text="🏁 Завершить сезон и выдать 50кк", callback_data="admin_finish_season")
    b.button(text="🧨 ПОЛНЫЙ СБРОС БАЗЫ", callback_data="admin_full_reset")
    b.adjust(1)
    await m.answer("🔧 Админ-панель:", reply_markup=b.as_markup())

@dp.callback_query(F.data == "run_cup_stage")
async def run_cup_stage(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    
    # 1. Находим текущую несыгранную стадию
    c.execute("SELECT stage FROM cup_bracket WHERE winner_id IS NULL LIMIT 1")
    row = c.fetchone()
    if not row: 
        conn.close()
        return await cb.answer("Все матчи текущих стадий сыграны!", show_alert=True)
    
    current_stage = row[0]
    
    # 2. Берем все матчи этой стадии
    c.execute("SELECT id, t1_id, t2_id, t1_name, t2_name, first_leg_score FROM cup_bracket WHERE stage = ? AND winner_id IS NULL", (current_stage,))
    matches = c.fetchall()

    report = f"⚽️ <b>РЕЗУЛЬТАТЫ КУБКА: {current_stage}</b>\n"
    report += "————————————————————\n"

    for m_id, t1_id, t2_id, t1_n, t2_n, fl_score in matches:
        prev_score = (0, 0)
        is_second_leg = False
        
        # ЛОГИКА ДВУХ МАТЧЕЙ (только для 1/2)
        if current_stage == '1/2':
            if fl_score is None:
                # ПЕРВЫЙ МАТЧ: играем 90 мин, без ОТ и пенальти
                res = await play_cup_match_full(t1_id, t2_id, t1_n, t2_n, cb.bot, use_extra_time=False)
                score_text = f"{res['h_s']}:{res['a_s']}"
                c.execute("UPDATE cup_bracket SET first_leg_score = ? WHERE id = ?", (score_text, m_id))
                report += f"🔹 {t1_n} <b>{score_text}</b> {t2_n} (Первый матч)\n"
                continue # Победителя не определяем, ждем ответку
            else:
                # ВТОРОЙ МАТЧ: учитываем счет первого
                h_p, a_p = map(int, fl_score.split(':'))
                prev_score = (h_p, a_p) # Передаем в движок для суммы
                is_second_leg = True

        # ЗАПУСК ДВИЖКА
        res = await play_cup_match_full(t1_id, t2_id, t1_n, t2_n, cb.bot, prev_score=prev_score)
        
        # Считаем общий итог для определения победителя
        total_h = res['h_s'] + prev_score[0]
        total_a = res['a_s'] + prev_score[1]
        
        # Кто прошел дальше? (учитываем пенальти если была ничья по сумме)
        winner_id = t1_id if (total_h + (res['h_p'] or 0)) > (total_a + (res['a_p'] or 0)) else t2_id
        
        c.execute('''UPDATE cup_bracket SET winner_id = ?, h_score = ?, a_score = ?, h_pen = ?, a_pen = ? 
                     WHERE id = ?''', (winner_id, res['h_s'], res['a_s'], res['h_p'], res['a_p'], m_id))
        
        # Формируем строку для отчета
        match_res = f"{res['h_s']}:{res['a_s']}"
        if is_second_leg: match_res += f" (Общ. {total_h}:{total_a})"
        if res['h_p'] is not None: match_res += f" [п. {res['h_p']}:{res['a_p']}]"
        
        report += f"✅ {t1_n} <b>{match_res}</b> {t2_n}\n"

    conn.commit()

    # 3. АВТОМАТИЧЕСКАЯ ГЕНЕРАЦИЯ СЛЕДУЮЩЕЙ СТАДИИ
    # Проверяем, все ли матчи ТЕКУЩЕЙ стадии завершены
    c.execute("SELECT COUNT(*) FROM cup_bracket WHERE stage = ? AND winner_id IS NULL", (current_stage,))
    remaining = c.fetchone()[0]

    if remaining == 0:
        c.execute("SELECT winner_id FROM cup_bracket WHERE stage = ?", (current_stage,))
        winners = [r[0] for r in c.fetchall()]
        
        next_stage = None
        if current_stage == 'Play-In':
            # 4 победителя + 12 топ-команд = 16 команд (1/8)
            c.execute("SELECT user_id, club FROM users ORDER BY (wins*3 + draws) DESC LIMIT 12")
            top_12 = c.fetchall()
            all_1_8 = []
            for w_id in winners:
                c.execute("SELECT user_id, club FROM users WHERE user_id = ?", (w_id,))
                all_1_8.append(c.fetchone())
            all_1_8.extend(top_12)
            random.shuffle(all_1_8)
            next_stage = '1/8'
            for i in range(0, 16, 2):
                c.execute("INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) VALUES (?, ?, ?, ?, ?)",
                          (next_stage, all_1_8[i][0], all_1_8[i][1], all_1_8[i+1][0], all_1_8[i+1][1]))
        
        elif current_stage in ['1/8', '1/4']:
            # 8 -> 4 или 4 -> 2
            next_stage = '1/4' if current_stage == '1/8' else '1/2'
            next_teams = []
            for w_id in winners:
                c.execute("SELECT user_id, club FROM users WHERE user_id = ?", (w_id,))
                next_teams.append(c.fetchone())
            random.shuffle(next_teams)
            for i in range(0, len(next_teams), 2):
                c.execute("INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) VALUES (?, ?, ?, ?, ?)",
                          (next_stage, next_teams[i][0], next_teams[i][1], next_teams[i+1][0], next_teams[i+1][1]))
        
        elif current_stage == '1/2':
            # 2 победителя -> Финал
            next_stage = 'Final'
            f_teams = []
            for w_id in winners:
                c.execute("SELECT user_id, club FROM users WHERE user_id = ?", (w_id,))
                f_teams.append(c.fetchone())
            c.execute("INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) VALUES (?, ?, ?, ?, ?)",
                      (next_stage, f_teams[0][0], f_teams[0][1], f_teams[1][0], f_teams[1][1]))

        if next_stage:
            report += f"\n🚀 <b>Стадия {next_stage} сформирована!</b>"

    conn.commit()
    conn.close()
    await cb.message.answer(report, parse_mode="HTML")

@dp.callback_query(F.data == "run_cup_final")
async def run_cup_final(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    c.execute("SELECT id, t1_id, t2_id, t1_name, t2_name FROM cup_bracket WHERE stage = 'Final' AND winner_id IS NULL")
    f = c.fetchone()
    if not f: return await cb.answer("Финал не найден или уже сыгран!")

    match_id, t1_id, t2_id, t1_n, t2_n = f
    res = await play_cup_match_full(t1_id, t2_id, t1_n, t2_n, cb.bot)
    
    win_id = t1_id if (res['h_s'] + (res['h_p'] or 0)) > (res['a_s'] + (res['a_p'] or 0)) else t2_id
    win_n = t1_n if win_id == t1_id else t2_n

    # Начисляем приз 20,000,000 €
    c.execute("UPDATE users SET balance = balance + 20000000 WHERE user_id = ?", (win_id,))
    c.execute("UPDATE cup_bracket SET winner_id = ?, h_score=?, a_score=?, h_pen=?, a_pen=? WHERE id=?", 
              (win_id, res['h_s'], res['a_s'], res['h_p'], res['a_p'], match_id))
    
    conn.commit(); conn.close()
    
    await cb.message.answer(f"🏆 <b>ФИНАЛ ЗАВЕРШЕН!</b>\nПобедитель: {win_n}\n💰 Приз 20кк выдан!", parse_mode="HTML")
    try: await cb.bot.send_message(win_id, "🏆 ТЫ ВЫИГРАЛ КУБОК! 20,000,000 € твои!")
    except: pass

@dp.callback_query(F.data == "admin_init_cup")
async def admin_init_cup(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: return
    
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id, club FROM users ORDER BY (wins*3 + draws) DESC')
    teams = c.fetchall()
    
    if len(teams) < 20:
        return await cb.answer(f"Нужно 20 команд! (У нас {len(teams)})", show_alert=True)

    c.execute('DELETE FROM cup_bracket') # Сброс старой сетки
    
    # 8 команд для Плей-ин (с 13-го по 20-е место)
    pi_pool = teams[12:]
    for i in range(0, 8, 2):
        c.execute('''INSERT INTO cup_bracket (stage, t1_id, t1_name, t2_id, t2_name) 
                     VALUES ('Play-In', ?, ?, ?, ?)''', 
                  (pi_pool[i][0], pi_pool[i][1], pi_pool[i+1][0], pi_pool[i+1][1]))
    
    conn.commit(); conn.close()
    await cb.message.answer("🏆 <b>Кубок инициализирован!</b>\nПары Плей-ин созданы. Когда они сыграют, победители попадут в 1/8 к топ-12 командам.", parse_mode="HTML")



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

def get_random_club(all_clubs):
    # Просто выбирает случайный клуб из списка, который ты ей дашь
    return random.choice(all_clubs) if all_clubs else "Неизвестный клуб"

@dp.callback_query(F.data == "admin_finish_season")
async def callback_finish_season(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: 
        return await cb.answer("Доступ закрыт!", show_alert=True)

    conn = get_db(); c = conn.cursor()

    # 1. Находим чемпиона (учитываем очки и голы)
    c.execute('''
        SELECT user_id, club, (wins * 3 + draws) as pts, goals_scored 
        FROM users 
        WHERE (wins + draws + losses) > 0
        ORDER BY pts DESC, goals_scored DESC 
        LIMIT 1
    ''')
    winner = c.fetchone()

    if not winner:
        conn.close()
        return await cb.message.answer("❌ Нет данных для завершения сезона (никто не играл).")

    w_id, w_club, w_pts, w_gs = winner
    prize = 50_000_000

    # 2. Начисляем приз и сбрасываем статистику лиги
    c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (prize, w_id))
    c.execute('UPDATE users SET wins=0, draws=0, losses=0, goals_scored=0')
    c.execute('DELETE FROM league_schedule')

    conn.commit(); conn.close()

    # 3. Отправляем ЛИЧНОЕ СООБЩЕНИЕ чемпиону
    congrats_text = (
        f"🏆 <b>ПОЗДРАВЛЯЕМ! ВЫ ВЫИГРАЛИ ЛИГУ!</b> 🏆\n"
        f"————————————————————\n"
        f"Ваш клуб <b>{w_club}</b> занял 1-е место в таблице.\n"
        f"💰 Вам начислено: <b>50,000,000 €</b> призовых!\n"
        f"📈 Итог сезона: {w_pts} очков.\n"
        f"————————————————————\n"
        f"Удачи в следующем сезоне!"
    )
    
    try:
        await cb.bot.send_message(w_id, congrats_text, parse_mode="HTML")
    except Exception as e:
        print(f"Не удалось отправить приз чемпиону {w_id}: {e}")

    # 4. Общий отчет для админа
    final_text = (
        f"🏆 <b>СЕЗОН ЗАКРЫТ!</b>\n\n"
        f"🥇 Чемпион: <b>{w_club}</b>\n"
        f"💵 Приз 50кк выдан (ID: {w_id})\n"
        f"⚙️ Статистика и расписание обнулены."
    )

    await cb.message.answer(final_text, parse_mode="HTML")
    await cb.answer("Приз выдан, чемпион уведомлен!")

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
        f"🎊 <b>СЕЗОН ОФИЦИАЛЬНО ЗАВЕРШЕН!</b> 🎊\n"
        f"————————————————————\n"
        f"🥇 Чемпион Лиги: <b>{w_club}</b>\n"
        f"📊 Очки: <b>{w_pts}</b> | Награда: <b>50,000,000 €</b>\n"
        f"{cup_report}"
        f"————————————————————\n"
        f"🚀 Вся статистика обнулена. Ждем вас в новом сезоне!"
    )
    
    await m.answer(final_text, parse_mode="HTML")
    
    try:
        await m.bot.send_message(w_id, f"🏆 <b>ПОЗДРАВЛЯЕМ!</b>\nВаш клуб {w_club} выиграл Лигу! 50,000,000 € зачислены!")
    except: pass

async def generate_daily_news():
    conn = get_db(); c = conn.cursor()

    c.execute('SELECT DISTINCT club FROM users WHERE club IS NOT NULL')
    db_clubs = [row[0] for row in c.fetchall()]
    
    all_clubs = list(set(db_clubs + list(CLUBS.keys())))
                     
    c1 = all_clubs[0] if len(all_clubs) > 0 else "Клуб А"
    c2 = all_clubs[1] if len(all_clubs) > 1 else "Клуб Б"
    c3 = all_clubs[2] if len(all_clubs) > 2 else "Клуб В"
    c4 = all_clubs[3] if len(all_clubs) > 3 else "Клуб Г"
    c5 = all_clubs[4] if len(all_clubs) > 4 else "Клуб Д"
    c6 = all_clubs[5] if len(all_clubs) > 5 else "Клуб Е"
    c7 = all_clubs[6] if len(all_clubs) > 6 else "Клуб Ж"
    c8 = all_clubs[7] if len(all_clubs) > 7 else "Клуб З"
    c9 = all_clubs[8] if len(all_clubs) > 8 else "Клуб И"
    c10 = all_clubs[9] if len(all_clubs) > 9 else "Клуб К"
    c11 = all_clubs[10] if len(all_clubs) > 10 else "Клуб Л"
    c12 = all_clubs[11] if len(all_clubs) > 11 else "Клуб М"
    c13 = all_clubs[12] if len(all_clubs) > 12 else "Клуб Н"
    c14 = all_clubs[13] if len(all_clubs) > 13 else "Клуб О"
    c15 = all_clubs[14] if len(all_clubs) > 14 else "Клуб П"
    c16 = all_clubs[15] if len(all_clubs) > 15 else "Клуб Р"
    c17 = all_clubs[16] if len(all_clubs) > 16 else "Клуб С"
    c18 = all_clubs[17] if len(all_clubs) > 17 else "Клуб Т"
    c19 = all_clubs[18] if len(all_clubs) > 18 else "Клуб У"
    c20 = all_clubs[19] if len(all_clubs) > 19 else "Клуб Ф"

    # 1. СБОР ДАННЫХ (Берем топ-5 для рандома внутри категорий)
    c.execute('SELECT s.player_name, s.goals, u.club FROM squad s JOIN users u ON s.user_id = u.user_id WHERE s.goals > 0 ORDER BY s.goals DESC LIMIT 5')
    scorers = c.fetchall()
    
    c.execute('SELECT s.player_name, s.assists, u.club FROM squad s JOIN users u ON s.user_id = u.user_id WHERE s.assists > 0 ORDER BY s.assists DESC LIMIT 5')
    assisters = c.fetchall()
    
    c.execute('SELECT s.player_name, s.yellow_cards, s.red_cards, u.club FROM squad s JOIN users u ON s.user_id = u.user_id WHERE (s.yellow_cards > 0 OR s.red_cards > 0) ORDER BY (s.red_cards * 3 + s.yellow_cards) DESC LIMIT 5')
    bad_boys = c.fetchall()
    
    c.execute('SELECT club, losses FROM users WHERE losses > 0 ORDER BY losses DESC LIMIT 5')
    losers = c.fetchall()
    
    # Берем трансферы ТОЛЬКО там, где клуб НЕ None
    c.execute('''SELECT s.player_name, s.market_price, u.club 
                 FROM squad s JOIN users u ON s.user_id = u.user_id 
                 WHERE s.market_price > 0 ORDER BY s.id DESC LIMIT 5''')
    deals = c.fetchall()
    conn.close()

    slogan = random.choice([
    "🗞 <b>Твой инсайд в мире голов.</b>",
    "🗞 <b>Не читал — считай, пропустил пенальти!</b>",
    "🗞 <b>Твой клуб. Твоя лига. Твоя история.</b>"
    ])

    mandatory_blocks = [] # Тут будут Голы и Трансферы
    random_pool = []      # Тут всё остальное (Ассисты, Костоломы, Лузеры, Слухи)

    club_names = list(CLUBS.keys())
    rand_club = random.choice(club_names)
    rand_club_2 = random.choice([c for c in club_names if c != rand_club])


    # Выбираем одну случайную цитату Шнякина
    expert_quote = random.choice([
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «Главное в матче <b>«{c1}»</b> — чтобы не выключили свет на стадионе. Остальное — нюансы!»",
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «Я внимательно изучил <b>«{c3}»</b>. Мой вердикт: если они забьют больше соперника, то точно не проиграют. Скриньте!» 📈",
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «В <b>«{c6}»</b> сейчас такая атмосфера, что даже мяч не хочет залетать в ворота. Я бы поставил на ничью, но боюсь проиграть свои последние 100 рублей». 📉",
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «Видел я тренировку <b>«{c2}»</b>... Там нападающий попал по мячу с первого раза. Это либо знак свыше, либо случайность. Ждем тур!» 🔮",
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «Игрокам <b>«{c10}»</b> нужно просто выйти на поле и сыграть в футбол. Если они выйдут играть в домино — шансов будет меньше. Записывайте!» ✍️",
        f"🎙 <b>ЭКСПЕРТ ШНЯКИН:</b> «По моим данным, <b>«{c7}»</b> сегодня выберет тактику 'бей-беги'. Куда бить и куда бежать — решат уже по ходу матча. Гениально!» 🧠"
    ])

    # --- ОБЯЗАТЕЛЬНО: ГОЛЫ ---
    if scorers:
        p = random.choice(scorers)
        mandatory_blocks.append(random.choice([
            f"🚀 <b>ГОЛЕВАЯ ФЕЕРИЯ!</b>\nПохоже, <b>{p[0]}</b> («{p[2]}») нашел чит-коды. Его {p[1]}-й гол заставляет фанатов визжать! ⚽️",
            f"🔥 <b>БЕЗЖАЛОСТНЫЙ КИЛЛЕР!</b>\nФорвард <b>{p[0]}</b> из «{p[2]}» снова дырявит сетку. {p[1]} мячей — это приговор!",
            f"⚡️ <b>ГРОЗА ВРАТАРЕЙ!</b>\nВ штрафной «{p[2]}» жарко, когда там <b>{p[0]}</b>. На его счету уже {p[1]} выстрелов!"
        ]))
    else:
        mandatory_blocks.append("👟 <b>ЗАТИШЬЕ НА ОСТРИЕ:</b> Нападающие сегодня забыли бутсы дома. Ни одного гола в туре! 🤔")

    # --- ОБЯЗАТЕЛЬНО: ТРАНСФЕРЫ (Без None!) ---
    if deals:
        d = random.choice(deals)
        mandatory_blocks.append(random.choice([
            f"💰 <b>ДЕНЕЖНЫЙ ДОЖДЬ!</b>\nРынок вздрогнул: <b>{d[0]}</b> перешел в <b>{d[2]}</b> за <b>{d[1]} млн €</b>. Деньги не пахнут! 💸",
            f"💣 <b>ТРАНСФЕРНАЯ БОМБА!</b>\nНикто не ждал, но <b>{d[0]}</b> теперь в «{d[2]}». Цена вопроса — <b>{d[1]} млн €</b>!",
            f"🤝 <b>НОВАЯ ПРОПИСКА!</b>\n<b>{d[0]}</b> сменил форму на цвета «{d[2]}». Сделка потянула на <b>{d[1]} млн €</b>."
        ]))
    else:
        mandatory_blocks.append(random.choice([
            "🏢 <b>ТРАНСФЕРНОЕ ЗАТИШЬЕ:</b> Скауты затаились, трансферный рынок сегодня спит. 😴",
            "🏖 <b>РЫНОК В ОТПУСКЕ:</b> Агенты уехали на острова. Громких переходов пока не ждите!"
        ]))

    # --- БЛОК: КОСТОЛОМЫ ---
    if bad_boys:
        b = random.choice(bad_boys) # b[0]-имя, b[1]-ЖК, b[2]-КК, b[3]-клуб
        mandatory_blocks.append(random.choice([
            f"🟥 <b>КРАСНАЯ ЗОНА!</b>\n<b>{b[0]}</b> (<b>«{b[3]}»</b>) перепутал футбол с регби. Коллекция из {b[1]} ЖК и {b[2]} КК намекает: парню пора в секцию бокса! 👺",
            f"⚔️ <b>СТАЛЬНЫЕ ПОДКАТЫ!</b>\nЗащитник <b>{b[0]}</b> не знает пощады. Ноги соперников для него — лишь препятствие. {b[1]} горчичников — это серьезная заявка на титул грубияна <b>«{b[3]}»</b>! 👊",
            f"🚨 <b>ОСТОРОЖНО, ГРУБОСТЬ!</b>\nВстреча с <b>{b[0]}</b> гарантирует синяки. Игрок клуба <b>«{b[3]}»</b> играет на грани, и судья уже зажег перед ним свет! 🛑"
        ]))
    else:
        mandatory_blocks.append("🤝 <b>ДЖЕНТЛЬМЕНСКИЙ ТУР:</b> На поле царит мир и взаимоуважение. Костоломы сегодня взяли выходной! ✨")

    # --- БЛОК: НЕУДАЧНИКИ ---
    if losers:
        l = random.choice(losers) # l[0]-клуб, l[1]-поражения
        mandatory_blocks.append(random.choice([
            f"📉 <b>КРИЗИС В РАЗДЕВАЛКЕ!</b>\nУ фанатов <b>«{l[0]}»</b> закончился валидол. Очередное поражение (уже {l[1]}-е) заставляет задуматься: а не пора ли менять тренера? 🤕",
            f"🥀 <b>ЧЕРНАЯ ПОЛОСА!</b>\nКлуб <b>«{l[0]}»</b> никак не найдет свою игру. {l[1]} проигрышей висят над командой тяжелым грузом. Болельщики в трауре... 🏴‍☠️",
            f"🆘 <b>SOS ДЛЯ КОМАНДЫ!</b>\nСтатистика <b>«{l[0]}»</b> пугает: {l[1]} поражений в сезоне. Пока другие празднуют, эти ребята разбирают ошибки у разбитого корыта! 🏚"
        ]))
    else:
        mandatory_blocks.append("📈 <b>БИТВА ТИТАНОВ:</b> В лиге не осталось явных аутсайдеров. Каждый зубами вырывает очки у соперника! 💪")

    # Живые филлеры (если данных мало, они спасают)
    random_pool.append("🏟 <b>АНШЛАГ!</b> Стадионы забиты, пиво льется рекой, а фанаты поют громче сирен! 📣")
    random_pool.append("⚠️ <b>СЛУХИ:</b> Говорят, админ готовит новый турнир с жирными призами. Копите силы! 🔥")
    random_pool.append(f"🌭 <b>НОВОСТИ КЕЙТЕРИНГА:</b> Сосиски на стадионе <b>«{c1}»</b> признаны самыми вкусными в лиге. Жрем! 🌭")
    random_pool.append(f"🏟 <b>СКАНДАЛ НА ТРИБУНАХ:</b> Фанаты клуба <b>«{c2}»</b> устроили невероятный перфоманс. Весь стадион в дыму! 🔥")
    random_pool.append(f"🎤 <b>ИНСАЙД:</b> Тренер <b>«{c3}»</b> в ярости. Говорят, игроки слишком расслабились перед выездом к <b>«{c4}»</b>. 🤬")
    random_pool.append(f"🚑 <b>МЕДИЦИНСКИЙ ШТАБ:</b> Врачи <b>«{c5}»</b> творят чудеса! Лидеры команды восстановились в рекордные сроки. 💊")
    random_pool.append(f"⭐ <b>НОВАЯ ЗВЕЗДА:</b> В молодежке <b>«{c6}»</b> подрастает новый Мбаппе. Цена уже взлетела до небес! 📈")
    random_pool.append(f"🤝 <b>ТОВАРИЩЕСКИЙ УЖИН:</b> Владельцы <b>«{c7}»</b> и <b>«{c8}»</b> были замечены в элитном ресторане. Обмен? 🤔")
    random_pool.append(f"🍺 <b>ПИВНОЙ СКАНДАЛ:</b> На стадионе <b>«{c9}»</b> фанаты выпили годовой запас пенного за первый тайм! 🍺")
    random_pool.append(f"🏠 <b>ЖИЛИЩНЫЙ ВОПРОС:</b> Клуб <b>«{c10}»</b> выставил на трансфер вратаря за пропущенный ипотечный платеж! 💸")
    random_pool.append(f"🐐 <b>АГРО-НОВОСТИ:</b> На поле клуба <b>«{c11}»</b> ночью паслись козы. Пасуются лучше защитников! 🐐")
    random_pool.append(f"🕺 <b>ДИСКО-БОЛ:</b> Игроков <b>«{c12}»</b> заметили в ночном клубе. Отрабатывали финты на танцполе! 💃")
    random_pool.append(f"🕶 <b>ЗРЕНИЕ ПРОВЕРЕНО:</b> Фанаты <b>«{c13}»</b> скинулись судье на операцию по коррекции зрения. Доброта! 👓")
    random_pool.append(f"🧦 <b>ПРОКЛЯТЫЕ ГЕТРЫ:</b> Игроки клуба <b>«{c14}»</b> вышли на поле в разных носках «на удачу». 🩹")
    random_pool.append(f"🍕 <b>ДИЕТА ЧЕМПИОНОВ:</b> Тренер <b>«{c15}»</b> застукал нападающих в бургерной. «Углеводная загрузка»! 🍔")
    random_pool.append(f"🚜 <b>АГРО-ФИТНЕС:</b> На базе <b>«{c16}»</b> игроки дубля стригли траву ножницами. Вот это преданность! ✂️")
    random_pool.append(f"📢 <b>ГОРЛОПАНЫ НЕДЕЛИ:</b> Фанаты <b>«{c17}»</b> пели так громко, что на соседней стройке рухнул забор! 🏗")
    random_pool.append(f"👓 <b>АКЦИЯ ДОБРОТЫ:</b> Клуб <b>«{c18}»</b> подарил судье собаку-поводыря. Арбитр не оценил, а трибуны — да! 🐕")
    random_pool.append(f"🧙‍♂️ <b>МАГИЯ:</b> Шаман <b>«{c19}»</b> побрызгал штанги святой водой. Штанги чистые, но мячи всё там же! ✨")
    random_pool.append(f"🚌 <b>АВТОБУСНЫЙ ПАРК:</b> Тактика «10 защитников» от <b>«{c20}»</b> признана самой скучной в истории! 😴")
    random_pool.append(f"🗿 <b>ДЗЕН-ФУТБОЛ:</b> Тренер <b>«{c1}»</b> заставил игроков медитировать на штангу. Вратарь познал дзен! 🧘‍♂️")
    random_pool.append(f"📦 <b>VAR НА МИНИМАЛКАХ:</b> В клубе <b>«{c2}»</b> повторы смотрят на телефоне охранника. Ничего не понятно! 📱")
    random_pool.append(f"🦖 <b>ПАРК ПЕРИОДА ЛИГИ:</b> Фанаты <b>«{c3}»</b> вывели на поле надувного динозавра. 😱")
    random_pool.append(f"🧺 <b>БЮДЖЕТНАЯ СТИРКА:</b> В <b>«{c4}»</b> форму стирают в фонтане. Запах свежести сбивает врагов! 🧼")
    random_pool.append(f"🐈 <b>ЧЕРНЫЙ СПИСОК:</b> Автобус <b>«{c5}»</b> официально ездит кругами, чтобы не встретить кота! 🚌")

    # Выбираем 2 случайных из пула
    random.shuffle(random_pool)
    selected_random = random_pool[:2]

    # Склеиваем обязательные и рандомные
    final_blocks = mandatory_blocks + selected_random
    random.shuffle(final_blocks) # Перемешиваем сами блоки в газете

    # Оформление
    header = random.choice(["🗞 <b>FOOTBALL DAILY</b>", "⚽️ <b>ВЕСТНИК ЛИГИ</b>", "🏟 <b>СТАДИОННЫЙ КУРЬЕР</b>"])
    date_str = datetime.datetime.now().strftime("%d.%m.%Y")
    
    final_report = f"{header}\n<i>Выпуск от {date_str}</i>\n" + ("—" * 20) + "\n\n"
    final_report += "\n\n".join(final_blocks)
    final_report += f"\n\n————————————————\n{expert_quote}"
    final_report += f"\n\n————————————————\n{slogan}"

    return final_report

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
    await state.set_state("waiting_for_money_amount")

# Шаг 3: Применение
@dp.message(F.state == "waiting_for_money_amount")
async def apply_money(m: types.Message, state: FSMContext):
    if not m.text.lstrip('-').isdigit():
        return await m.answer("❌ Введите число!")
    
    amount = int(m.text)
    data = await state.get_data()
    target_id = data.get('target_uid')
    
    if not target_id: return await state.clear()

    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, target_id))
    conn.commit(); conn.close()
    
    await m.answer(f"✅ Пользователю {target_id} начислено {amount} млн €.")
    await state.clear()

# 2. Снятие пользователя с клуба (обнуление состава без удаления юзера)
@dp.callback_query(F.data == "admin_kick_club")
async def pre_kick_club(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.answer("Введите ID пользователя, которого нужно исключить из клуба:")
    await state.set_state("waiting_for_kick_id")

@dp.callback_query(F.data == "admin_league_run_tour")
async def run_league_tour(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: 
        return await cb.answer("Только для админов!", show_alert=True)
    
    conn = get_db(); c = conn.cursor()
    
    # Узнаем номер текущего тура (берем минимальный из несыгранных)
    c.execute('SELECT MIN(tour_number) FROM league_schedule WHERE status = "pending"')
    current_tour = c.fetchone()[0]
    
    if current_tour is None:
        conn.close(); return await cb.message.answer("🎉 Все туры сезона завершены!")

    # Выбираем матчи ТОЛЬКО текущего тура
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
        # АВТО-КИК забаненных и травмированных
        for uid in [h_id, a_id]:
            c.execute('''UPDATE squad SET slot_id = NULL, status = "bench" 
                         WHERE user_id = ? AND slot_id IS NOT NULL 
                         AND (is_banned = 1 OR injury_remaining > 0)''', (uid,))
        conn.commit()

        # 1. СЧИТАЕМ ТОЛЬКО "ЛЕГИТИМНЫХ" ИГРОКОВ (Здоровых, не забаненных, в слотах)
        c.execute('''
            SELECT COUNT(*) FROM squad 
            WHERE user_id = ? 
            AND slot_id IS NOT NULL 
            AND is_banned = 0 
            AND injury_remaining = 0
        ''', (h_id,))
        h_count = c.fetchone()[0]
        
        c.execute('''
            SELECT COUNT(*) FROM squad 
            WHERE user_id = ? 
            AND slot_id IS NOT NULL 
            AND is_banned = 0 
            AND injury_remaining = 0
        ''', (a_id,))
        a_count = c.fetchone()[0]

        # 2. ЖЕСТКИЙ ТЕХНАРЬ
        if h_count < 11 or a_count < 11:
            if h_count < 11 and a_count < 11:
                h_res, a_res = 0, 0
                c.execute('UPDATE users SET league_losses=league_losses+1 WHERE user_id IN (?,?)', (h_id, a_id))
            elif h_count < 11:
                h_res, a_res = 0, 3
                c.execute('UPDATE users SET league_wins=league_wins+1, league_goals=league_goals+3 WHERE user_id=?', (a_id,))
                c.execute('UPDATE users SET league_losses=league_losses+1 WHERE user_id=?', (h_id,))
            else:
                h_res, a_res = 3, 0
                c.execute('UPDATE users SET league_wins=league_wins+1, league_goals=league_goals+3 WHERE user_id=?', (h_id,))
                reason = f"Некомплект у {a_club} ({a_count}/11)"
                c.execute('UPDATE users SET league_losses=league_losses+1 WHERE user_id=?', (a_id,))

            # Закрываем матч в базе
            c.execute('UPDATE league_schedule SET status = "finished" WHERE id = ?', (m_id,))
            conn.commit()
            tech_msg = (f"🏟 <b>ТЕХНИЧЕСКИЙ РЕЗУЛЬТАТ</b>\n\n"
                        f"⚔️ <b>{h_club}</b> {h_res}:{a_res} <b>{a_club}</b>\n"
                        f"————————————————————\n"
                        f"❌ {reason}")
            
            for user_id in [h_id, a_id]:
                try: await bot.send_message(user_id, tech_msg, parse_mode="HTML")
                except: pass

            final_report += (
                f"<b>{h_club}</b> 🆚 <b>{a_club}</b>\n"
                f"      ⚽️  <b>{h_res} : {a_res}</b>\n"
                f"❌ <b>Техническое поражение!</b>\n"
                f"ℹ️ {reason}\n🏁 ————————————————————\n\n"
            )
            continue # Пропускаем симуляцию матча

        h_ovr = get_squad_rating(h_id) 
        a_ovr = get_squad_rating(a_id)
        
        h_f = FORMATION_MODS.get(h_form, {"atk": 1.0, "def": 1.0})
        a_f = FORMATION_MODS.get(a_form, {"atk": 1.0, "def": 1.0})

        h_chance = (0.15 + (h_ovr - a_ovr) / 250) * h_f["atk"] / a_f["def"]
        a_chance = (0.14 + (a_ovr - h_ovr) / 250) * a_f["atk"] / h_f["def"]

        c.execute('SELECT id, player_name, pos, stamina FROM squad WHERE user_id = ? AND slot_id IS NOT NULL', (h_id,))
        h_players = c.fetchall()
        c.execute('SELECT id, player_name, pos, stamina FROM squad WHERE user_id = ? AND slot_id IS NOT NULL', (a_id,))
        a_players = c.fetchall()

        h_score, a_score = 0, 0
        match_events = []
        

        for _ in range(8):
            minute = random.randint(1, 90)
            roll = random.random()
            
            if roll < h_chance:
                h_score += 1
                p = random.choice([p for p in h_players if p[2] != 'GK'])
                assister = get_weighted_assister(h_players, p[0]) 
                
                event_txt = f"⚽️ {minute}' Гол! {p[1]} ({h_club})"
                

                c.execute('UPDATE squad SET goals = goals + 1 WHERE id = ?', (p[0],))
                c.execute('''INSERT INTO league_stats (player_id, user_id, goals) VALUES (?, ?, 1) 
                             ON CONFLICT(player_id) DO UPDATE SET goals = goals + 1''', (p[0], h_id))

                if assister and assister[0] != p[0]:
                    event_txt += f" (пас: {assister[1]})"
                    c.execute('UPDATE squad SET assists = assists + 1 WHERE id = ?', (assister[0],))
                    c.execute('''INSERT INTO league_stats (player_id, user_id, assists) VALUES (?, ?, 1) 
                            ON CONFLICT(player_id) DO UPDATE SET assists = assists + 1''', (assister[0], a_id))
                match_events.append((minute, event_txt))
            
            elif roll < h_chance + a_chance:
                a_score += 1
                p = random.choice([p for p in a_players if p[2] != 'GK'])
                assister = None
                if random.random() < 0.75:
                    possible_assisters = [pa for pa in a_players if pa[0] != p[0]]
                    if possible_assisters: assister = random.choice(possible_assisters)

                event_text = f"⚽️ {minute}' Гол! {p[1]} ({a_club})"
                
                if assister:
                    event_text += f" (пас: {assister[1]})"
                    c.execute('UPDATE squad SET assists = assists + 1 WHERE id = ?', (assister[0],))
                match_events.append((minute, event_text))

            if random.random() < 0.08:
                side = random.choice([(h_id, h_players, h_club), (a_id, a_players, a_club)])
                p_c = random.choice(side[1])
                p_c_id = p_c[0]
                p_c_name = p_c[1]

                if random.random() < 0.3:  # ТРАВМА
                    dur = random.randint(2, 3) 
                    match_events.append((minute, f"🚑 {minute}' Травма! {p_c_name} ({side[2]}) на {dur-1} т."))
                    c.execute('UPDATE squad SET injury_remaining = ?, slot_id = NULL, status = "bench" WHERE id = ?', (dur, p_c_id))

                else:  # КАРТОЧКА
                    c.execute('SELECT yellow_cards FROM squad WHERE id = ?', (p_c_id,))
                    res = c.fetchone()
                    current_yc = res[0] if res else 0

                    if current_yc >= 1: # ВТОРАЯ ЖЕЛТАЯ -> КРАСНАЯ
                        match_events.append((minute, f"🟥 {minute}' Удаление! {p_c_name} (2-я ЖК) ({side[2]})"))
                        c.execute('UPDATE squad SET yellow_cards = 0, is_banned = 2, slot_id = NULL, status = "bench" WHERE id = ?', (p_c_id,))
                    else:
                        match_events.append((minute, f"🟨 {minute}' ЖК: {p_c_name} ({side[2]})"))
                        c.execute('UPDATE squad SET yellow_cards = yellow_cards + 1 WHERE id = ?', (p_c_id,))

        for p_data in h_players + a_players:
            p_id, p_pos = p_data[0], p_data[2]
            add_tired = 2 if p_pos == 'GK' else (4 if p_pos == 'DEF' else (7 if p_pos == 'MID' else 9))
            c.execute('UPDATE squad SET stamina = MIN(50, stamina + ?) WHERE id = ?', (add_tired, p_id))
         

        match_events.sort(key=lambda x: x[0])
        events_html = "\n".join([e[1] for e in match_events])
        c.execute('UPDATE league_schedule SET status = "finished" WHERE id = ?', (m_id,))

        if h_score > a_score:
            c.execute('UPDATE users SET league_wins=league_wins+1, league_goals=league_goals+? WHERE user_id=?', (h_score, h_id))
            c.execute('UPDATE users SET league_losses=league_losses+1, league_goals=league_goals+? WHERE user_id=?', (a_score, a_id))
        elif a_score > h_score:
            c.execute('UPDATE users SET league_wins=league_wins+1, league_goals=league_goals+? WHERE user_id=?', (a_score, a_id))
            c.execute('UPDATE users SET league_losses=league_losses+1, league_goals=league_goals+? WHERE user_id=?', (h_score, h_id))
        else:
            c.execute('UPDATE users SET league_draws=league_draws+1, league_goals=league_goals+? WHERE user_id=?', (h_score, h_id))
            c.execute('UPDATE users SET league_draws=league_draws+1, league_goals=league_goals+? WHERE user_id=?', (a_score, a_id))

        match_report = (
            f"<b>{h_club}</b> 🆚 <b>{a_club}</b>\n"
            f"<code>┏━━━━━━━━━━━━━━━━━━━━┓</code>\n"
            f"      ⚽️  <b>{h_score} : {a_score}</b>  ⚽️\n"
            f"<code>┗━━━━━━━━━━━━━━━━━━━━┛</code>\n"
            f"{events_html if events_html else '<i>— Без моментов</i>'}\n"
            f"🏁 ————————————————————\n\n"
        )

        final_report += match_report

        msg_text = (f"🏟 <b>МАТЧ ЗАВЕРШЕН!</b>\n\n⚔️ <b>{h_club}</b> {h_score}:{a_score} <b>{a_club}</b>\n"
                    f"————————————————————\n{events_html if events_html else 'Тихая игра.'}")
        for user_id in [h_id, a_id]:
            try: await bot.send_message(user_id, msg_text, parse_mode="HTML")
            except: pass

    c.execute('SELECT player_name, pos, injury_remaining, is_banned FROM squad WHERE user_id = ? AND (injury_remaining > 0 OR is_banned > 0)', (user_id,))
    players = c.fetchall()
    
    conn.commit(); conn.close()
    await cb.message.answer(final_report, parse_mode="HTML")

def process_league_aftermath(conn):
    """
    Вызывается после завершения всех матчей тура.
    Лечит травмы и снимает баны за красные карточки.
    """
    c = conn.cursor()

    c.execute('UPDATE squad SET is_banned = 0 WHERE is_banned = 1')

    c.execute('UPDATE squad SET injury_remaining = injury_remaining - 1 WHERE injury_remaining > 0')

    conn.commit()
    print("✅ Лазарет обновлен: травмы уменьшены, баны сняты.")

@dp.callback_query(F.data == "admin_drop_player")
async def admin_drop_start(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.edit_text("👤 <b>Шаг 1:</b> Введите Имя и Фамилию игрока:", parse_mode="HTML")
    await state.set_state(AdminMarketStates.waiting_for_name)

@dp.message(AdminMarketStates.waiting_for_name)
async def admin_set_name(m: types.Message, state: FSMContext):
    await state.update_data(adm_name=m.text)
    await m.answer(f"Ок, рейтинг для {m.text} (1-99):")
    await state.set_state(AdminMarketStates.waiting_for_rating)

@dp.message(AdminMarketStates.waiting_for_rating)
async def admin_set_rating(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Введите число!")
    await state.update_data(adm_rat=int(m.text))
    
    # Кнопки позиций
    kb = InlineKeyboardBuilder()
    for p in ["GK", "DEF", "MID", "FWD"]:
        kb.button(text=p, callback_data=f"adm_pos_{p}")
    
    await m.answer("Выберите позицию:", reply_markup=kb.as_markup())
    await state.set_state(AdminMarketStates.waiting_for_pos)

@dp.callback_query(F.data.startswith("adm_pos_"), AdminMarketStates.waiting_for_pos)
async def admin_set_pos(cb: types.CallbackQuery, state: FSMContext):
    pos = cb.data.split("_")[2]
    await state.update_data(adm_pos=pos)
    await cb.message.answer(f"Позиция {pos} принята. Введите цену выставления (млн €):")
    await state.set_state(AdminMarketStates.waiting_for_price)

@dp.message(AdminMarketStates.waiting_for_price)
async def admin_finish_drop(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): 
        return await m.answer("Введите число!")
    
    price = int(m.text)
    data = await state.get_data()
    
    p_name = str(data.get("adm_name"))
    p_rat = int(data.get("adm_rat"))
    p_pos = str(data.get("adm_pos"))
    
    # СИСТЕМНЫЙ ID ДЛЯ СВОБОДНЫХ АГЕНТОВ
    # Используем 0, чтобы игрок не попадал ни в чьё меню "Весь состав"
    SYSTEM_USER_ID = 0 

    conn = get_db(); c = conn.cursor()
    try:
        c.execute('''
            INSERT INTO squad (user_id, player_name, rating, pos, status, market_price) 
            VALUES (?, ?, ?, ?, 'on_sale', ?)
        ''', (SYSTEM_USER_ID, p_name, p_rat, p_pos, price))
        
        conn.commit()
        await m.answer(f"✅ {p_name} выставлен на рынок как свободный агент!")
    except Exception as e:
        await m.answer(f"❌ Ошибка БД: {e}")
    finally:
        conn.close()
        await state.clear()

@dp.callback_query(F.data == "next_season_half")
async def next_half_callback(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    
    # 1. Проверяем, открыто ли ТО (логика: менять полугодие можно только в перерыве)
    c.execute('SELECT value FROM settings WHERE key = "transfer_window"')
    tw = c.fetchone()
    is_open = int(tw[0]) if tw else 0
    
    if is_open == 0:
        conn.close()
        return await cb.answer("❌ Смена полугодия доступна только при ОТКРЫТОМ ТО!", show_alert=True)

    # 2. Получаем текущее полугодие и меняем его (1 -> 2 или 2 -> 1)
    c.execute('SELECT value FROM settings WHERE key = "current_half"')
    ch = c.fetchone()
    current = int(ch[0]) if ch else 1
    new_half = 2 if current == 1 else 1
    
    try:
        # Обновляем системное полугодие
        c.execute('UPDATE settings SET value = ? WHERE key = "current_half"', (new_half,))
        
        # 3. ЛОГИКА ВОЗВРАТА: Ищем всех, чья аренда заканчивается на НОВОМ полугодии
        # Мы ищем тех, у кого loan_expires_window == new_half
        c.execute('''SELECT id, player_name, original_owner_id, user_id 
                     FROM squad 
                     WHERE status = "loaned" AND loan_expires_window = ?''', (new_half,))
        to_return = c.fetchall()
        
        returned_count = 0
        for lid, name, owner_id, current_user in to_return:
            # Возвращаем игрока владельцу, сбрасываем слот и статус
            c.execute('''UPDATE squad 
                         SET user_id = ?, 
                             original_owner_id = NULL, 
                             status = "bench", 
                             slot_id = NULL, 
                             loan_expires_window = 0 
                         WHERE id = ?''', (owner_id, lid))
            
            # Уведомляем (опционально, можно в лог)
            try:
                await bot.send_message(owner_id, f"🔙 <b>Возврат!</b> {name} вернулся из аренды.")
                await bot.send_message(current_user, f"⌛ <b>Аренда окончена!</b> {name} покинул ваш клуб.")
            except: pass
            returned_count += 1

        conn.commit()
        
        half_text = "ВТОРОЕ (Зима-Весна)" if new_half == 2 else "ПЕРВОЕ (Лето-Осень)"
        await cb.message.edit_text(
            f"✅ <b>Этап сезона успешно изменен!</b>\n\n"
            f"Теперь наступило: <b>{half_text}</b> полугодие.\n"
            f"Вернулось игроков из аренды: <b>{returned_count}</b>",
            parse_mode="HTML"
        )

    except Exception as e:
        conn.rollback()
        await cb.answer(f"Ошибка БД: {e}", show_alert=True)
    finally:
        conn.close()

@dp.callback_query(F.data == "next_half_season")
async def next_half_season_handler(cb: types.CallbackQuery):
    await cb.answer("⏳ Пересчет сезона...")
    
    with get_db() as conn:
        c = conn.cursor()
        
        # 1. Узнаем текущее полугодие
        c.execute('SELECT value FROM settings WHERE key = "current_half"')
        res = c.fetchone()
        current = int(res[0]) if res else 1
        
        # Переключаем: если было 1 -> станет 2, если было 2 -> станет 1
        new_half = 2 if current == 1 else 1
        
        # 2. Ищем игроков, которые должны вернуться в ЭТОМ новом полугодии
        c.execute('''SELECT id, player_name, original_owner_id 
                     FROM squad 
                     WHERE loan_expires_window = ? AND status = "loaned"''', (new_half,))
        returned_players = c.fetchall()
        
        # 3. Возвращаем игроков "домой"
        for p_id, p_name, owner_id in returned_players:
            # Возвращаем владельцу, сбрасываем аренду и убираем из состава (в запас)
            c.execute('''UPDATE squad 
                         SET user_id = ?, 
                             status = "bench", 
                             original_owner_id = NULL, 
                             loan_expires_window = 0,
                             slot_id = NULL 
                         WHERE id = ?''', (owner_id, p_id))
            
            # Опционально: уведомляем владельца
            try:
                await bot.send_message(owner_id, f"🔙 Ваш игрок <b>{p_name}</b> вернулся из аренды!", parse_mode="HTML")
            except: pass

        # 4. Сохраняем новое полугодие в настройки
        c.execute('UPDATE settings SET value = ? WHERE key = "current_half"', (new_half,))
        conn.commit()

    # Текст для админа
    half_text = "Зима/Весна (2-е полугодие)" if new_half == 2 else "Лето/Осень (1-е полугодие)"
    await cb.message.edit_text(
        f"✅ <b>Сезон обновлен!</b>\n"
        f"📅 Текущий этап: {half_text}\n"
        f"🔄 Вернулось из аренды: {len(returned_players)} чел.",
        parse_mode="HTML"
    )

@dp.callback_query(F.data == "admin_league_start")
async def admin_league_start(cb: types.CallbackQuery):
    if cb.from_user.id not in ADMINS: 
        return await cb.answer("Только для админов!", show_alert=True)
    
    conn = get_db(); c = conn.cursor()
    try:
        # 1. Сбор участников (берем тех, кто в списке участников)
        c.execute('SELECT user_id FROM league_participants')
        participants = [row[0] for row in c.fetchall()]
        
        # Перемешиваем список, чтобы сетка была случайной
        random.shuffle(participants)
        n = len(participants)

        if n < 2:
            return await cb.message.answer("❌ Нужно минимум 2 команды!")
        
        # Если нечетное, можно либо выдать ошибку, либо добавить "Бота-пустышку"
        if n % 2 != 0:
            return await cb.message.answer(f"❌ Нужно четное количество команд (сейчас {n}).")

        # 2. Очистка старых данных
        c.execute('DELETE FROM league_schedule')
        c.execute('UPDATE users SET wins=0, draws=0, losses=0, goals_scored=0')

        # 3. Генерация туров (Round-robin)
        teams = participants[:]
        first_circle = []
        
        for tour in range(n - 1):
            tour_matches = []
            for i in range(n // 2):
                home = teams[i]
                away = teams[n - 1 - i]
                tour_matches.append((home, away))
            first_circle.append(tour_matches)
            # Вращение
            teams = [teams[0]] + [teams[-1]] + teams[1:-1]

        # 2 круга (Зеркальный второй круг)
        all_rounds = first_circle + [[(a, h) for h, a in t] for t in first_circle]

        # 4. Запись в БД и подготовка текста расписания
        match_data = []
        full_schedule_text = "📅 <b>ПОЛНОЕ РАСПИСАНИЕ СЕЗОНА</b>\n\n"
        
        # Для отображения названий клубов в расписании
        c.execute('SELECT user_id, club FROM users WHERE club IS NOT NULL')
        clubs_dict = {row[0]: row[1] for row in c.fetchall()}

        for tour_idx, matches in enumerate(all_rounds, 1):
            full_schedule_text += f"<b>Тур {tour_idx}:</b>\n"
            for h_id, a_id in matches:
                match_data.append((h_id, a_id, tour_idx, "pending"))
                h_name = clubs_dict.get(h_id, f"ID:{h_id}")
                a_name = clubs_dict.get(a_id, f"ID:{a_id}")
                full_schedule_text += f"▫️ {h_name} — {a_name}\n"
            full_schedule_text += "\n"
        
        c.executemany('''INSERT INTO league_schedule (home_id, away_id, tour_number, status) 
                         VALUES (?, ?, ?, ?)''', match_data)
        
        conn.commit()

        # 5. Вывод результата
        summary = (
            f"🏆 <b>ЛИГА СФОРМИРОВАНА!</b>\n"
            f"————————————————————\n"
            f"✅ Команд: <b>{n}</b>\n"
            f"📅 Всего туров: <b>{len(all_rounds)}</b>\n"
            f"⚽️ Всего игр: <b>{len(match_data)}</b>\n"
            f"————————————————————\n"
        )
        
        await cb.message.answer(summary, parse_mode="HTML")

        # Если расписание очень длинное, отправим его файлом, чтобы не спамить
        if len(full_schedule_text) > 4000:
            file_buf = io.BytesIO(full_schedule_text.encode())
            file_buf.name = "schedule.html" # Можно открыть в браузере
            await cb.message.answer_document(types.BufferedInputFile(file_buf.getvalue(), filename="schedule.txt"), 
                                             caption="📄 Полный список матчей")
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
        clear_msg = "\n📦 <b>Все лоты сняты с рынка и вернулись в составы!</b>"
        msg_text = "🛑 <b>ТРАНСФЕРНОЕ ОКНО ЗАКРЫТО!</b>\nСделки больше не принимаются. Смена составов завершена."
    else:
        c.execute('UPDATE settings SET value = value + 1 WHERE key = "window_counter"')
        msg_text = "✅ <b>ТРАНСФЕРНОЕ ОКНО ОТКРЫТО!</b>\nВыставляйте игроков на рынок и укрепляйте составы!"

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
            
            c.execute('INSERT INTO squad (user_id, player_name, rating, pos, status, market_price, stamina) VALUES (0, ?, ?, ?, "free_agent", ?, 100)', 
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

    init_db() 
    print("✅ База данных инициализирована")
    
    asyncio.create_task(process_recovery(get_db)) 
    scheduler.start()

    # 3. Запускаем бота
    print("🚀 Бот запущен...")
    await dp.start_polling(bot, allowed_updates=["message", "callback_query", "chat_member"])
    

if __name__ == "__main__":
    asyncio.run(main()) 
