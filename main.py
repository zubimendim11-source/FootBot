import asyncio, sqlite3, logging
from aiogram import Bot, Dispatcher, F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# --- КОНФИГ ---
TOKEN = "8356474742:AAHbPN6YQnPlaEievQ_wLSyL5HMKCfW8C-8"
bot = Bot(token=TOKEN)
dp = Dispatcher(storage=MemoryStorage())
ADMINS = [5611356552]  
ALLOWED_CHATS = [] # Замени на реальный ID своего чата

# --- СОСТОЯНИЯ ---
class AdminStates(StatesGroup):
    target_id = State()
    amount = State()
    player_data = State()

class GameStates(StatesGroup):
    choosing_club = State()
    setting_price = State()

# --- ДАННЫЕ КЛУБОВ ---
CLUBS = {
    "Real Madrid": {"emoji": "⚪️", "players": [{"name": "Куртуа", "rating": 89, "pos": "GK"}, {"name": "Карвахаль", "rating": 86, "pos": "DEF"}, {"name": "Милитао", "rating": 85, "pos": "DEF"}, {"name": "Рюдигер", "rating": 87, "pos": "DEF"}, {"name": "Менди", "rating": 82, "pos": "DEF"}, {"name": "Вальверде", "rating": 88, "pos": "MID"}, {"name": "Беллингем", "rating": 90, "pos": "MID"}, {"name": "Модрич", "rating": 86, "pos": "MID"}, {"name": "Родриго", "rating": 86, "pos": "FWD"}, {"name": "Мбаппе", "rating": 91, "pos": "FWD"}, {"name": "Винисиус", "rating": 90, "pos": "FWD"}]},
    "Man City": {"emoji": "🔵", "players": [{"name": "Эдерсон", "rating": 88, "pos": "GK"}, {"name": "Уокер", "rating": 84, "pos": "DEF"}, {"name": "Диаш", "rating": 89, "pos": "DEF"}, {"name": "Аканджи", "rating": 84, "pos": "DEF"}, {"name": "Гвардиол", "rating": 83, "pos": "DEF"}, {"name": "Родри", "rating": 91, "pos": "MID"}, {"name": "Де Брюйне", "rating": 91, "pos": "MID"}, {"name": "Бернарду Силва", "rating": 88, "pos": "MID"}, {"name": "Фоден", "rating": 88, "pos": "FWD"}, {"name": "Холанд", "rating": 91, "pos": "FWD"}, {"name": "Доку", "rating": 81, "pos": "FWD"}]},
    "Arsenal": {"emoji": "🔴", "players": [{"name": "Райя", "rating": 84, "pos": "GK"}, {"name": "Уайт", "rating": 84, "pos": "DEF"}, {"name": "Салиба", "rating": 87, "pos": "DEF"}, {"name": "Габриэл", "rating": 86, "pos": "DEF"}, {"name": "Зинченко", "rating": 80, "pos": "DEF"}, {"name": "Райс", "rating": 87, "pos": "MID"}, {"name": "Эдегор", "rating": 89, "pos": "MID"}, {"name": "Мерино", "rating": 82, "pos": "MID"}, {"name": "Сака", "rating": 88, "pos": "FWD"}, {"name": "Хаверц", "rating": 84, "pos": "FWD"}, {"name": "Мартинелли", "rating": 83, "pos": "FWD"}]},
    "Barcelona": {"emoji": "🔵🔴", "players": [{"name": "Тер Штеген", "rating": 87, "pos": "GK"}, {"name": "Кунде", "rating": 85, "pos": "DEF"}, {"name": "Кубарси", "rating": 79, "pos": "DEF"}, {"name": "Иньиго Мартинес", "rating": 80, "pos": "DEF"}, {"name": "Бальде", "rating": 81, "pos": "DEF"}, {"name": "Педри", "rating": 86, "pos": "MID"}, {"name": "Касадо", "rating": 76, "pos": "MID"}, {"name": "Дани Ольмо", "rating": 84, "pos": "MID"}, {"name": "Ямаль", "rating": 84, "pos": "FWD"}, {"name": "Левандовски", "rating": 88, "pos": "FWD"}, {"name": "Рафинья", "rating": 84, "pos": "FWD"}]},
    "Liverpool": {"emoji": "🔻", "players": [{"name": "Алиссон", "rating": 89, "pos": "GK"}, {"name": "Ван Дейк", "rating": 91, "pos": "DEF"}, {"name": "Конате", "rating": 83, "pos": "DEF"}, {"name": "Трент", "rating": 86, "pos": "DEF"}, {"name": "Робертсон", "rating": 85, "pos": "DEF"}, {"name": "Мак Аллистер", "rating": 86, "pos": "MID"}, {"name": "Собослаи", "rating": 83, "pos": "MID"}, {"name": "Гравенберх", "rating": 81, "pos": "MID"}, {"name": "Салах", "rating": 90, "pos": "FWD"}, {"name": "Диас", "rating": 84, "pos": "FWD"}, {"name": "Нуньес", "rating": 82, "pos": "FWD"}]}
}

# --- БАЗА ДАННЫХ ---
def get_db(): return sqlite3.connect('football.db')

def init_db():
    conn = get_db(); c = conn.cursor()
    c.execute('CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY, balance INTEGER DEFAULT 1000, username TEXT, club TEXT)')
    c.execute('''CREATE TABLE IF NOT EXISTS squad (
        id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER, player_name TEXT, 
        rating INTEGER, pos TEXT, status TEXT DEFAULT "bench", slot_id INTEGER DEFAULT NULL, market_price INTEGER DEFAULT 0)''')
    conn.commit(); conn.close()

# --- КЛАВИАТУРЫ ---
def get_main_kb(user_id: int):
    b = ReplyKeyboardBuilder()
    b.button(text="💰 Баланс"); b.button(text="📋 Состав")
    b.button(text="🚀 Рынок"); b.button(text="📋 Весь состав")
    if user_id in ADMINS: b.button(text="🛠 Админка")
    b.adjust(2, 2, 1); return b.as_markup(resize_keyboard=True)

# --- ЛОГИКА ---
async def edit_squad_message(message: types.Message, user_id: int):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT club FROM users WHERE user_id = ?', (user_id,))
    res = c.fetchone()
    if not res: return
    club_name = res[0]
    c.execute('SELECT id, player_name, rating, pos, slot_id FROM squad WHERE user_id = ? AND slot_id IS NOT NULL', (user_id,))
    slots = {row[4]: row for row in c.fetchall()}; conn.close()

    formation = [("GK", 1), ("DEF", 4), ("MID", 3), ("FWD", 3)]
    text = f"🗃 <b>Состав {club_name}</b>\n\n"
    builder = InlineKeyboardBuilder()

    curr = 1
    for p_type, limit in formation:
        for _ in range(limit):
            if curr in slots:
                pid, name, rat, _, _ = slots[curr]
                text += f"{curr}. {p_type}: {name} - ⭐ {rat}\n"
                builder.button(text="✅", callback_data=f"manage_{pid}")
            else:
                text += f"{curr}. {p_type}: <i>Пусто</i>\n"
                builder.button(text="❌", callback_data=f"selectpos_{p_type}_{curr}")
            curr += 1
    builder.row(types.InlineKeyboardButton(text="⚡️ Автосбор", callback_data="autofill"))
    builder.adjust(1, 4, 3, 3, 1)
    await message.edit_text(text, reply_markup=builder.as_markup(), parse_mode="HTML")

# --- ОБРАБОТЧИКИ ---

@dp.message(Command("start"))
async def start(m: types.Message, state: FSMContext):
    init_db(); conn = get_db(); c = conn.cursor()
    c.execute('SELECT club FROM users WHERE user_id = ?', (m.from_user.id,))
    user = c.fetchone()
    if user:
        return await m.answer("Вы уже в игре!", reply_markup=get_main_kb(m.from_user.id))
    
    b = InlineKeyboardBuilder()
    for n in CLUBS: b.button(text=f"{CLUBS[n]['emoji']} {n}", callback_data=f"club_{n}")
    b.adjust(1); await m.answer("Выберите клуб:", reply_markup=b.as_markup())
    await state.set_state(GameStates.choosing_club)

@dp.callback_query(F.data.startswith("club_"), GameStates.choosing_club)
async def choose_club(cb: types.CallbackQuery, state: FSMContext):
    club = cb.data.split("_")[1]
    conn = get_db(); c = conn.cursor()
    c.execute('INSERT OR REPLACE INTO users (user_id, username, club) VALUES (?, ?, ?)', (cb.from_user.id, cb.from_user.username, club))
    if club in CLUBS:
        for p in CLUBS[club]["players"]:
            c.execute('INSERT INTO squad (user_id, player_name, rating, pos) VALUES (?, ?, ?, ?)', (cb.from_user.id, p['name'], p['rating'], p['pos']))
    conn.commit(); conn.close(); await state.clear()
    await cb.message.edit_text(f"✅ Вы стали тренером {club}!", reply_markup=get_main_kb(cb.from_user.id))

@dp.message(F.text == "📋 Состав")
async def show_squad(m: types.Message):
    msg = await m.answer("⏳ Загрузка...")
    await edit_squad_message(msg, m.from_user.id)

@dp.callback_query(F.data.startswith("selectpos_"))
async def list_players(cb: types.CallbackQuery):
    _, pos, slot = cb.data.split("_")
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT id, player_name, rating FROM squad WHERE user_id = ? AND slot_id IS NULL AND status = "bench" AND pos = ?', (cb.from_user.id, pos))
    ps = c.fetchall(); conn.close()
    if not ps: return await cb.answer(f"Нет свободных {pos}!", show_alert=True)
    b = InlineKeyboardBuilder()
    for pid, name, rat in ps: b.button(text=f"{name} ({rat})", callback_data=f"set_{pid}_{slot}")
    b.button(text="⬅️ Назад", callback_data="back_to_field"); b.adjust(1)
    await cb.message.edit_text(f"Выберите {pos} в слот {slot}:", reply_markup=b.as_markup())

@dp.callback_query(F.data == "back_to_field")
async def back(cb: types.CallbackQuery): await edit_squad_message(cb.message, cb.from_user.id)

@dp.callback_query(F.data.startswith("set_"))
async def set_player(cb: types.CallbackQuery):
    _, pid, slot = cb.data.split("_")
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE squad SET slot_id = ?, status = "active" WHERE id = ?', (slot, pid))
    conn.commit(); conn.close()
    await edit_squad_message(cb.message, cb.from_user.id)

@dp.callback_query(F.data.startswith("manage_"))
async def manage_player(cb: types.CallbackQuery, state: FSMContext):
    pid = cb.data.split("_")[1]
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT player_name, rating, pos FROM squad WHERE id = ?', (pid,))
    name, rat, pos = c.fetchone(); conn.close()
    await state.update_data(curr_pid=pid)
    b = InlineKeyboardBuilder()
    b.button(text="📥 В запас", callback_data="quick_bench")
    b.button(text="🚀 Продать", callback_data="pre_sell")
    b.button(text="⬅️ Назад", callback_data="back_to_field")
    b.adjust(1)
    await cb.message.edit_text(f"Игрок: {name} ({rat})\nПозиция: {pos}", reply_markup=b.as_markup())

@dp.callback_query(F.data == "quick_bench")
async def quick_bench(cb: types.CallbackQuery, state: FSMContext):
    data = await state.get_data(); pid = data.get("curr_pid")
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE squad SET slot_id = NULL, status = "bench" WHERE id = ?', (pid,))
    conn.commit(); conn.close()
    await cb.answer("Игрок убран")
    await edit_squad_message(cb.message, cb.from_user.id)

@dp.callback_query(F.data == "autofill")
async def autofill(cb: types.CallbackQuery):
    conn = get_db()
    c = conn.cursor()
    user_id = cb.from_user.id

    # 1. Сбрасываем текущий состав: всех, кто не на рынке, отправляем в запас
    c.execute('''UPDATE squad 
                 SET slot_id = NULL, status = "bench" 
                 WHERE user_id = ? AND status != "on_sale"''', (user_id,))

    # 2. Определяем схему (4-3-3)
    formation = [
        ("GK", 1), 
        ("DEF", 4), 
        ("MID", 3), 
        ("FWD", 3)
    ]
    
    current_slot = 1
    players_added = 0

    # 3. Для каждой позиции выбираем лучших по рейтингу
    for pos, limit in formation:
        c.execute('''SELECT id FROM squad 
                     WHERE user_id = ? AND pos = ? AND status = "bench" 
                     ORDER BY rating DESC LIMIT ?''', (user_id, pos, limit))
        
        best_players = c.fetchall()
        for row in best_players:
            player_id = row[0]
            c.execute('''UPDATE squad 
                         SET slot_id = ?, status = "active" 
                         WHERE id = ?''', (current_slot, player_id))
            current_slot += 1
            players_added += 1

    conn.commit()
    conn.close()

    if players_added == 0:
        await cb.answer("❌ У вас нет подходящих игроков в запасе!", show_alert=True)
    else:
        await cb.answer(f"⚡️ Состав собран! Автоматически выставлено: {players_added} чел.")
    
    # Обновляем сообщение с полем, чтобы сразу увидеть результат
    await edit_squad_message(cb.message, user_id)

@dp.callback_query(F.data == "pre_sell")
async def pre_sell(cb: types.CallbackQuery, state: FSMContext):
    await cb.message.edit_text("Введите цену продажи (в млн €):")
    await state.set_state(GameStates.setting_price)

@dp.message(GameStates.setting_price)
async def market_sell(m: types.Message, state: FSMContext):
    if not m.text.isdigit(): return await m.answer("Введите число!")
    price = int(m.text); data = await state.get_data(); pid = data.get("curr_pid")
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE squad SET status = "on_sale", market_price = ?, slot_id = NULL WHERE id = ?', (price, pid))
    conn.commit(); conn.close(); await state.clear()
    await m.answer(f"✅ Выставлен за {price} млн!", reply_markup=get_main_kb(m.from_user.id))

@dp.message(F.text == "🚀 Рынок")
async def show_market(m: types.Message):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT s.id, s.player_name, s.rating, s.market_price, u.username, s.user_id FROM squad s JOIN users u ON s.user_id = u.user_id WHERE s.status = "on_sale"')
    lots = c.fetchall(); conn.close()
    if not lots: return await m.answer("На рынке пусто.")
    for lid, name, rat, pr, seller, sid in lots:
        b = InlineKeyboardBuilder()
        if sid == m.from_user.id: b.button(text="↩️ Снять", callback_data=f"remove_m_{lid}")
        else: b.button(text=f"Купить за {pr}", callback_data=f"buy_{lid}")
        await m.answer(f"📦 {name} (⭐{rat})\nЦена: {pr} млн | Продавец: @{seller}", reply_markup=b.as_markup())

@dp.callback_query(F.data.startswith("remove_m_"))
async def remove_sale(cb: types.CallbackQuery):
    pid = cb.data.split("_")[2]
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE squad SET status = "bench", market_price = 0 WHERE id = ? AND user_id = ?', (pid, cb.from_user.id))
    conn.commit(); conn.close()
    await cb.answer("Снято с продажи"); await cb.message.delete()

@dp.callback_query(F.data.startswith("buy_"))
async def buy_player(cb: types.CallbackQuery):
    lid = cb.data.split("_")[1]
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT user_id, market_price, player_name FROM squad WHERE id = ?', (lid,))
    res = c.fetchone()
    if not res: return
    seller_id, price, p_name = res
    c.execute('SELECT balance FROM users WHERE user_id = ?', (cb.from_user.id,))
    buyer_bal = c.fetchone()[0]
    if buyer_bal < price: return await cb.answer("Нет денег!", show_alert=True)
    c.execute('UPDATE users SET balance = balance - ? WHERE user_id = ?', (price, cb.from_user.id))
    c.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (price, seller_id))
    c.execute('UPDATE squad SET user_id = ?, status = "bench", market_price = 0, slot_id = NULL WHERE id = ?', (cb.from_user.id, lid))
    conn.commit(); conn.close()
    await cb.message.edit_text(f"🎉 Куплен {p_name}!")

@dp.message(F.text == "📋 Весь состав")
async def show_all_interactive(m: types.Message):
    conn = get_db()
    c = conn.cursor()
    # Берем всех игроков пользователя
    c.execute('''SELECT id, player_name, rating, pos, status 
                 FROM squad WHERE user_id = ? 
                 ORDER BY rating DESC''', (m.from_user.id,))
    ps = c.fetchall()
    conn.close()
    
    if not ps: 
        return await m.answer("У вас еще нет игроков.")

    text = "📂 <b>Управление картотекой</b>\n"
    text += "<i>Нажмите на кнопку с игроком, чтобы управлять им</i>\n"
    text += "⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n"
    
    builder = InlineKeyboardBuilder()
    em = {"GK": "🧤", "DEF": "🛡", "MID": "🧠", "FWD": "🎯"}
    
    for pid, name, rat, pos, stat in ps:
        # Иконка статуса для кнопки
        if stat == "active": s_icon = "🏃"
        elif stat == "on_sale": s_icon = "💰"
        else: s_icon = "🪑"
        
        # Создаем кнопку для каждого игрока
        builder.button(
            text=f"{em.get(pos, '⚽️')} {name} ({rat}) {s_icon}", 
            callback_data=f"manage_{pid}" # Используем уже готовый обработчик manage_
        )
    
    builder.adjust(1) # Кнопки в один столбец для удобства
    
    # Добавляем легенду в текст
    footer = "\n⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯⎯\n🏃 — в старте | 🪑 — в запасе | 💰 — на рынке"
    
    await m.answer(text + footer, reply_markup=builder.as_markup(), parse_mode="HTML")

@dp.message(F.text == "💰 Баланс")
async def bal(m: types.Message):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT balance FROM users WHERE user_id = ?', (m.from_user.id,))
    await m.answer(f"💰 Баланс: {c.fetchone()[0]} млн €"); conn.close()

# --- АДМИНКА ---

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
    b.button(text="👥 Список юзеров (ID)", callback_data="admin_list_users") # Новая кнопка
    b.button(text="🏃 Дать игрока", callback_data="admin_give_player")
    b.button(text="🚫 Выгнать", callback_data="admin_kick_user")
    b.adjust(1)
    await m.answer("🔧 Админ-панель:", reply_markup=b.as_markup())

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

async def main(): init_db(); await dp.start_polling(bot)
if __name__ == "__main__": asyncio.run(main())