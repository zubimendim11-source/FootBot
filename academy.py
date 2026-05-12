import sqlite3
import random
from datetime import datetime, timedelta
from aiogram import types
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

def get_db():
    return sqlite3.connect('game.db') 

# Настройки уровней: req - сколько нужно продать игроков 85+ для перехода
ACADEMY_LEVELS = {
    1: {"name": "Локальный инкубатор", "req": 0, "slots": 2, "candidates": 3, "chance_boost": 0},
    2: {"name": "Признанная школа", "req": 1, "slots": 3, "candidates": 3, "chance_boost": 5},
    3: {"name": "Региональный гигант", "req": 3, "slots": 4, "candidates": 4, "chance_boost": 10},
    4: {"name": "Мировая кузница", "req": 7, "slots": 5, "candidates": 4, "chance_boost": 20}
}

def get_level_info(stars_sold):
    lvl = 1
    for l, data in ACADEMY_LEVELS.items():
        if stars_sold >= data["req"]:
            lvl = l
    return lvl, ACADEMY_LEVELS[lvl]

def init_academy_db():
    conn = get_db(); c = conn.cursor()
    # Твои стандартные таблицы
    c.execute('''CREATE TABLE IF NOT EXISTS academy_players (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER, name TEXT, position TEXT,
        ovr INTEGER, start_ovr INTEGER, potential INTEGER,
        trainings_left INTEGER DEFAULT 5,
        next_training_finish DATETIME,
        last_spawn_date DATETIME
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS academy_candidates (
        user_id INTEGER PRIMARY KEY,
        c1_data TEXT, c2_data TEXT, c3_data TEXT, c4_data TEXT,
        search_date DATETIME
    )''')
    # Добавляем таблицу статистики для уровней (Трамплин)
    c.execute('CREATE TABLE IF NOT EXISTS academy_stats (user_id INTEGER PRIMARY KEY, stars_sold INTEGER DEFAULT 0)')
    conn.commit(); conn.close()

async def get_academy_main(user_id):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT stars_sold FROM academy_stats WHERE user_id = ?', (user_id,))
    stars_sold = (c.fetchone() or [0])[0]
    conn.close()

    lvl, l_data = get_level_info(stars_sold)
    
    # Красивый статус прогресса
    next_lvl = lvl + 1
    if next_lvl in ACADEMY_LEVELS:
        needed = ACADEMY_LEVELS[next_lvl]["req"]
        progress = f"📈 До уровня {next_lvl}: продать {stars_sold}/{needed} топ-игроков (85+)"
    else:
        progress = "👑 У вас максимальный уровень академии!"

    text = (
        f"🏫 <b>ЦЕНТР ПОДГОТОВКИ: УРОВЕНЬ {lvl}</b>\n"
        f"● Статус: <i>{l_data['name']}</i>\n"
        f"● Доступно мест: <b>{l_data['slots']}</b>\n"
        f"● Кандидатов в поиске: <b>{l_data['candidates']}</b>\n\n"
        f"{progress}"
    )

    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="👥 Моя Академия", callback_data="acad_list")],
        [types.InlineKeyboardButton(text="🔎 Найти таланты", callback_data="acad_search")],
        [types.InlineKeyboardButton(text="⬅️ Назад", callback_data="open_main_menu")]
    ])
    return text, kb

async def get_academy_list(user_id):
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT id, name, position, ovr, trainings_left, next_training_finish, potential FROM academy_players WHERE user_id = ?', (user_id,))
    players = c.fetchall()
    conn.close()

    kb = types.InlineKeyboardMarkup(inline_keyboard=[])
    text = "🏫 <b>АКАДЕМИЯ ТАЛАНТОВ</b>\n\n"
    
    if not players:
        text += "<i>В академии сейчас нет игроков.</i>"

    for p_id, name, pos, ovr, t_left, finish, pot in players:
        status = "✅ Готов"
        if finish:
            f_time = datetime.strptime(finish.split('.')[0], '%Y-%m-%d %H:%M:%S')
            if f_time > datetime.now():
                status = f"⏳ Отдых до {f_time.strftime('%H:%M')}"
        
        icon = "🌟" if pot >= 88 else "💎" if pot >= 82 else "👤"
        text += f"{icon} <b>{name}</b> ({pos})\n— Рейтинг: {ovr} | Попыток: {t_left}/5\n— Статус: {status}\n\n"
        
        row = []
        if t_left > 0 and status == "✅ Готов":
            row.append(types.InlineKeyboardButton(text="🏋️ Тренить", callback_data=f"acad_train_{p_id}"))
        
        # Кнопка перевода в основу теперь всегда доступна (как ты и просил)
        row.append(types.InlineKeyboardButton(text="🎓 В основу", callback_data=f"acad_promote_{p_id}"))
        kb.inline_keyboard.append(row)

    kb.inline_keyboard.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data="open_academy_main")])
    return text, kb

async def get_search_menu(user_id):
    conn = get_db(); c = conn.cursor()
    
    # Получаем уровень для лимитов
    c.execute('SELECT stars_sold FROM academy_stats WHERE user_id = ?', (user_id,))
    stars_sold = (c.fetchone() or [0])[0]
    lvl, l_data = get_level_info(stars_sold)

    # 1. Проверка лимита мест (из конфига уровня)
    c.execute('SELECT COUNT(*) FROM academy_players WHERE user_id = ?', (user_id,))
    if c.fetchone()[0] >= l_data["slots"]:
        conn.close()
        return f"⚠️ В академии нет мест! На вашем уровне лимит: {l_data['slots']} чел.", None

    # 2. Проверка кандидатов
    c.execute('SELECT c1_data, c2_data, c3_data, c4_data FROM academy_candidates WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    
    if row and row[0]:
        candidates = [c for c in row if c is not None]
        conn.close()
    else:
        # 3. Проверка КД (убрал, если есть сохраненные)
        c.execute('SELECT MAX(last_spawn_date) FROM academy_players WHERE user_id = ?', (user_id,))
        last_spawn = c.fetchone()[0]
        now = datetime.now()
        if last_spawn:
            last_time = datetime.strptime(last_spawn.split('.')[0], '%Y-%m-%d %H:%M:%S')
            if (now - last_time).total_seconds() < 86400:
                conn.close()
                ostalos = 86400 - (now - last_time).total_seconds()
                return f"⏳ Скауты еще в пути. Будут через {int(ostalos // 3600)} ч.", None

        # Шанс Wonderkid зависит от уровня (база 10% + бонус уровня)
        wk_chance = 10 + l_data["chance_boost"]

        surnames = ["Ouedraogo", "Mastantuono", "Echeverri", "Restes", "Pafundi", "Scalvini", "Moukoko", "Yoro", "Hato"]
        
        candidates = []
        for _ in range(l_data["candidates"]):
            name = random.choice(surnames)
            pos = random.choice(["GK", "CB", "LB", "RB", "CM", "CAM", "ST", "RW", "LW"])
            roll = random.randint(1, 100)
            
            if roll <= wk_chance: 
                pot = random.randint(88, 94); ovr = random.randint(64, 68); icon = "🌟"
            elif roll <= 45: 
                pot = random.randint(82, 87); ovr = random.randint(60, 65); icon = "💎"
            else: 
                pot = random.randint(75, 81); ovr = random.randint(55, 62); icon = "👤"
            
            # Бонус OVR для высоких уровней
            if lvl >= 2: ovr += 2

            candidates.append(f"{icon} {name}|{pos}|{ovr}|{pot}")

        # Сохранение (на 4 слота)
        save_data = candidates + [None] * (4 - len(candidates))
        c.execute('REPLACE INTO academy_candidates (user_id, c1_data, c2_data, c3_data, c4_data, search_date) VALUES (?, ?, ?, ?, ?, ?)',
                  (user_id, save_data[0], save_data[1], save_data[2], save_data[3], now))
        conn.commit(); conn.close()

    text = "🔎 <b>РЕЗУЛЬТАТЫ СКАНЕРОВ:</b>"
    kb = types.InlineKeyboardMarkup(inline_keyboard=[])
    for i, cand in enumerate(candidates):
        name_part, pos, ovr, pot = cand.split('|')
        kb.inline_keyboard.append([types.InlineKeyboardButton(text=f"{name_part} ({pos}) OVR:{ovr}", callback_data=f"acad_select_{i}")])
    
    kb.inline_keyboard.append([types.InlineKeyboardButton(text="⬅️ Отмена", callback_data="open_academy_main")])
    return text, kb

async def process_start_train(cb: types.CallbackQuery):
    player_id = int(cb.data.split("_")[2])
    conn = get_db(); c = conn.cursor()
    
    c.execute('SELECT ovr, potential, trainings_left, name FROM academy_players WHERE id = ?', (player_id,))
    player = c.fetchone()
    
    if player and player[2] > 0:
        curr_ovr, pot, t_left, name = player
        roll = random.randint(1, 100)
        
        # Логика роста с учетом потенциала
        if curr_ovr >= pot:
            gain = random.choice([0, 1])
            res_msg = "Максимум достигнут."
        elif roll > 85:
            gain = random.randint(4, 5); res_msg = "Шикарная треня! 🔥"
        elif roll > 25:
            gain = random.randint(2, 3); res_msg = "Есть прогресс."
        else:
            gain = 0; res_msg = "Игрок ленился. 😴"

        new_ovr = min(curr_ovr + gain, 95)
        finish_time = datetime.now() + timedelta(hours=6)
        
        c.execute('''UPDATE academy_players 
                     SET ovr = ?, next_training_finish = ?, trainings_left = trainings_left - 1 
                     WHERE id = ?''', (new_ovr, finish_time, player_id))
        conn.commit()
        await cb.answer(f"🏋️ {name}: {res_msg} (+{gain} OVR)", show_alert=True)
    else:
        await cb.answer("❌ Тренировки закончились!", show_alert=True)
    
    conn.close()
    text, kb = await get_academy_list(cb.from_user.id)
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

async def restore_training_tasks(bot):
    conn = get_db(); c = conn.cursor()
    now = datetime.now()
    c.execute('SELECT id, user_id, name, next_training_finish FROM academy_players WHERE next_training_finish IS NOT NULL')
    for pid, uid, name, finish_str in c.fetchall():
        try:
            until_dt = datetime.strptime(finish_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
            if until_dt <= now:
                await training_done_academy_callback(bot, uid, pid, name)
            else:
                job_id = f"acad_train_{pid}"
                if not scheduler.get_job(job_id):
                    scheduler.add_job(training_done_academy_callback, 'date', run_date=until_dt, args=[bot, uid, pid, name], id=job_id, replace_existing=True)
        except Exception as e: print(f"Ошибка восстановления {pid}: {e}")
    conn.close()

async def training_done_academy_callback(bot, user_id, player_id, name):
    conn = get_db(); c = conn.cursor()
    c.execute('UPDATE academy_players SET next_training_finish = NULL WHERE id = ?', (player_id,))
    conn.commit(); conn.close()
    try: await bot.send_message(user_id, f"✅ Тренировка <b>{name}</b> завершена!", parse_mode="HTML")
    except: pass

async def process_select_candidate(cb: types.CallbackQuery):
    user_id = cb.from_user.id
    candidate_index = int(cb.data.split("_")[2])
    conn = get_db(); c = conn.cursor()
    c.execute('SELECT c1_data, c2_data, c3_data FROM academy_candidates WHERE user_id = ?', (user_id,))
    row = c.fetchone()
    
    if not row:
        conn.close()
        return await cb.answer("❌ Срок предложения истек.", show_alert=True)
    
    selected_data = row[candidate_index]
    # Убираем иконку перед сохранением в БД для чистоты имени
    raw_data = selected_data.replace("🌟 ", "").replace("💎 ", "").replace("👤 ", "")
    name, pos, ovr, pot = raw_data.split('|')
    now = datetime.now()

    c.execute('''
        INSERT INTO academy_players (user_id, name, position, ovr, start_ovr, potential, trainings_left, last_spawn_date)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''', (user_id, name, pos, int(ovr), int(ovr), int(pot), 5, now))
    
    c.execute('DELETE FROM academy_candidates WHERE user_id = ?', (user_id,))
    conn.commit(); conn.close()
    await cb.answer(f"💎 {name} зачислен в твою Академию!", show_alert=True)
    text, kb = await get_academy_list(user_id)
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")

async def process_promote_to_squad(cb: types.CallbackQuery):
    player_id = int(cb.data.split("_")[2])
    conn = get_db(); c = conn.cursor()
    
    # Берем данные игрока из академии
    c.execute('SELECT name, position, ovr, user_id FROM academy_players WHERE id = ?', (player_id,))
    p = c.fetchone()
    
    if not p:
        conn.close()
        return await cb.answer("❌ Игрок не найден!", show_alert=True)
    
    name, pos, ovr, uid = p
    
    # 1. Вставляем в таблицу основного состава (squad)
    c.execute('''INSERT INTO squad (user_id, player_name, pos, rating, status) 
                 VALUES (?, ?, ?, ?, 'bench')''', (uid, name, pos, ovr))
    
    # 2. Удаляем из академии
    c.execute('DELETE FROM academy_players WHERE id = ?', (player_id,))
    
    conn.commit()
    conn.close()
    
    await cb.answer(f"🎓 {name} переведен в основной состав!", show_alert=True)
    
    # Обновляем список академии
    text, kb = await get_academy_list(uid)
    await cb.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
