import datetime as dt
from aiogram import Router, F, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from database import get_db

router = Router()
ADMINS = [5611356552]

class AwardStates(StatesGroup):
    choosing_type = State()
    choosing_user = State()
    entering_data = State() # Оставили одно имя для всех хендлеров

# --- ПРОСМОТР ЗАЛА СЛАВЫ ---
@router.message(F.text == "🏛 Зал Славы")
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
            try:
                d = dt.datetime.strptime(date_str.split('.')[0], '%Y-%m-%d %H:%M:%S').strftime('%d.%m.%y')
            except:
                d = "??.??"
            text += f"🏆 {award}\n👤 <b>{player}</b> ({club})\n📅 <i>{d}</i>\n————————————————\n"

    kb = None
    if m.from_user.id in ADMINS:
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="⚙️ Вручить награду", callback_data="admin_give_award")]
        ])
    
    await m.answer(text, reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "admin_cancel")
async def cancel_admin_action(cb: types.CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.edit_text("❌ Действие отменено.")

# --- ЭТАП 1: ВЫБОР ТИПА ---
@router.callback_query(F.data == "admin_give_award", F.from_user.id.in_(ADMINS))
async def start_award(cb: types.CallbackQuery, state: FSMContext):
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🥇 Чемпион Лиги", callback_data="award_Победитель Лиги_team")],
        [types.InlineKeyboardButton(text="🏆 Обладатель Кубка", callback_data="award_Обладатель Кубка_team")],
        [types.InlineKeyboardButton(text="⭐ Победитель ЛЧ", callback_data="award_Победитель ЛЧ_team")],
        [types.InlineKeyboardButton(text="🏅 Игрок сезона", callback_data="award_Игрок сезона_player")],
        [types.InlineKeyboardButton(text="🟡 Золотой Мяч", callback_data="award_Золотой Мяч_player")],
        [types.InlineKeyboardButton(text="⚽ Лучший бомбардир", callback_data="award_Лучший бомбардир_player")],
        [types.InlineKeyboardButton(text="❌ Отмена", callback_data="admin_cancel")]
    ])
    await cb.message.edit_text("Выберите тип награды:", reply_markup=kb)
    await state.set_state(AwardStates.choosing_type)

# --- ЭТАП 2: ВЫБОР ЮЗЕРА ---
@router.callback_query(F.data.startswith("award_"), AwardStates.choosing_type)
async def award_type_selected(cb: types.CallbackQuery, state: FSMContext):
    parts = cb.data.split("_")
    award_name = parts[1]
    award_kind = parts[2] 
    
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

# --- ЭТАП 3: СОХРАНЕНИЕ ИЛИ ПЕРЕХОД К ИМЕНИ ---
@router.callback_query(F.data.startswith("seluser_"), AwardStates.choosing_user)
async def user_for_award_selected(cb: types.CallbackQuery, state: FSMContext):
    target_id = cb.data.split("_")[1]
    data = await state.get_data()
    
    if data['award_kind'] == 'player':
        await state.update_data(target_user_id=target_id)
        await cb.message.edit_text("Введите <b>имя футболиста</b>:", parse_mode="HTML")
        await state.set_state(AwardStates.entering_data)
    else:
        conn = get_db(); c = conn.cursor()
        c.execute('INSERT INTO hall_of_fame (user_id, player_name, achievement_type, date_awarded) VALUES (?, ?, ?, ?)',
                  (int(target_id), "Главный тренер", data['current_award'], dt.datetime.now()))
        conn.commit(); conn.close()
        
        await cb.message.edit_text(f"✅ Достижение <b>{data['current_award']}</b> записано!")
        await state.clear()

# --- ЭТАП 4: ФИНАЛ ДЛЯ ИГРОКОВ ---
@router.message(AwardStates.entering_data)
async def process_award_final(m: types.Message, state: FSMContext):
    data = await state.get_data()
    conn = get_db(); c = conn.cursor()
    c.execute('INSERT INTO hall_of_fame (user_id, player_name, achievement_type, date_awarded) VALUES (?, ?, ?, ?)',
              (int(data['target_user_id']), m.text.strip(), data['current_award'], dt.datetime.now()))
    conn.commit(); conn.close()
    
    await m.answer(f"✅ <b>{m.text}</b> занесен в Зал Славы!")
    await state.clear()

@router.callback_query(F.data == "admin_delete_award_list", F.from_user.id.in_(ADMINS))
async def list_awards_for_delete(cb: types.CallbackQuery):
    conn = get_db(); c = conn.cursor()
    # Берем последние 15 наград, чтобы было из чего выбрать
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
        # Кнопка с кратким описанием награды
        btn_text = f"❌ {award} | {player} ({club})"
        kb_builder.inline_keyboard.append([
            types.InlineKeyboardButton(text=btn_text, callback_data=f"delaward_{row_id}")
        ])
    
    kb_builder.inline_keyboard.append([types.InlineKeyboardButton(text="⬅️ Назад", callback_data="admin_cancel")])
    
    await cb.message.edit_text("Выберите награду, которую нужно <b>УДАЛИТЬ</b>:", 
                               reply_markup=kb_builder, parse_mode="HTML")

# Само удаление из БД
@router.callback_query(F.data.startswith("delaward_"), F.from_user.id.in_(ADMINS))
async def confirm_delete_award(cb: types.CallbackQuery):
    award_id = cb.data.split("_")[1]
    
    conn = get_db(); c = conn.cursor()
    c.execute('DELETE FROM hall_of_fame WHERE id = ?', (award_id,))
    conn.commit(); conn.close()
    
    await cb.answer("✅ Награда удалена!", show_alert=True)
    # Обновляем сообщение
    await cb.message.edit_text("Запись успешно удалена из Зала Славы.")
