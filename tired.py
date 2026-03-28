import sqlite3
import random
from datetime import datetime
from database import get_db

MAX_STAMINA = 50
RECOVERY_RATE = 5

def calculate_match_fatigue(pos, is_league=False):
    """Рассчитывает усталость за матч в зависимости от позиции и типа матча"""
    multiplier = 1.5 if is_league else 1.0
    
    pos = pos.upper()
    if pos == 'FWD':
        base = random.randint(8, 12)
    elif pos == 'MID':
        base = random.randint(5, 8)
    elif pos == 'DEF':
        base = random.randint(3, 5)
    else: # GK
        base = random.randint(1, 2)
        
    return int(base * multiplier)

def get_recovery_amount(last_recovery_str):
    if not last_recovery_str:
        return 0
    last_time = datetime.strptime(last_recovery_str, "%Y-%m-%d %H:%M:%S")
    hours_passed = int((datetime.now() - last_time).total_seconds() // 3600)
    return hours_passed * 5

# В tired.py
def process_stamina_recovery(uid):
    conn = get_db(); c = conn.cursor()
    
    c.execute('SELECT last_recovery FROM users WHERE user_id = ?', (uid,))
    res = c.fetchone()
    
    if not res or not res[0]:
        c.execute('UPDATE users SET last_recovery = ? WHERE user_id = ?', 
                  (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), uid))
        conn.commit(); conn.close(); return

    last_time = datetime.strptime(res[0], "%Y-%m-%d %H:%M:%S")
    hours_passed = int((datetime.now() - last_time).total_seconds() // 3600)

    if hours_passed > 0:
        # Уменьшаем усталость на 5 за каждый час (но не ниже 0)
        recovery_amount = hours_passed * 5
        c.execute('UPDATE squad SET stamina = MAX(0, stamina - ?) WHERE user_id = ?', 
                  (recovery_amount, uid))
        
        c.execute('UPDATE users SET last_recovery = ? WHERE user_id = ?', 
                  (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), uid))
        conn.commit()
    
    conn.close()
