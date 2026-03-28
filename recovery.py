import asyncio
import sqlite3
from datetime import datetime

async def process_recovery(db_func):
    await asyncio.sleep(5) 
    
    while True:
        try:
            conn = db_func() 
            c = conn.cursor()
            
            # ЛОГИКА: Уменьшаем усталость на 10 каждый час (минимум до 0)
            # Если у игрока было 50 (макс усталость), станет 40, потом 30... и до 0.
            c.execute('''
                UPDATE squad 
                SET stamina = CASE 
                    WHEN stamina - 10 < 0 THEN 0 
                    ELSE stamina - 10 
                END 
                WHERE stamina > 0
            ''')
            
            # Травмы и баны остаются так же
            c.execute('UPDATE squad SET injury_remaining = injury_remaining - 1 WHERE injury_remaining > 0')
            c.execute('UPDATE squad SET injury_type = NULL WHERE injury_remaining = 0 AND injury_type IS NOT NULL')
            
            conn.commit()
            conn.close()
            print("🔋 Усталость снижена (игроки отдохнули)!")
            
        except Exception as e:
            print(f"❌ Ошибка в рековери: {e}")
            
        await asyncio.sleep(3600)
