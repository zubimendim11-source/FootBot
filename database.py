import sqlite3
import os

# Получаем путь к папке, где лежит скрипт
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, 'game.db') # Убедись, что тут database.db

def get_db():
    # Используем DB_PATH вместо простого текста
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.execute('PRAGMA busy_timeout = 5000') 
    conn.execute('PRAGMA journal_mode = WAL') 
    return conn
