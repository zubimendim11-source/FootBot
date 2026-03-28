import sqlite3

def get_db():
    conn = sqlite3.connect('football_bot.db', check_same_thread=False)
    conn.execute('PRAGMA busy_timeout = 5000') 
    conn.execute('PRAGMA journal_mode = WAL') 
    return conn
