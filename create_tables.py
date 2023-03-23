import sqlite3

db_conn = sqlite3.connect("events.sqlite")
db_cursor = db_conn.cursor()

db_cursor.execute('''
    CREATE TABLE buy
    (id INTEGER PRIMARY KEY AUTOINCREMENT,
    buy_id TEXT NOT NULL,
    item_name TEXT NOT NULL,
    item_price REAL NOT NULL,
    buy_qty INTEGER NOT NULL,
    trace_id TEXT NOT NULL,
    date_created TEXT NOT NULL)
''')

db_cursor.execute('''
    CREATE TABLE sell
    (id INTEGER PRIMARY KEY AUTOINCREMENT,
    sell_id TEXT NOT NULL,
    item_name TEXT NOT NULL,
    item_price REAL NOT NULL,
    sell_qty INTEGER NOT NULL,
    trace_id TEXT NOT NULL,
    date_created TEXT NOT NULL)
''')

db_conn.commit()
db_conn.close()