

import sqlite3

db_conn = sqlite3.connect("events.sqlite")
db_cursor = db_conn.cursor()

table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='buy'"

if db_cursor.execute(table_check_query).fetchone():
    db_cursor.execute("DROP TABLE buy")

table_check_query = "SELECT name FROM sqlite_master WHERE type='table' AND name='sell'"

if db_cursor.execute(table_check_query).fetchone():
    db_cursor.execute("DROP TABLE sell")

db_conn.commit()
db_conn.close()

#import sqlite3

#db_conn = sqlite3.connect("events.sqlite3")
#db_cursor = db_conn.cursor()

#db_cursor.execute('''
#DROP TABLE buy, sell
#''')

#db_conn.commit()
#db_conn.close()