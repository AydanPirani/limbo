#!/usr/bin/env python3

import sqlite3
from faker import Faker
import random

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

# Create tables
cursor.execute('DROP TABLE IF EXISTS users')
cursor.execute('''
    CREATE TABLE users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT
    )
''')

# Insert users
fake = Faker()
for _ in range(10000):
    cursor.execute('''
        INSERT INTO users (first_name)
        VALUES (?)
    ''', (
        fake.first_name(),
    ))

# Get user IDs after inserting
user_ids = [row[0] for row in cursor.execute('SELECT id FROM users').fetchall()]

def create_orders(num):
    tabname = f"orders_{num}"
    cursor.execute(f'DROP TABLE IF EXISTS {tabname}')
    cursor.execute(f'''
        CREATE TABLE {tabname} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            product_id INTEGER
        )
    ''')

    # Insert orders
    for _ in range(num):
        user_id = random.choice(user_ids)
        product_id = random.randint(1, 60)  # Random number between 1 and number of products

        cursor.execute(f'''
            INSERT INTO {tabname} (user_id, product_id)
            VALUES (?, ?)
        ''', (user_id, product_id))

# TODO: create users
create_orders(1000)
create_orders(1000000)
# create_orders(100000000)

conn.commit()
conn.close()
