#!/usr/bin/env python3

import sqlite3
from faker import Faker
import random

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

fake = Faker()

# Create tables
def create_users(num):
    tabname = f"users_{num}"
    cursor.execute(f'DROP TABLE IF EXISTS {tabname}')
    cursor.execute(f'''
        CREATE TABLE {tabname} (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT
        )
    ''')

    # Insert users
    for _ in range(num):
        cursor.execute(f'''
            INSERT INTO {tabname} (first_name)
            VALUES (?)
        ''', (fake.first_name(),))

    # Get and return user IDs
    user_ids = [row[0] for row in cursor.execute(f'SELECT id FROM {tabname}').fetchall()]
    return user_ids

def create_orders(user_ids, num_users, num):
    tabname = f"orders_{num_users}_{num}"
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

user_ids_1000 = create_users(1000)
create_orders(user_ids_1000, 1000, 1000)
create_orders(user_ids_1000, 1000, 10000)
create_orders(user_ids_1000, 1000, 100000)
user_ids_10000 = create_users(10000)
create_orders(user_ids_10000, 10000, 1000)
create_orders(user_ids_10000, 10000, 10000)
create_orders(user_ids_10000, 10000, 100000)
user_ids_100000 = create_users(100000)
create_orders(user_ids_100000, 100000, 1000)
create_orders(user_ids_100000, 100000, 10000)
create_orders(user_ids_100000, 100000, 100000)

conn.commit()
conn.close()
