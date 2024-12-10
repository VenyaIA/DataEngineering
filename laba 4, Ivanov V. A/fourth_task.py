import csv
import json

from connect import connect_to_db


def read_text(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        items = []
        item = {}
        for line in lines:
            if line == '=====\n':
                if 'category' not in item.keys():
                    continue
                items.append(item)
                item = {}
                continue
            pair = line.strip().split('::')
            key = pair[0]
            if key in ['price']:
                pair[1] = float(pair[1])
            if key in ['quantity', 'views']:
                pair[1] = int(pair[1])
            if key in ['isAvailable']:
                pair[1] = pair[1] == 'True'


            item[pair[0]] = pair[1]

    return items


def read_upd_csv(path):
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f,
                                ['name', 'method', 'param'], delimiter=';')
        reader.__next__()
        updates = list(reader)
        for update in updates:
            if update['method'] == 'available':
                update['param'] = update['param'] == 'True'
            elif update['method'] != 'remove':
                update['param'] = float(update['param'])

        return updates


def create_product_table(db):
    cursor = db.cursor()

    cursor.execute("""
        CREATE TABLE product (
            id integer primary key, 
            name text, 
            price float, 
            quantity integer, 
            category text, 
            fromCity text,
            isAvailable boolean,
            views integer,
            version integer default 0
        )
    """)


def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO product (name, price, quantity, category, fromCity, isAvailable, views)
        VALUES (:name, :price, :quantity, :category, :fromCity, :isAvailable, :views)
    """, items)
    db.commit()


def handle_remove(db, name):
    cursor = db.cursor()
    cursor.execute("DELETE FROM product WHERE name = ?", [name])
    db.commit()


def handle_price_percent(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET price = ROUND(price * (1 + ?), 2), version = version + 1 
        WHERE name = ?""",
        [param, name]
    )
    db.commit()


def handle_price_abs(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET price =  ROUND(price + ?, 2), version = version + 1 
        WHERE name = ?""",
        [param, name]
    )
    db.commit()


def handle_quantity_add(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET quantity = quantity + ?, version = version + 1 
        WHERE name = ?""",
        [param, name]
    )
    db.commit()


def handle_available(db, name, param):
    cursor = db.cursor()
    cursor.execute("""
        UPDATE product 
        SET isAvailable = ?, version = version + 1 
        WHERE name = ?""",
        [param, name]
    )
    db.commit()


def handle_updates(db, updates):
    # {'quantity_sub', 'quantity_add', 'price_percent', 'available', 'remove', 'price_abs'}
    for update in updates:
        if update['method'] == 'remove':
            handle_remove(db, update['name'])
        elif update['method'] == 'price_percent':
            handle_price_percent(db, update['name'], update['param'])
        elif update['method'] == 'price_abs':
            handle_price_abs(db, update['name'], update['param'])
        elif update['method'] == 'quantity_add':
            handle_quantity_add(db, update['name'], update['param'])
        elif update['method'] == 'quantity_sub':
            handle_quantity_add(db, update['name'], update['param'])
        elif update['method'] == 'available':
            handle_available(db, update['name'], update['param'])


def first_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM product
        ORDER BY version DESC
        LIMIT 10
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def second_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            category,
            SUM(price) as sum_price,
            MIN(price) as min_price,
            MAX(price) as max_price,
            ROUND(AVG(price), 2) as avg_price,
            SUM(quantity) as quantity_in_group
        FROM product
        GROUP BY category
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            category,
            SUM(quantity) as sum_price,
            MIN(quantity) as min_price,
            MAX(quantity) as max_price,
            ROUND(AVG(quantity), 2) as avg_price
        FROM product
        GROUP BY category
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def fourth_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM product
        WHERE name LIKE 'g%'
        ORDER BY price DESC
        LIMIT 5
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


# CREATE
db = connect_to_db('fourth.db')
create_product_table(db)
# INSERT
insert_data(db, read_text('70/4/_product_data.text'))
# UPDATE
updates = read_upd_csv('70/4/_update_data.csv')
handle_updates(db, updates)
# QUERYING
with open('fourth_results/fourth_result_1.json', 'w', encoding='utf-8') as f:
    json.dump(first_query(db), f, ensure_ascii=False, indent=4)

with open('fourth_results/fourth_result_2.json', 'w', encoding='utf-8') as f:
    json.dump(second_query(db), f, ensure_ascii=False, indent=4)

with open('fourth_results/fourth_result_3.json', 'w', encoding='utf-8') as f:
    json.dump(third_query(db), f, ensure_ascii=False, indent=4)

with open('fourth_results/fourth_result_4.json', 'w', encoding='utf-8') as f:
    json.dump(fourth_query(db), f, ensure_ascii=False, indent=4)

