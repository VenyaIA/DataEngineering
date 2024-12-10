import json
from connect import connect_to_db


def load_text(filename):
    items = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = f.read().split('=====')
        for row in reader[:-1]:  # не считаем последнюю пустую строку
            values = row.strip().split('\n')
            item = {
                'id': int(values[0].split('::')[1]),
                'name': values[1].split('::')[1].strip(),
                'street': values[2].split('::')[1],
                'city': values[3].split('::')[1],
                'zipcode': int(values[4].split('::')[1]),
                'floors': int(values[5].split('::')[1]),
                'year': int(values[6].split('::')[1]),
                'parking': bool(values[7].split('::')[1]),
                'prob_price': int(values[8].split('::')[1]),
                'views': int(values[9].split('::')[1])
            }
            items.append(item)

    return items


def create_tournament_table(db):
    cursor = db.cursor()

    cursor.execute("""
        CREATE TABLE buildings (
            id integer primary key, 
            name text, 
            street text, 
            city text, 
            zipcode integer, 
            floors integer,
            year integer, 
            parking boolean,
            prob_price integer, 
            views integer
        )
    """)


def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO buildings (id, name, street, city, zipcode, 
            floors, year, parking, prob_price, views)
        VALUES (:id, :name, :street, :city, :zipcode, 
            :floors, :year, :parking, :prob_price, :views)
    """, items)
    db.commit()


def first_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM buildings
        ORDER BY views
        LIMIT 80
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def second_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            SUM(prob_price) as sum_prob_price,
            MIN(views) as min_views,
            MAX(year) as max_views,
            ROUND(AVG(floors), 2) as avg_floors
        FROM buildings
    """)

    return dict(res.fetchone())


def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            COUNT(*) as count,
            city
        FROM buildings
        GROUP BY city
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def fourth_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM buildings
        WHERE floors < 6
        ORDER BY floors DESC
        LIMIT 80
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


# STEP 1.
db = connect_to_db('first.db')
create_tournament_table(db)
# STEP 2.
items = load_text('70/1-2/item.text')
insert_data(db, items)
# STEP 3.
with open('first_results/first_result_1.json', 'w', encoding='utf-8') as f:
    json.dump(first_query(db), f, ensure_ascii=False, indent=4)

with open('first_results/first_result_2.json', 'w', encoding='utf-8') as f:
    json.dump(second_query(db), f, ensure_ascii=False, indent=4)

with open('first_results/first_result_3.json', 'w', encoding='utf-8') as f:
    json.dump(third_query(db), f, ensure_ascii=False, indent=4)

with open('first_results/first_result_4.json', 'w', encoding='utf-8') as f:
    json.dump(fourth_query(db), f, ensure_ascii=False, indent=4)




