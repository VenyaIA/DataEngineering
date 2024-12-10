import csv
import json

from connect import connect_to_db


def load_csv(filename):
    items = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        reader.__next__()
        for row in reader:
            if len(row) == 0: continue
            item = {
                'name': row[0].strip(),
                'rating': float(row[1]),
                'convenience': int(row[2]),
                'security': int(row[3]),
                'functionality': int(row[4]),
                'comment': row[5],
            }
            items.append(item)

    return items


def create_reviews_table(db):
    cursor = db.cursor()

    cursor.execute("""
        CREATE TABLE reviews (
            id integer primary key, 
            buildings_name text references buildings(name), 
            rating real, 
            convenience integer, 
            security integer, 
            functionality integer,
            comment text
        )
    """)


def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO reviews (buildings_name, rating, convenience, security, functionality, comment)
        VALUES (:name, :rating, :convenience, :security, :functionality, :comment)
    """, items)
    db.commit()


def first_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM reviews
        WHERE buildings_name = 'Кубло 55'
        ORDER BY rating
        
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def second_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT b.name, b.id, r.rating
        FROM buildings b
        JOIN reviews r ON b.name = r.buildings_name
        WHERE convenience = 2
        LIMIT 5
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT b.name, sum(r.rating) as sum_rating, count(r.convenience) as count_convenience
        FROM buildings b
        JOIN reviews r ON b.name = r.buildings_name
        GROUP BY b.name
        ORDER BY sum_rating DESC
        LIMIT 10
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


# STEP 1. GET STRUCTURE
items = load_csv('70/1-2/subitem.csv')
# STEP 2. CREATE TABLE
db = connect_to_db('first.db')
create_reviews_table(db)
# STEP 3. INSERT DATA
insert_data(db, items)
# #  STEP 4. QUERYING
with open('second_results/second_result_1.json', 'w', encoding='utf-8') as f:
    json.dump(first_query(db), f, ensure_ascii=False, indent=4)

with open('second_results/second_result_2.json', 'w', encoding='utf-8') as f:
    json.dump(second_query(db), f, ensure_ascii=False, indent=4)

with open('second_results/second_result_3.json', 'w', encoding='utf-8') as f:
    json.dump(third_query(db), f, ensure_ascii=False, indent=4)


















