import json
import pickle
from connect import connect_to_db


def load_pkl(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)


def load_text(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        items = []
        item = {}
        for line in lines:
            if line == '=====\n':
                items.append(item)
                item = {}
                continue
            pair = line.strip().split('::')
            key = pair[0]
            if key in ['instrumentalness', 'explicit', 'loudness']:
                continue
            if key in ['tempo']:
                pair[1] = float(pair[1])

            item[pair[0]] = pair[1]

    return items


def create_songs_table(db):
    cursor = db.cursor()

    cursor.execute("""
        CREATE TABLE songs (
            id integer primary key, 
            artist text, 
            song text, 
            duration_ms integer, 
            year integer, 
            tempo real,
            genre text
        )
    """)


def insert_data(db, items):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO songs (artist, song, duration_ms, year, tempo, genre)
        VALUES (:artist, :song, :duration_ms, :year, :tempo, :genre)
    """, items)
    db.commit()


def first_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM songs
        ORDER BY year
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
            SUM(tempo) as sum_tempo,
            MIN(duration_ms) as min_duration_ms,
            MAX(duration_ms) as max_duration_ms,
            ROUND(AVG(tempo), 2) as avg_tempo
        FROM songs
    """)

    return dict(res.fetchone())


def third_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT 
            COUNT(*) as count,
            artist
        FROM songs
        GROUP BY artist
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def fourth_query(db):
    cursor = db.cursor()
    res = cursor.execute("""
        SELECT *
        FROM songs
        WHERE year < 2002
        ORDER BY year DESC
        LIMIT 85
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


# STEP 1. CREATE TABLE
db = connect_to_db('third.db')
create_songs_table(db)

# STEP 2. INSERT INTO TABLE
insert_data(db, load_pkl('70/3/_part_1.pkl'))
insert_data(db, load_text('70/3/_part_2.text'))

# STEP 3. QUERYING
with open('third_results/third_result_1.json', 'w', encoding='utf-8') as f:
    json.dump(first_query(db), f, ensure_ascii=False, indent=4)

with open('third_results/third_result_2.json', 'w', encoding='utf-8') as f:
    json.dump(second_query(db), f, ensure_ascii=False, indent=4)

with open('third_results/third_result_3.json', 'w', encoding='utf-8') as f:
    json.dump(third_query(db), f, ensure_ascii=False, indent=4)

with open('third_results/third_result_4.json', 'w', encoding='utf-8') as f:
    json.dump(fourth_query(db), f, ensure_ascii=False, indent=4)