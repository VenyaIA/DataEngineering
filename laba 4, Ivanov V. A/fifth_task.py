import csv
import json

from connect import connect_to_db


def read_csv(filename):
    items = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        reader.__next__()
        for row in reader:
            item = {
                "phone_brand": row[0],
                "phone_model": row[1],
                "store": row[2],
                "price_usd": float(row[3]),
                "storage": int(row[4]),
                "ram": int(row[5]),
                "launch_date": row[6],
                "dimensions": row[7],
                "weight": float(row[8]),
                "display_type": row[9],
                "display_size": float(row[10]),
                "display_resolution": row[11],
                "os": row[12],
                "nfc": bool(int(row[13])),
                "usb": row[14],
                "battery": row[15],
                "features_sensors": row[16],
                "colors": row[17],
                "video": row[18],
                "chipset": row[19],
                "cpu": row[20],
                "gpu": row[21],
                "year": row[22],
                "foldable": bool(int(row[23])),
                "ppi_density": int(row[24]),
                "quantile_10": float(row[25]),
                "quantile_50": float(row[26]),
                "quantile_90": float(row[27]),
                "price_range": row[28],
                "os_type": row[29],
                "os_version": row[30],
                "battery_size": row[31],
                "colors_available": int(row[32]),
                "chip_company": row[33],
                "cpu_core": row[34],
                "gpu_company": row[35],
                "fingerprint": row[36],
                "video_resolution": row[37]
            }
            items.append(item)
    return items


def read_json(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        items = json.load(f)

    for item in items:
        item["price_usd"] = float(item["price_usd"])
        item["storage"] = int(item["storage"])
        item["ram"] = int(item["ram"])
        item["weight"] = float(item["weight"])
        item["display_size"] = float(item["display_size"])
        item["nfc"] = bool(int(item["nfc"]))
        item["foldable"] = bool(int(item["foldable"]))
        item["ppi_density"] = int(item["ppi_density"])
        item["quantile_10"] = float(item["quantile_10"])
        item["quantile_50"] = float(item["quantile_50"])
        item["quantile_90"] = float(item["quantile_90"])
        item["colors_available"] = int(item["colors_available"])

    return items


def create_tables(db):
    cursor = db.cursor()

    # Название предметной области Телефоны 2024
    # Этот файл содержит очищенные и обработанные данные о
    # телефонах с информацией с различных платформ электронной коммерции.
    # Предметная область охватывает информацию о мобильных устройствах
    # (смартфонах, планшетах и других портативных устройствах), а также
    # о данных, связанных с их продажей и характеристиками.
    # Включает в себя сведения о моделях устройств, их технических
    # характеристиках, ценах на устройства в различных магазинах и других
    # аспектах, таких как операционная система, размер экрана, камера,
    # процессор и другие специфические особенности.

    cursor.execute("""
        CREATE TABLE devices (
            id integer primary key,
            phone_brand text,
            phone_model text,
            storage integer,
            ram integer,
            launch_date text,
            dimensions text,
            weight float,
            battery text,
            foldable integer, -- 0 или 1 (FALSE/TRUE)
            ppi_density integer
        )
    """)

    cursor.execute("""
        CREATE TABLE specifications (
            id integer primary key,
            device_id integer references devices (id),
            os text,
            os_type text,
            os_version text,
            display_type text,
            display_size float,
            display_resolution text,
            chipset text,
            cpu text,
            gpu text,
            video text,
            video_resolution text,
            battery_size text
        )
    """)

    cursor.execute("""
        CREATE TABLE market (
            id integer primary key,
            device_id integer references devices (id),
            store text,
            price_usd float,
            year text,
            quantile_10 float,
            quantile_50 float,
            quantile_90 float,
            price_range text,
            colors_available integer,
            chip_company text,
            cpu_core text,
            gpu_company text,
            fingerprint text,
            features_sensors text,
            colors text
        )
    """)


def insert_data(db, items):
    cursor = db.cursor()
    device_ids = []

    # Вставляем данные в таблицу devices по одной записи за раз
    for item in items:
        cursor.execute("""
            INSERT INTO devices (
                phone_brand, phone_model, storage, ram, launch_date, 
                dimensions, weight, battery, foldable, ppi_density
            ) VALUES (
                :phone_brand, :phone_model, :storage, :ram, :launch_date, 
                :dimensions, :weight, :battery, :foldable, :ppi_density
            )
            """, item)

        device_ids.append(cursor.lastrowid)  # Получаем device_id для каждой записи

    # Теперь добавляем device_id в элементы для таблиц specifications и market
    for i, item in enumerate(items):
        item['device_id'] = device_ids[i]


    cursor.executemany("""
        INSERT INTO specifications (
            device_id, os, os_type, os_version, display_type, display_size, 
            display_resolution, chipset, cpu, gpu, video, video_resolution, battery_size
        ) VALUES (
            :device_id, :os, :os_type, :os_version, :display_type, :display_size, 
            :display_resolution, :chipset, :cpu, :gpu, :video, :video_resolution, :battery_size
        )
        """, items)

    cursor.executemany("""
        INSERT INTO market (
            device_id, store, price_usd, year, quantile_10, quantile_50, quantile_90, 
            price_range, colors_available, chip_company, cpu_core, gpu_company, 
            fingerprint, features_sensors, colors
        ) VALUES (
            :device_id, :store, :price_usd, :year, :quantile_10, :quantile_50, :quantile_90, 
            :price_range, :colors_available, :chip_company, :cpu_core, :gpu_company, 
            :fingerprint, :features_sensors, :colors
        )
        """, items)
    db.commit()


def first_query(db):
    cursor = db.cursor()
    # Выбирает phone_brand, phone_model, store, и price_usd из таблиц devices и market,
    # с условием, что цена устройства меньше 1000, сортирует по цене по возрастанию и
    # ограничивает количество 10.
    res = cursor.execute("""
        SELECT d.phone_brand, d.phone_model, m.store, m.price_usd
        FROM devices d
        JOIN market m ON d.id = m.device_id
        WHERE m.price_usd < 1000
        ORDER BY m.price_usd ASC
        LIMIT 10
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def second_query(db):
    cursor = db.cursor()
    # Подсчитывает количество устройств, цена которых больше 500.
    res = cursor.execute("""
        SELECT COUNT(*) as total_devices
        FROM devices d
        JOIN market m ON d.id = m.device_id
        WHERE m.price_usd > 500
    """)

    result = res.fetchone()
    return {"total_devices": result[0]}


def third_query(db):
    cursor = db.cursor()
    # Группирует устройства по phone_brand, считая количество моделей
    # в каждой группе, и возвращает бренды, у которых больше двух моделей.
    res = cursor.execute("""
        SELECT d.phone_brand, COUNT(*) as device_count
        FROM devices d
        JOIN market m ON d.id = m.device_id
        GROUP BY d.phone_brand
        HAVING device_count > 2
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def fourth_query(db):
    cursor = db.cursor()
    # Выбирает phone_brand, phone_model и среднюю цену для каждого устройства,
    # где средняя цена больше 500.
    res = cursor.execute("""
        SELECT d.phone_brand, d.phone_model, AVG(m.price_usd) as average_price
        FROM devices d
        JOIN market m ON d.id = m.device_id
        GROUP BY d.phone_brand, d.phone_model
        HAVING average_price > 500
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def fifth_query(db):
    cursor = db.cursor()
    # Для каждого бренда выбирает максимальную цену устройства.
    res = cursor.execute("""
        SELECT d.phone_brand, MAX(m.price_usd) as max_price
        FROM devices d
        JOIN market m ON d.id = m.device_id
        GROUP BY d.phone_brand
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


def sixth_query(db):
    cursor = db.cursor()
    # Выбирает устройства, чьи цены находятся в пределах
    # от 500 до 1500, сортируя их по убыванию цены.
    res = cursor.execute("""
        SELECT d.phone_brand, m.store, m.price_usd
        FROM devices d
        JOIN market m ON d.id = m.device_id
        WHERE m.price_usd BETWEEN 500 AND 1500
        ORDER BY m.price_usd DESC
    """)

    items = []
    for row in res.fetchall():
        items.append(dict(row))

    return items


# CREATE
db = connect_to_db('fifth.db')
create_tables(db)
# INSERT
insert_data(db, read_csv("70/5/processed_data1.csv"))
insert_data(db, read_json("70/5/processed_data2.json"))
# QUERYING
with open('fifth_results/fifth_result_1.json', 'w', encoding='utf-8') as f:
    json.dump(first_query(db), f, ensure_ascii=False, indent=4)

with open('fifth_results/fifth_result_2.json', 'w', encoding='utf-8') as f:
    json.dump(second_query(db), f, ensure_ascii=False, indent=4)

with open('fifth_results/fifth_result_3.json', 'w', encoding='utf-8') as f:
    json.dump(third_query(db), f, ensure_ascii=False, indent=4)

with open('fifth_results/fifth_result_4.json', 'w', encoding='utf-8') as f:
    json.dump(fourth_query(db), f, ensure_ascii=False, indent=4)

with open('fifth_results/fifth_result_5.json', 'w', encoding='utf-8') as f:
    json.dump(fifth_query(db), f, ensure_ascii=False, indent=4)

with open('fifth_results/fifth_result_6.json', 'w', encoding='utf-8') as f:
    json.dump(sixth_query(db), f, ensure_ascii=False, indent=4)










