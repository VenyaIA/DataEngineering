import csv
import json
import pymongo
from pymongo import MongoClient


def connect_db():
    client = MongoClient()
    db = client['db-2024']
    print(db.phones)
    return db.phones


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
                "battery": int(row[15]),
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
        item["battery"] = int(item["battery"])
        item["foldable"] = bool(int(item["foldable"]))
        item["ppi_density"] = int(item["ppi_density"])
        item["quantile_10"] = float(item["quantile_10"])
        item["quantile_50"] = float(item["quantile_50"])
        item["quantile_90"] = float(item["quantile_90"])
        item["colors_available"] = int(item["colors_available"])

    return items


def delete_object_id(items):
    for el in items:
        el.pop('_id')


def writing_to_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


collection = connect_db()
collection.insert_many(read_csv('70/processed_data1.csv'))
collection.insert_many(read_json('70/processed_data2.json'))


def sort_by_price_usd(collection):
    result = list(collection.find(limit=10).sort({'price_usd': pymongo.DESCENDING}))
    return result


def filter_by_storage(collection):
    return list(collection
                .find({
                    "storage": {"$gte": 512}
                }, limit=15)
                .sort({'price_usd': pymongo.DESCENDING}))


def complex_filter(collection):
    return list(collection
                .find({
                    "os_type": "Android",
                    "store": {"$in": ["Amazon UK", 'Samsung', 'Best Buy']}
                }, limit=10)
                .sort({'storage': pymongo.ASCENDING}))


def fourth_filter(collection):
    return (collection
                .count_documents({
                    "storage": {"$gt": 64, "$lte": 512},
                    "battery": {"$gte": 4000, "$lte": 5000},
                    "$or": [
                        {"price_usd": {"$gt": 100, "$lte": 400}},
                        {"price_usd": {"$gt": 750, "$lt": 1500}}
                    ]
                }))


def fifth_filter(collection):
    return list(collection
                .find({
                    "os_type": {"$in": ["iOS", 'Android']},
                    "store": {"$in": ["Amazon UK", 'Amazon DE', 'Best Buy']}
                }, limit=20)
                .sort({'price_usd': pymongo.DESCENDING}))


def get_price_usd_agg(collection):
    q = [
        {
            "$group": {
                "_id": "result",
                "max_price_usd": {"$max": "$price_usd"},
                "min_price_usd": {"$min": "$price_usd"},
                "avg_price_usd": {"$avg": "$price_usd"}
            }
        }
    ]

    return list(collection.aggregate(q))


def get_freq_by_os_type(collection):
    q = [
        {
            "$group": {
                "_id": "$os_type",
                "count": {"$sum": 1}
            }
        }
    ]

    return list(collection.aggregate(q))


def get_stat_by_custom_key(collection, group_by_key, agg_key):
    q = [
        {
            "$group": {
                "_id": f"${group_by_key}",
                f"max_{agg_key}": {"$max": f"${agg_key}"},
                f"min_{agg_key}": {"$min": f"${agg_key}"},
                f"avg_{agg_key}": {"$avg": f"${agg_key}"}
            }
        }
    ]

    return list(collection.aggregate(q))


def get_max_price_usd_by_min_storage_sort(collection):
    result = list(collection.find(limit=1)
                  .sort({'storage': pymongo.ASCENDING, 'price_usd': pymongo.DESCENDING}))
    return result


def custom_query(collection):
    q = [
        {
          "$match": {
              "os_type": {"$in": ["iOS", 'Android']},
              "store": {"$in": ["Amazon UK", 'Amazon DE', 'Best Buy']},
              "$or": [
                  {"storage": {"$gt": 20, "$lt": 100}},
                  {"storage": {"$gt": 500, "$lt": 1000}}
              ]
          }
        },
        {
            "$group": {
                "_id": "result",
                "max_price_usd": {"$max": "$price_usd"},
                "min_price_usd": {"$min": "$price_usd"},
                "avg_price_usd": {"$avg": "$price_usd"}
            }
        }
    ]

    return list(collection.aggregate(q))


def delete_by_price_usd(collection):
    return collection.delete_many({
        "$or": [
            {"price_usd": {"$lt": 300}},
            {"price_usd": {"$gt": 2000}},
        ]
    })


def inc_storage(collection):
    return collection.update_many({}, {
        "$inc": {
            "storage": 1
        }
    })


def inc_price_usd(collection):
    return collection.update_many({
        "phone_brand": {"$in": ["honor", "apple"]}
    }, {
        "$mul": {
            "price_usd": 1.05
        }
    })


def inc_price_usd_by_store(collection):
    return collection.update_many({
        "store": {"$in": ["Best Buy", "Amazon US"]}
    }, {
        "$mul": {
            "price_usd": 1.07
        }
    })


def delete_by_custom_predicate(collection):
    return collection.delete_many({
        "$and": [
            {"weight": {"$lt": 170}},
            {"weight": {"$gt": 100}}
        ]
    })


# Выборка
res_1 = sort_by_price_usd(collection)
delete_object_id(res_1)
writing_to_json('fourth_results/first_category/first_result_1.json', res_1)

res_2 = filter_by_storage(collection)
delete_object_id(res_2)
writing_to_json('fourth_results/first_category/first_result_2.json', res_2)

res_3 = complex_filter(collection)
delete_object_id(res_3)
writing_to_json('fourth_results/first_category/first_result_3.json', res_3)

res_4 = fourth_filter(collection)
data_res_4 = {'result_count': res_4}
writing_to_json('fourth_results/first_category/first_result_4.json', data_res_4)

res_5 = fifth_filter(collection)
delete_object_id(res_5)
writing_to_json('fourth_results/first_category/first_result_5.json', res_5)

# Выборка с агрегацией
res_2_1 = get_price_usd_agg(collection)
writing_to_json('fourth_results/second_category/second_result_1.json', res_2_1)

res_2_2 = get_freq_by_os_type(collection)
writing_to_json('fourth_results/second_category/second_result_2.json', res_2_2)


res_2_3 = get_stat_by_custom_key(collection, 'os_type', 'price_usd')
writing_to_json('fourth_results/second_category/second_result_3.json', res_2_3)

res_2_4 = get_max_price_usd_by_min_storage_sort(collection)
delete_object_id(res_2_4)
writing_to_json('fourth_results/second_category/second_result_4.json', res_2_4)

res_2_5 = custom_query(collection)
writing_to_json('fourth_results/second_category/second_result_5.json', res_2_5)

# Обновление/Удаление данных
print(delete_by_price_usd(collection))
print(inc_storage(collection))
print(inc_price_usd(collection))
print(inc_price_usd_by_store(collection))
print(delete_by_custom_predicate(collection))
