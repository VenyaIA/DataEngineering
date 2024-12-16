import json

import pymongo
from pymongo import MongoClient
import csv
import msgpack


def connect_db():
    client = MongoClient()
    db = client['db-2024']
    print(db.jobs)
    return db.jobs


def read_csv(filename):
    items = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        reader.__next__()
        # job;salary;id;city;year;age
        for row in reader:
            item = {
                "job": row[0],
                "salary": float(row[1]),
                "id": int(row[2]),
                "city": row[3],
                "year": int(row[4]),
                "age": int(row[5])
            }
            items.append(item)
    return items


def read_msgpack(filename):
    with open(filename, 'rb') as f:
        return msgpack.load(f)


def read_text(filename):
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
            if key in ['salary']:
                pair[1] = float(pair[1])
            if key in ['id', 'year', 'age']:
                pair[1] = int(pair[1])

            item[pair[0]] = pair[1]

    return items


def delete_object_id(items):
    for el in items:
        el.pop('_id')


def writing_to_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)


collection = connect_db()


def sort_by_salary(collection):
    result = list(collection.find(limit=10).sort({'salary': pymongo.DESCENDING}))
    return result


def filter_by_age(collection):
    return list(collection
                .find({
                    "age": {"$lt": 30}
                }, limit=15)
                .sort({'salary': pymongo.DESCENDING}))


def complex_filter(collection):
    return list(collection
                .find({
                    "city": "Москва",
                    "job": {"$in": ["Программист", 'Продавец', 'Строитель']}
                }, limit=10)
                .sort({'age': pymongo.ASCENDING}))


def fourth_filter(collection):
    return (collection
                .count_documents({
                    "age": {"$gt": 18, "$lte": 28},  # (18;28]
                    "year": {"$gte": 2019, "$lte": 2022},
                    "$or": [
                        {"salary": {"$gt": 50000, "$lte": 75000}},
                        {"salary": {"$gt": 1250000, "$lt": 150000}}
                    ]
                }))


def get_salary_agg(collection):
    q = [
        {
            "$group": {
                "_id": "result",
                "max_salary": {"$max": "$salary"},
                "min_salary": {"$min": "$salary"},
                "avg_salary": {"$avg": "$salary"}
            }
        }
    ]

    return list(collection.aggregate(q))


def get_freq_by_job(collection):
    q = [
        {
            "$group": {
                "_id": "$job",
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


def get_max_salary_by_min_age(collection):
    q = [
        {
            "$group": {
                "_id": "$age",
                "max_salary": {"$max": "$salary"}
            }
        },
        {
            "$group": {
                "_id": "result",
                "min_age": {"$min": "$_id"},
                "max_salary": {"$max": "$max_salary"}
            }
        }
    ]

    return list(collection.aggregate(q))


def get_max_salary_by_min_age_sort(collection):
    result = list(collection.find(limit=1).sort({'age': pymongo.ASCENDING, 'salary': pymongo.DESCENDING}))
    return result


def get_min_salary_by_max_age_sort(collection):
    result = list(collection.find(limit=1).sort({'age': pymongo.DESCENDING, 'salary': pymongo.ASCENDING}))
    return result


def get_age_stat_by_city_with_match(collection):
    q = [
        {
          "$match": {
              "salary": {"$gt": 50_000}
          }
        },
        {
            "$group": {
                "_id": "$city",
                "max": {"$max": "$age"},
                "min": {"$min": "$age"},
                "avg": {"$avg": "$age"}
            }
        },
        {
            "$sort": {
                "avg": pymongo.DESCENDING
            }
        }
    ]

    return list(collection.aggregate(q))


def custom_query(collection):
    q = [
        {
          "$match": {
              "city": {"$in": ["Варшава", "Краков", "Москва"]},
              "job": {"$in": ["Строитель", "Программист"]},
              "$or": [
                  {"age": {"$gt": 18, "$lt": 25}},
                  {"age": {"$gt": 50, "$lt": 65}}
              ]
          }
        },
        {
            "$group": {
                "_id": "result",
                "max": {"$max": "$salary"},
                "min": {"$min": "$salary"},
                "avg": {"$avg": "$salary"}
            }
        }
    ]

    return list(collection.aggregate(q))


def custom_query_2(collection):
    q = [
        {
          "$match": {
              "year": {"$gt": 2002}
          }
        },
        {
            "$group": {
                "_id": "$job",
                "max_age": {"$max": "$age"},
                "min_age": {"$min": "$age"},
                "avg_age": {"$avg": "$age"}
            }
        },
        {
            "$sort": {
                "year": pymongo.DESCENDING
            }
        }
    ]

    return list(collection.aggregate(q))


def delete_by_salary(collection):
    return collection.delete_many({
        "$or": [
            {"salary": {"$lt": 25_000}},
            {"salary": {"$gt": 175_000}},
        ]
    })


def inc_age(collection):
    return collection.update_many({}, {
        "$inc": {
            "age": 1
        }
    })


def inc_salary(collection):
    return collection.update_many({
        "job": {"$in": ["Строитель", "Программист"]}
    }, {
        "$mul": {
            "salary": 1.05
        }
    })


def inc_salary_by_city(collection):
    return collection.update_many({
        "city": {"$in": ["Варшава", "Краков", "Москва"]}
    }, {
        "$mul": {
            "salary": 1.07
        }
    })


def inc_salary_by_custom_predicate(collection):
    return collection.update_many({
        "city": {"$in": ["Минск", "Эльче", "Мадрид"]},
        "job": {"$in": ["Бухгалтер", "Психолог"]},
        "$or": [
            {"age": {"$gt": 20, "$lt": 29}},
            {"age": {"$gt": 40, "$lt": 55}}
        ]
    }, {
        "$mul": {
            "salary": 1.1
        }
    })


def delete_by_custom_predicate(collection):
    return collection.delete_many({
        "$or": [
            {"age": {"$lt": 20}},
            {"age": {"$gt": 50}},
        ]
    })


# TASK 1
collection.insert_many(read_csv('70/task_1_item.csv'))

res_1 = sort_by_salary(collection)
delete_object_id(res_1)
writing_to_json('first_results/first_result_1.json', res_1)

res_2 = filter_by_age(collection)
delete_object_id(res_2)
writing_to_json('first_results/first_result_2.json', res_2)

res_3 = complex_filter(collection)
delete_object_id(res_3)
writing_to_json('first_results/first_result_3.json', res_3)

res_4 = fourth_filter(collection)
data_res_4 = {'result_count': res_4}
writing_to_json('first_results/first_result_4.json', data_res_4)

# TASK 2
collection.insert_many(read_msgpack('70/task_2_item.msgpack'))
res_2_1 = get_salary_agg(collection)
writing_to_json('second_results/second_result_1.json', res_2_1)

res_2_2 = get_freq_by_job(collection)
writing_to_json('second_results/second_result_2.json', res_2_2)

res_2_3 = get_stat_by_custom_key(collection, 'city', 'salary')
writing_to_json('second_results/second_result_3.json', res_2_3)

res_2_4 = get_stat_by_custom_key(collection, 'job', 'salary')
writing_to_json('second_results/second_result_4.json', res_2_4)

res_2_5 = get_stat_by_custom_key(collection, 'city', 'age')
writing_to_json('second_results/second_result_5.json', res_2_5)

res_2_6 = get_stat_by_custom_key(collection, 'job', 'age')
writing_to_json('second_results/second_result_6.json', res_2_6)

res_2_7 = get_max_salary_by_min_age_sort(collection)
delete_object_id(res_2_7)
writing_to_json('second_results/second_result_7.json', res_2_7)

res_2_8 = get_min_salary_by_max_age_sort(collection)
delete_object_id(res_2_8)
writing_to_json('second_results/second_result_8.json', res_2_8)

res_2_9 = get_age_stat_by_city_with_match(collection)
writing_to_json('second_results/second_result_9.json', res_2_9)

res_2_10 = custom_query(collection)
writing_to_json('second_results/second_result_10.json', res_2_10)

res_2_11 = custom_query_2(collection)  # Доделай
writing_to_json('second_results/second_result_11.json', res_2_11)

# TASK 3
collection.insert_many(read_text('70/task_3_item.text'))

print(delete_by_salary(collection))
print(inc_age(collection))
print(inc_salary(collection))
print(inc_salary_by_city(collection))
print(inc_salary_by_custom_predicate(collection))
print(delete_by_custom_predicate(collection))












