import csv
import json
import math
import os
import pickle
import pandas as pd
import msgpack


def read_csv(path):
    data = []
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append({
                'measure': row['measure'],
                'quantile': float(row['quantile'].strip('%')),
                'area': row['area'],
                'sex': row['sex'],
                'age': row['age'], # не стандартные данные в виде строк
                'geography': row['geography'],
                'ethnic': row['ethnic'],
                'value': float(row['value'])
            })

    return data


def calculate_numerical_stats(data, fields):
    stats = {}
    for field in fields:
        values = [row[field] for row in data]
        n = len(values)
        mean = sum(values) / n
        variance = sum((x - mean) ** 2 for x in values) / n
        stats[field] = {
            'max': max(values),
            'min': min(values),
            'avg': mean,
            'sum': sum(values),
            'std': math.sqrt(variance)
        }
    return stats


def calculate_categorical_stats(data, fields):
    stats = {}
    for field in fields:
        frequency = {}
        for row in data:
            value = row[field]
            if value in frequency:
                frequency[value] += 1
            else:
                frequency[value] = 1
        stats[field] = frequency
    return stats


def save_data(data, filename_prefix):
    csv_path = f'{filename_prefix}.csv'
    df = pd.DataFrame(data)
    df.to_csv(csv_path, index=False)

    json_path = f'{filename_prefix}.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    msgpack_path = f'{filename_prefix}.msgpack'
    with open(msgpack_path, 'wb') as f:
        msgpack.dump(data, f)

    pkl_path = f'{filename_prefix}.pkl'
    with open(pkl_path, 'wb') as f:
        pickle.dump(data, f)

    return csv_path, json_path, msgpack_path, pkl_path


def compare_file_sizes(files):
    sizes = {}
    for file in files:
        sizes[file] = os.path.getsize(file)
    return sizes


data = read_csv('70/data_for_task_fifth.csv')

numerical_fields = ['quantile', 'value']
categorical_fields = ['measure', 'area', 'sex', 'age', 'geography', 'ethnic']

numerical_stats = calculate_numerical_stats(data, numerical_fields)
categorical_stats = calculate_categorical_stats(data, categorical_fields)

stats = {'numerical': numerical_stats, 'categorical': categorical_stats}


files = save_data(stats, 'fifth_task')

file_sizes = compare_file_sizes(files)

print("Размеры файлов:")
for file, size in file_sizes.items():
    print(f"{file}: {size}")
