import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns


# данные взяты: https://www.kaggle.com/datasets/goulvenfuret/books-dataset-isbn-based
# предварительно данные обрезаны вдвое из-за большого размера
# обрезанные данные можно скачать отсюда: https://drive.google.com/drive/folders/17ljuWAQqnbzNnUKrlYGdyVWeep0sWVMX
def read_file(filename):
    return pd.read_csv(filename)


def get_memory_stat_by_column(df: pd.DataFrame):
    memory_usage_stat = df.memory_usage(deep=True)
    total_memory_usage = memory_usage_stat.sum()
    print(f'file in memory size = {total_memory_usage // 1024:10} КБ')
    column_stat = list()
    for key in df.dtypes.keys():
        column_stat.append({
            "column_name": key,
            "memory_abs": memory_usage_stat[key] // 1024,
            "memory_per": round(memory_usage_stat[key] / total_memory_usage * 100, 4),
            "dtype": df.dtypes[key]
        })
    column_stat.sort(key=lambda x: x['memory_abs'], reverse=True)
    for column in column_stat:
        print(
            f"{column['column_name']:30}: {column['memory_abs']:10} КБ: {column['memory_per']:10}% : {column['dtype']}"
        )


def mem_usage(pandas_obj):
    if isinstance(pandas_obj, pd.DataFrame):
        usage_b = pandas_obj.memory_usage(deep=True).sum()
    else:
        usage_b = pandas_obj.memory_usage(deep=True)
    usage_b = usage_b / 1024 ** 2  # Преобразуем байты в мегабайты
    return "{:03.2f} MB".format(usage_b)


def opt_obj(df):
    converted_obj = pd.DataFrame()
    dataset_obj = df.select_dtypes(include=['object']).copy()

    for col in dataset_obj.columns:
        num_unique_values = len(dataset_obj[col].unique())
        num_total_values = len(dataset_obj[col])
        if num_unique_values / num_total_values < 0.5:
            converted_obj.loc[:, col] = dataset_obj[col].astype('category')
        else:
            converted_obj.loc[:, col] = dataset_obj[col]

    print(mem_usage(dataset_obj))
    print(mem_usage(converted_obj))
    return converted_obj


def opt_int(df):
    dataset_int = df.select_dtypes(include=['int'])
    converted_int = dataset_int.apply(pd.to_numeric, downcast='unsigned')
    print(mem_usage(dataset_int))
    print(mem_usage(converted_int))
    #
    compare_ints = pd.concat([dataset_int.dtypes, converted_int.dtypes], axis=1)
    compare_ints.columns = ['before', 'after']
    compare_ints.apply(pd.Series.value_counts)
    print(compare_ints)

    return converted_int


def opt_float(df):
    dataset_float = df.select_dtypes(include=['float'])
    converted_float = dataset_float.apply(pd.to_numeric, downcast='float')

    print(mem_usage(dataset_float))
    print(mem_usage(converted_float))

    compare_floats = pd.concat([dataset_float.dtypes, converted_float.dtypes], axis=1)
    compare_floats.columns = ['before', 'after']
    compare_floats.apply(pd.Series.value_counts)
    print(compare_floats)

    return converted_float


# Step 1-3
file_name = 'data/open4goods-isbn-dataset.csv'
dataset = read_file(file_name)
get_memory_stat_by_column(dataset)
print("---------------------------------------------------------")

# Step 4-6
optimized_dataset = dataset.copy()

converted_obj = opt_obj(dataset)
converted_int = opt_int(dataset)
converted_float = opt_float(dataset)

optimized_dataset[converted_obj.columns] = converted_obj
optimized_dataset[converted_int.columns] = converted_int
optimized_dataset[converted_float.columns] = converted_float
print("---------------------------------------------------------")

# Step 7
get_memory_stat_by_column(dataset)
print(mem_usage(dataset))
print(mem_usage(optimized_dataset))
optimized_dataset.info(memory_usage='deep')
print("---------------------------------------------------------")

# Step 8
need_column = dict()
# "isbn","title","last_updated","offers_count",
# "min_price","min_price_compensation","currency",
# "url","editeur","format","nb_page","classification_decitre_1",
# "classification_decitre_2","classification_decitre_3","souscategorie","souscategorie2"
column_names = ['isbn', 'title', 'last_updated',
                 'offers_count', 'min_price', 'min_price_compensation',
                 'currency', 'url', 'editeur', 'format']

opt_dtypes = optimized_dataset.dtypes
for key in column_names:
    need_column[key] = opt_dtypes[key]
    print(f"{key}:{opt_dtypes[key]}")

with open("dtypes_2.json", mode='w') as file:
    dtype_json = need_column.copy()
    for key in dtype_json.keys():
        dtype_json[key] = str(dtype_json[key])

    json.dump(dtype_json, file)

output_file = "filtered_dataset.csv"
read_and_optimized = pd.read_csv(file_name, usecols=lambda x: x in column_names, dtype=need_column)

read_and_optimized.to_csv(output_file)

# Step 9
df = read_and_optimized

# 1. Линейный график: изменение min_price с течением времени
plt.figure(figsize=(14, 6))
# Преобразование last_updated из миллисекунд в datetime
df['last_updated'] = pd.to_datetime(df['last_updated'] // 1000, unit='s')
df_sorted = df.sort_values(by='last_updated')
plt.plot(df_sorted['last_updated'], df_sorted['min_price'], color='blue', label='Min Price')
plt.xlabel('Last Updated')
plt.ylabel('Min Price')
plt.title('Change in Minimum Price Over Time')
plt.legend()
plt.grid()
plt.savefig('graphs/graph_1')
plt.show()


# 2. Столбчатый график: количество предложений offers_count по издателям editeur
plt.figure(figsize=(12, 8))
top_editeurs = df['editeur'].value_counts().nlargest(10)  # Топ 10 издателей
plt.bar(top_editeurs.index, top_editeurs.values, color='orange')
plt.xticks(rotation=45)
plt.title('Top 10 Publishers by Offers Count')
plt.xlabel('Publisher')
plt.ylabel('Number of Offers')
plt.grid(axis='y')
plt.savefig('graphs/graph_2')
plt.show()


# 3. Круговая диаграмма: распределение форматов книг
plt.figure(figsize=(8, 8))
format_counts = df['format'].value_counts().nlargest(10)
plt.pie(format_counts, labels=format_counts.index, autopct='%1.1f%%')
plt.title('Format Distribution')
plt.savefig('graphs/graph_3')
plt.show()


# 4. Создание матрицы корреляции
numeric_columns = ['offers_count', 'min_price', 'min_price_compensation']
correlation_matrix = df[numeric_columns].corr()

# Построение тепловой карты
plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, annot=True, cmap='coolwarm', fmt=".2f", linewidths=0.5)
plt.title("Correlation Heatmap")
plt.savefig('graphs/graph_4')
plt.show()


# 5. Гистограмма: распределение минимальной цены (min_price)
# Логарифмируем min_price (добавляем 1, чтобы избежать log(0))
log_min_price = np.log1p(df['min_price'])

plt.figure(figsize=(10, 6))
plt.hist(log_min_price, bins=50, color='purple', edgecolor='black')
plt.title('Logarithmic Distribution of Minimum Prices')
plt.xlabel('Log(1 + Min Price)')
plt.ylabel('Frequency')
plt.grid(axis='y')
plt.savefig('graphs/graph_5')
plt.show()



