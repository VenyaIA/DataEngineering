def calculate_stats(field, data):
    stats = {}
    values = [row[field] for row in data]
    n = len(values)
    mean = sum(values) / n
    stats[field] = {
        'max': max(values),
        'min': min(values),
        'avg': mean,
        'sum': sum(values)
    }
    return stats


def calculate_frequency(field, data):
    result = {}
    for item in data:
        fild_value = item.get(field, 'no data available')  # если нет данных
        if fild_value not in result:
            result[fild_value] = 1
        else:
            result[fild_value] += 1

    return result
